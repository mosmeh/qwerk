use clap::{arg, Parser, ValueEnum};
use qwerk::{
    ConcurrencyControl, Database, DatabaseOptions, Error, Optimistic, Pessimistic, Worker,
};
use rand::{distributions::WeightedIndex, prelude::Distribution, rngs::SmallRng, Rng, SeedableRng};
use serde::Serialize;
use std::{
    io::Write,
    num::NonZeroUsize,
    ops::AddAssign,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Barrier,
    },
    time::{Duration, Instant},
};

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Debug, Clone, Parser, Serialize)]
struct Cli {
    #[arg(long)]
    path: Option<PathBuf>,

    #[arg(long, default_value_t = 8)]
    threads: usize,

    #[arg(long, default_value = "1")]
    logging_threads: NonZeroUsize,

    #[arg(long, default_value_t = 100000)]
    records: u64,

    #[arg(long, default_value_t = 8)]
    payload: usize,

    #[arg(long, default_value_t = 2000)]
    duration: u64,

    #[arg(long, default_value_t = 10000)]
    checkpoint_interval: u64,

    #[arg(long, default_value_t = 4)]
    working_set: usize,

    #[arg(long, default_value_t = 0.5)]
    theta: f64,

    #[arg(long, value_enum, default_value_t = Protocol::Optimistic)]
    protocol: Protocol,

    #[arg(long, value_enum, default_value_t = WorkloadKind::A)]
    workload: WorkloadKind,

    #[arg(long, required_if_eq("workload", "variable"))]
    read_proportion: Option<f64>,

    #[arg(long)]
    seed: Option<u64>,
}

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
enum Protocol {
    Optimistic,
    Pessimistic,
}

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
enum WorkloadKind {
    A,
    B,
    C,
    D,
    F,
    Variable,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.protocol {
        Protocol::Optimistic => run_benchmark(cli, Optimistic::new()),
        Protocol::Pessimistic => run_benchmark(cli, Pessimistic::new()),
    }
}

fn run_benchmark<C: ConcurrencyControl>(cli: Cli, concurrency_control: C) -> anyhow::Result<()> {
    let workload = match cli.workload {
        WorkloadKind::A => Workload {
            read_proportion: 50,
            update_proportion: 50,
            insert_proportion: 0,
            rmw_proportion: 0,
        },
        WorkloadKind::B => Workload {
            read_proportion: 95,
            update_proportion: 5,
            insert_proportion: 0,
            rmw_proportion: 0,
        },
        WorkloadKind::C => Workload {
            read_proportion: 100,
            update_proportion: 0,
            insert_proportion: 0,
            rmw_proportion: 0,
        },
        WorkloadKind::D => Workload {
            read_proportion: 95,
            update_proportion: 0,
            insert_proportion: 5,
            rmw_proportion: 0,
        },
        WorkloadKind::F => Workload {
            read_proportion: 50,
            update_proportion: 0,
            insert_proportion: 0,
            rmw_proportion: 50,
        },
        WorkloadKind::Variable => {
            let read_proportion = ((100.0 * cli.read_proportion.unwrap()) as u32).clamp(0, 100);
            Workload {
                read_proportion,
                update_proportion: 100 - read_proportion,
                insert_proportion: 0,
                rmw_proportion: 0,
            }
        }
    };

    let options = DatabaseOptions::with_concurrency_control(concurrency_control)
        .logging_threads(cli.logging_threads)
        .checkpoint_interval(Duration::from_millis(cli.checkpoint_interval));
    let shared = Arc::new(SharedState {
        db: match &cli.path {
            Some(path) => options.open(path)?,
            None => options.open_temporary(),
        },
        barrier: Barrier::new(cli.threads + 1),
        is_running: true.into(),
        latest: cli.records.into(),
    });

    #[cfg(feature = "affinity")]
    let core_ids = {
        use rand::seq::SliceRandom;
        let mut core_ids = core_affinity::get_core_ids().unwrap();
        anyhow::ensure!(core_ids.len() >= cli.threads);
        let mut rng = match cli.seed {
            Some(seed) => SmallRng::seed_from_u64(seed),
            None => SmallRng::from_entropy(),
        };
        core_ids.shuffle(&mut rng);
        core_ids
    };

    let workers: Vec<_> = (0..cli.threads)
        .map(|worker_index| {
            #[cfg(feature = "affinity")]
            let core_id = core_ids[worker_index];
            let shared = shared.clone();
            let cli = cli.clone();
            let workload = workload.clone();
            std::thread::spawn(move || {
                #[cfg(feature = "affinity")]
                anyhow::ensure!(core_affinity::set_for_current(core_id));
                run_worker(cli, workload, &shared, worker_index)
            })
        })
        .collect();

    eprintln!("Preparing");
    shared.barrier.wait();
    shared.db.flush()?;

    eprintln!("Start");
    shared.barrier.wait();

    let start = Instant::now();
    std::thread::sleep(Duration::from_millis(cli.duration));
    shared.is_running.store(false, Ordering::SeqCst);
    let elapsed = start.elapsed();

    eprintln!("Finished");

    let mut stats = Statistics::default();
    for worker in workers {
        stats += worker.join().unwrap()?;
    }

    let tps = (stats.num_commits as f64 / elapsed.as_secs_f64()) as u64;
    let abort_rate = stats.num_aborts as f64 / (stats.num_commits + stats.num_aborts) as f64;

    #[derive(Debug, Serialize)]
    struct Summary {
        #[serde(flatten)]
        stats: Statistics,
        elapsed: u128,
        tps: u64,
        abort_rate: f64,
        #[serde(flatten)]
        args: Cli,
    }
    let summary = Summary {
        stats,
        elapsed: elapsed.as_millis(),
        tps,
        abort_rate,
        args: cli,
    };

    eprintln!("{:#?}", summary);

    let mut stdout = std::io::stdout().lock();
    serde_json::ser::to_writer_pretty(&mut stdout, &summary)?;
    stdout.write_all(b"\n")?;
    Ok(())
}

struct SharedState<C: ConcurrencyControl> {
    db: Database<C>,
    barrier: Barrier,
    is_running: AtomicBool,
    latest: AtomicU64,
}

fn run_worker<C: ConcurrencyControl>(
    cli: Cli,
    workload: Workload,
    shared: &SharedState<C>,
    worker_index: usize,
) -> anyhow::Result<Statistics> {
    let mut worker = shared.db.worker()?;

    let payload = vec![0; cli.payload];

    let from = cli.records * worker_index as u64 / cli.threads as u64;
    let to = cli.records * (worker_index as u64 + 1) / cli.threads as u64;
    for i in from..to {
        let key = i.to_ne_bytes();
        let mut txn = worker.transaction();
        txn.insert(key, &payload).unwrap();
        txn.set_wait_for_durability(false);
        txn.commit().unwrap();
    }

    let mut rng = match cli.seed {
        Some(seed) => SmallRng::seed_from_u64(seed ^ worker_index as u64),
        None => SmallRng::from_entropy(),
    };
    let op_weights = [
        workload.read_proportion,
        workload.update_proportion,
        workload.insert_proportion,
        workload.rmw_proportion,
    ];
    let has_insert = workload.insert_proportion > 0;
    let mut generator =
        KeyGenerator::new(&mut rng, &shared.latest, cli.records, cli.theta, has_insert);
    let op_dist = WeightedIndex::new(op_weights).unwrap();
    let mut keys = Vec::with_capacity(cli.working_set);
    let mut stats = Statistics::default();

    shared.barrier.wait(); // Signal that the worker is ready

    shared.barrier.wait(); // Signal that the benchmark will start
    while shared.is_running.load(Ordering::SeqCst) {
        let op = OPERATIONS[op_dist.sample(&mut rng)];
        for _ in 0..cli.working_set {
            keys.push(if op == Operation::Insert {
                generator.insert()
            } else {
                generator.zipfian(&mut rng)
            });
        }
        let result = run_transaction(&mut worker, op, &keys, &payload);
        match result {
            Ok(()) => stats.num_commits += 1,
            Err(Error::TransactionNotSerializable | Error::TooManyTransactions) => {
                stats.num_aborts += 1
            }
            Err(e) => return Err(e.into()),
        }
        keys.clear();
    }
    Ok(stats)
}

fn run_transaction<C: ConcurrencyControl>(
    worker: &mut Worker<C>,
    op: Operation,
    keys: &[u64],
    payload: &[u8],
) -> qwerk::Result<()> {
    use std::hint::black_box;

    let mut txn = worker.transaction();
    for key in keys {
        let key = key.to_ne_bytes();
        match op {
            Operation::Read => {
                black_box(txn.get(black_box(key))?);
            }
            Operation::Update | Operation::Insert => {
                txn.insert(black_box(key), black_box(payload))?
            }
            Operation::ReadModifyWrite => {
                let key = black_box(key);
                black_box(txn.get(key)?);
                txn.insert(key, black_box(payload))?;
            }
        }
    }
    txn.set_wait_for_durability(false);
    txn.commit()?;
    Ok(())
}

#[derive(Debug, Default, Serialize)]
struct Statistics {
    #[serde(rename = "commits")]
    num_commits: u64,
    #[serde(rename = "aborts")]
    num_aborts: u64,
}

impl AddAssign<Self> for Statistics {
    fn add_assign(&mut self, rhs: Self) {
        self.num_commits += rhs.num_commits;
        self.num_aborts += rhs.num_aborts;
    }
}

#[derive(Clone)]
struct Workload {
    read_proportion: u32,
    update_proportion: u32,
    insert_proportion: u32,
    rmw_proportion: u32,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Operation {
    Read,
    Update,
    Insert,
    ReadModifyWrite,
}

const OPERATIONS: [Operation; 4] = [
    Operation::Read,
    Operation::Update,
    Operation::Insert,
    Operation::ReadModifyWrite,
];

struct KeyGenerator<'a> {
    max: u64,
    theta: f64,
    alpha: f64,
    count_for_zeta: u64,
    zeta2theta: f64,
    zeta_n: f64,
    eta: f64,
    has_insert: bool,
    latest: &'a AtomicU64,
}

impl<'a> KeyGenerator<'a> {
    fn new<R: Rng>(
        rng: &mut R,
        latest: &'a AtomicU64,
        record_count: u64,
        theta: f64,
        has_insert: bool,
    ) -> Self {
        let max = record_count - 1;
        let zeta2theta = (0..2).map(|i| 1.0 / f64::powf((i + 1).into(), theta)).sum();
        let mut this = Self {
            max,
            theta,
            alpha: 1.0 / (1.0 - theta),
            count_for_zeta: 2,
            zeta2theta,
            eta: (1.0 - f64::powf(2.0 / max as f64, 1.0 - theta)) / (1.0 - zeta2theta / 0.0),
            zeta_n: 0.0,
            has_insert,
            latest,
        };
        this.next(rng, max);
        this
    }

    fn insert(&mut self) -> u64 {
        self.latest.fetch_add(1, Ordering::SeqCst)
    }

    fn zipfian<R: Rng>(&mut self, rng: &mut R) -> u64 {
        let max = if self.has_insert {
            self.latest.load(Ordering::Relaxed)
        } else {
            self.max
        };
        self.next(rng, max)
    }

    fn next<R: Rng>(&mut self, rng: &mut R, max: u64) -> u64 {
        if max != self.count_for_zeta {
            self.zeta_n = if max > self.count_for_zeta {
                self.zeta(self.count_for_zeta, max, self.zeta_n)
            } else {
                self.zeta(0, max, 0.0)
            };
            self.eta = (1.0 - f64::powf(2.0 / max as f64, 1.0 - self.theta))
                / (1.0 - self.zeta2theta / self.zeta_n);
        }
        assert!(max >= self.count_for_zeta);

        let u: f64 = rng.gen();
        let uz = u * self.zeta_n;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + f64::powf(0.5, self.theta) {
            return 1;
        }
        (max as f64 * f64::powf(self.eta.mul_add(u, 1.0 - self.eta), self.alpha)) as u64
    }

    fn zeta(&mut self, st: u64, n: u64, initial_sum: f64) -> f64 {
        self.count_for_zeta = n;
        (st..n)
            .map(|i| 1.0 / f64::powf((i + 1) as f64, self.theta))
            .sum::<f64>()
            + initial_sum
    }
}
