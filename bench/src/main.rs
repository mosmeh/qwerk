use clap::{arg, Parser, ValueEnum};
use qwerk::{ConcurrencyControl, Database, Optimistic, Pessimistic};
use rand::{
    distributions::{Uniform, WeightedIndex},
    prelude::Distribution,
    Rng, SeedableRng,
};
use serde::Serialize;
use std::{
    io::Write,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Barrier,
    },
    time::{Duration, Instant},
};

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
struct Cli {
    #[arg(long, default_value_t = 8)]
    threads: usize,

    #[arg(long, default_value_t = 100000)]
    records: u64,

    #[arg(long, default_value_t = 8)]
    payload: usize,

    #[arg(long, default_value_t = 2000)]
    duration: u64,

    #[arg(long, default_value_t = 4)]
    working_set: usize,

    #[arg(long, default_value_t = 0.5)]
    theta: f64,

    #[arg(long, value_enum, default_value_t = Protocol::Pessimistic)]
    protocol: Protocol,

    #[arg(long, value_enum, default_value_t = WorkloadKind::A)]
    workload: WorkloadKind,

    #[arg(long, required_if_eq("workload", "variable"))]
    read_proportion: Option<f64>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Protocol {
    Optimistic,
    Pessimistic,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum WorkloadKind {
    A,
    B,
    C,
    Variable,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.protocol {
        Protocol::Optimistic => run_benchmark::<Optimistic>(cli),
        Protocol::Pessimistic => run_benchmark::<Pessimistic>(cli),
    }
}

fn run_benchmark<C: ConcurrencyControl>(cli: Cli) -> anyhow::Result<()> {
    let workload = match cli.workload {
        WorkloadKind::A => Workload {
            read_proportion: 50,
            update_proportion: 50,
            insert_proportion: 0,
            rmw_proportion: 0,
            request_distribution: RequestDistribution::Zipfian,
        },
        WorkloadKind::B => Workload {
            read_proportion: 95,
            update_proportion: 5,
            insert_proportion: 0,
            rmw_proportion: 0,
            request_distribution: RequestDistribution::Zipfian,
        },
        WorkloadKind::C => Workload {
            read_proportion: 100,
            update_proportion: 0,
            insert_proportion: 0,
            rmw_proportion: 0,
            request_distribution: RequestDistribution::Zipfian,
        },
        WorkloadKind::Variable => {
            let read_proportion = ((100.0 * cli.read_proportion.unwrap()) as u32).clamp(0, 100);
            Workload {
                read_proportion,
                update_proportion: 100 - read_proportion,
                insert_proportion: 0,
                rmw_proportion: 0,
                request_distribution: RequestDistribution::Zipfian,
            }
        }
    };

    let db: Database<C> = Database::new();
    struct State<C: ConcurrencyControl> {
        db: Database<C>,
        barrier: Barrier,
        is_running: AtomicBool,
        latest: AtomicU64,
    }
    let state = Arc::new(State {
        db,
        barrier: Barrier::new(cli.threads + 1),
        is_running: true.into(),
        latest: 0.into(),
    });

    let num_threads = match std::thread::available_parallelism() {
        Ok(n) => n.get(),
        Err(_) => 1,
    };
    eprintln!("Populating keys");
    let threads: Vec<_> = (0..num_threads)
        .map(|thread_index| {
            let state = state.clone();
            let from = cli.records * thread_index as u64 / num_threads as u64;
            let to = cli.records * (thread_index as u64 + 1) / num_threads as u64;
            let payload = vec![0; cli.payload];
            std::thread::spawn(move || {
                let mut worker = state.db.spawn_worker();
                let mut txn = worker.begin_transaction();
                for i in from..to {
                    let key = format!("{}", i).into_bytes();
                    txn.insert(key, &payload).unwrap();
                }
                txn.commit().unwrap();
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }

    #[derive(Default)]
    struct Statistics {
        num_commits: u64,
        num_aborts: u64,
    }

    #[cfg(feature = "affinity")]
    let core_ids = core_affinity::get_core_ids().unwrap();
    #[cfg(feature = "affinity")]
    assert!(core_ids.len() >= cli.threads);

    eprintln!("Spawning worker threads");
    let clients: Vec<_> = (0..cli.threads)
        .map(|#[allow(unused)] i| {
            #[cfg(feature = "affinity")]
            let core_id = core_ids[i];
            let state = state.clone();
            std::thread::spawn(move || {
                #[cfg(feature = "affinity")]
                assert!(core_affinity::set_for_current(core_id));
                let mut worker = state.db.spawn_worker();
                let mut rng = rand::rngs::SmallRng::from_entropy();
                let mut generator =
                    NumberGenerator::new(&mut rng, &state.latest, cli.records, cli.theta);
                let mut stats = Statistics::default();
                let mut keys = Vec::with_capacity(cli.working_set);
                let payload = vec![0; cli.payload];
                let op_weights = &[
                    workload.read_proportion,
                    workload.update_proportion,
                    workload.insert_proportion,
                    workload.rmw_proportion,
                ];
                let op_dist = WeightedIndex::new(op_weights).unwrap();
                let has_insert = workload.insert_proportion > 0;

                state.barrier.wait();
                while state.is_running.load(Ordering::SeqCst) {
                    let op = OPERATIONS[op_dist.sample(&mut rng)];
                    for _ in 0..cli.working_set {
                        let key = if op == Operation::Insert {
                            state.latest.fetch_add(1, Ordering::SeqCst)
                        } else {
                            match workload.request_distribution {
                                RequestDistribution::Uniform => generator.uniform(&mut rng),
                                RequestDistribution::Zipfian => {
                                    generator.next(&mut rng, has_insert, &state.latest)
                                }
                            }
                        };
                        keys.push(format!("{}", key).into_bytes());
                    }

                    let mut txn = worker.begin_transaction();
                    for key in &keys {
                        match op {
                            Operation::Read => {
                                let _ = txn.get(key);
                            }
                            Operation::Update | Operation::Insert => {
                                let _ = txn.insert(key, &payload);
                            }
                            Operation::Rmw => {
                                let _ = txn.get(key);
                                let _ = txn.insert(key, &payload);
                            }
                        }
                    }
                    let result = txn.commit();
                    if state.is_running.load(Ordering::Relaxed) {
                        match result {
                            Ok(()) => stats.num_commits += 1,
                            Err(_) => stats.num_aborts += 1,
                        }
                    } else {
                        break;
                    }
                    keys.clear();
                }
                stats
            })
        })
        .collect();

    eprintln!("Start");
    state.barrier.wait();

    let start = Instant::now();
    std::thread::sleep(Duration::from_millis(cli.duration));
    state.is_running.store(false, Ordering::SeqCst);
    let elapsed = start.elapsed();

    let mut stats = Statistics::default();
    for client in clients {
        let s = client.join().unwrap();
        stats.num_commits += s.num_commits;
        stats.num_aborts += s.num_aborts;
    }

    eprintln!("Finished");
    eprintln!("Elapsed\t{:.3?}", elapsed);
    eprintln!("Commits\t{}", stats.num_commits);
    eprintln!("Aborts\t{}", stats.num_aborts);
    eprintln!(
        "Abort rate\t{:.3}",
        stats.num_aborts as f64 / (stats.num_commits + stats.num_aborts) as f64
    );
    let tps = (stats.num_commits as f64 / elapsed.as_secs_f64()) as u64;
    eprintln!("TPS\t{}", tps);

    #[derive(Serialize)]
    struct Summary {
        etime: u64,
        commits: u64,
        aborts: u64,
        tps: u64,
        workload: String,
        protocol: String,
        threads: usize,
        theta: f64,
        read_proportion: Option<f64>,
    }
    let mut stdout = std::io::stdout().lock();
    serde_json::ser::to_writer_pretty(
        &mut stdout,
        &Summary {
            etime: elapsed.as_millis() as u64,
            commits: stats.num_commits,
            aborts: stats.num_aborts,
            tps,
            workload: format!("{:?}", cli.workload),
            protocol: format!("{:?}", cli.protocol),
            threads: cli.threads,
            theta: cli.theta,
            read_proportion: cli.read_proportion,
        },
    )?;
    stdout.write_all(b"\n")?;
    Ok(())
}

struct NumberGenerator {
    uniform: Uniform<u64>,
    max: u64,
    theta: f64,
    alpha: f64,
    count_for_zeta: u64,
    zeta2theta: f64,
    zeta_n: f64,
    eta: f64,
}

impl NumberGenerator {
    fn new<R: Rng>(rng: &mut R, latest: &AtomicU64, record_count: u64, theta: f64) -> Self {
        let max = record_count - 1;
        let mut zeta2theta = 0.0;
        for i in 0..2 {
            zeta2theta += 1.0 / f64::powf((i + 1) as f64, theta);
        }
        let mut this = Self {
            uniform: Uniform::new(0, record_count + 1),
            max,
            theta,
            alpha: 1.0 / (1.0 - theta),
            count_for_zeta: 2,
            zeta2theta,
            eta: (1.0 - f64::powf(2.0 / max as f64, 1.0 - theta)) / (1.0 - zeta2theta / 0.0),
            zeta_n: 0.0,
        };
        this.next(rng, false, latest);
        let _ = latest.compare_exchange(0, record_count, Ordering::SeqCst, Ordering::SeqCst);
        this
    }

    fn uniform<R: Rng>(&mut self, rng: &mut R) -> u64 {
        self.uniform.sample(rng)
    }

    fn next<R: Rng>(&mut self, rng: &mut R, is_insert: bool, latest: &AtomicU64) -> u64 {
        let max = if is_insert {
            latest.load(Ordering::Relaxed)
        } else {
            self.max
        };
        self.next_(rng, max)
    }

    fn next_<R: Rng>(&mut self, rng: &mut R, max: u64) -> u64 {
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
        (max as f64 * f64::powf(self.eta.mul_add(u, 1.0) - self.eta, self.alpha)) as u64
    }

    fn zeta(&mut self, st: u64, n: u64, initial_sum: f64) -> f64 {
        let mut sum = initial_sum;
        self.count_for_zeta = n;
        for i in st..n {
            sum += 1.0 / f64::powf((i + 1) as f64, self.theta);
        }
        sum
    }
}

const OPERATIONS: [Operation; 4] = [
    Operation::Read,
    Operation::Update,
    Operation::Insert,
    Operation::Rmw,
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Operation {
    Read,
    Update,
    Insert,
    Rmw,
}

struct Workload {
    read_proportion: u32,
    update_proportion: u32,
    insert_proportion: u32,
    rmw_proportion: u32,
    request_distribution: RequestDistribution,
}

#[derive(Clone, Copy)]
#[allow(unused)]
enum RequestDistribution {
    Uniform,
    Zipfian,
}
