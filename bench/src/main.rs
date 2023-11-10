use clap::{arg, Parser, ValueEnum};
use qwerk::{
    concurrency_control::{ConcurrencyControl, Optimistic, Pessimistic},
    Database,
};
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
    #[arg(short, long, default_value_t = 8)]
    threads: usize,

    #[arg(short, long, default_value_t = 100000)]
    records: u64,

    #[arg(short, long, default_value_t = 8)]
    payload: usize,

    #[arg(short, long, default_value_t = 2000)]
    duration: u64,

    #[arg(short, long, default_value_t = 4)]
    working_set: usize,

    #[arg(short, long, default_value_t = 0.5)]
    contention: f64,

    #[arg(long, value_enum, default_value_t = Protocol::Pessimistic)]
    protocol: Protocol,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Protocol {
    Optimistic,
    Pessimistic,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.protocol {
        Protocol::Optimistic => run_benchmark::<Optimistic>(cli),
        Protocol::Pessimistic => run_benchmark::<Pessimistic>(cli),
    }

    Ok(())
}

fn run_benchmark<C>(cli: Cli)
where
    C: ConcurrencyControl + Default + Send + Sync + 'static,
    C::Record: Send + Sync,
{
    let workload = Workload {
        read_proportion: 50,
        update_proportion: 50,
        insert_proportion: 0,
        rmw_proportion: 0,
        request_distribution: RequestDistribution::Zipfian,
        record_count: cli.records,
    };

    let db: Database<C> = Database::new();
    struct Shared<C: ConcurrencyControl> {
        db: Database<C>,
        barrier: Barrier,
        is_running: AtomicBool,
        latest: AtomicU64,
    }
    let shared = Arc::new(Shared {
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
            let shared = shared.clone();
            let from = workload.record_count * thread_index as u64 / num_threads as u64;
            let to = workload.record_count * (thread_index as u64 + 1) / num_threads as u64;
            let payload = vec![0; cli.payload];
            std::thread::spawn(move || {
                let mut worker = shared.db.spawn_worker();
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
            let shared = shared.clone();
            std::thread::spawn(move || {
                #[cfg(feature = "affinity")]
                assert!(core_affinity::set_for_current(core_id));
                let mut worker = shared.db.spawn_worker();
                let mut rng = rand::rngs::SmallRng::from_entropy();
                let mut generator = NumberGenerator::new(
                    &mut rng,
                    &shared.latest,
                    workload.record_count,
                    cli.contention,
                );
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

                shared.barrier.wait();
                while shared.is_running.load(Ordering::SeqCst) {
                    let op = OPERATIONS[op_dist.sample(&mut rng)];
                    for _ in 0..cli.working_set {
                        let key = if op == Operation::Insert {
                            shared.latest.fetch_add(1, Ordering::SeqCst)
                        } else {
                            match workload.request_distribution {
                                RequestDistribution::Uniform => generator.uniform(&mut rng),
                                RequestDistribution::Zipfian => {
                                    generator.next(&mut rng, has_insert, &shared.latest)
                                }
                            }
                        };
                        keys.push(format!("{}", key).into_bytes());
                    }

                    let mut txn = worker.begin_transaction();
                    for key in keys.drain(..) {
                        match op {
                            Operation::Read => {
                                let _ = txn.get(key);
                            }
                            Operation::Update | Operation::Insert => {
                                let _ = txn.insert(key, &payload);
                            }
                            Operation::Rmw => {
                                let _ = txn.get(&key);
                                let _ = txn.insert(key, &payload);
                            }
                        }
                    }
                    let result = txn.commit();
                    if shared.is_running.load(Ordering::Relaxed) {
                        match result {
                            Ok(()) => stats.num_commits += 1,
                            Err(_) => stats.num_aborts += 1,
                        }
                    } else {
                        break;
                    }
                }
                stats
            })
        })
        .collect();

    eprintln!("Start");
    shared.barrier.wait();

    let start = Instant::now();
    std::thread::sleep(Duration::from_millis(cli.duration));
    shared.is_running.store(false, Ordering::SeqCst);
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
        threads: u64,
        clients: usize,
        handler: bool,
        theta: f64,
    }
    let mut stdout = std::io::stdout().lock();
    serde_json::ser::to_writer(
        &mut stdout,
        &Summary {
            etime: elapsed.as_millis() as u64,
            commits: stats.num_commits,
            aborts: stats.num_aborts,
            tps,
            workload: "a".to_owned(),
            protocol: format!("{:?}", cli.protocol),
            threads: 1,
            clients: cli.threads,
            handler: true,
            theta: cli.contention,
        },
    )
    .unwrap();
    stdout.write_all(b"\n").unwrap();
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
    record_count: u64,
}

#[derive(Clone, Copy)]
#[allow(unused)]
enum RequestDistribution {
    Uniform,
    Zipfian,
}
