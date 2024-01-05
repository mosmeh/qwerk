use itertools::Itertools;
use proptest::prelude::*;
use qwerk::{ConcurrencyControl, Database, Epoch, Error, Pessimistic};
use std::collections::BTreeMap;
use Operation::*;

#[cfg(shuttle)]
use shuttle::{sync::Arc, thread};

#[cfg(not(shuttle))]
use std::{sync::Arc, thread};

#[test]
fn shuttle_regression1() {
    test(vec![vec![Insert(0, 0)], vec![Get(0)]]);
}

#[test]
fn shuttle_regression2() {
    test(vec![
        vec![Insert(0, 0)],
        vec![Insert(1, 0)],
        vec![Get(0), Insert(1, 0)],
    ]);
}

const MAX_TXNS: usize = 2;
const MAX_OPS_PER_TXN: usize = 1;
const NUM_KEYS: Key = 4;
const NUM_ITERATIONS: usize = 100;

proptest! {
#[test]
fn shuttle_concurrent_transactions(scenario in arbitrary_scenario(MAX_TXNS, MAX_OPS_PER_TXN, NUM_KEYS)) {
    test(scenario);
}
}

type Key = u8;
type Value = u8;

#[derive(Debug, Clone)]
enum Operation {
    Get(Key),
    Insert(Key, Value),
    Remove(Key),
}

fn arbitrary_op(num_keys: Key) -> impl Strategy<Value = Operation> {
    let key_range = 0..num_keys;
    prop_oneof![
        key_range.clone().prop_map(Operation::Get),
        (key_range.clone(), any::<Value>()).prop_map(|(key, value)| Operation::Insert(key, value)),
        key_range.prop_map(Operation::Remove),
    ]
}

fn arbitrary_ops(max_ops: usize, num_keys: Key) -> impl Strategy<Value = Vec<Operation>> {
    prop::collection::vec(arbitrary_op(num_keys), 1..=max_ops)
}

type Scenario = Vec<Vec<Operation>>;

fn arbitrary_scenario(
    max_txns: usize,
    max_ops_per_txn: usize,
    num_keys: Key,
) -> impl Strategy<Value = Scenario> {
    prop::collection::vec(arbitrary_ops(max_ops_per_txn, num_keys), 2..=max_txns)
}

type TransactionIndex = usize;
type ReadHistory = Vec<(Key, Option<Value>)>;

#[derive(Debug, PartialEq, Hash)]
struct ScenarioOutcome {
    records: BTreeMap<Key, Value>,
    read_histories: BTreeMap<TransactionIndex, ReadHistory>,
}

fn test(scenario: Scenario) {
    fn inner<C: ConcurrencyControl>(scenario: &Scenario, concurrency_control: C) {
        let (db_outcome, commit_epochs) = run_db(scenario, concurrency_control);
        'outer: for perm in commit_epochs.iter().permutations(commit_epochs.len()) {
            let mut prev_epoch = None;
            for (_, &epoch) in &perm {
                if prev_epoch.map_or(false, |prev_epoch| prev_epoch > epoch) {
                    continue 'outer;
                }
                prev_epoch = Some(epoch);
            }
            let reduced_scenario = perm
                .iter()
                .copied()
                .map(|(i, _)| (*i, scenario[*i].as_slice()));
            let reference_model_outcome = run_txn_on_reference_model(reduced_scenario);
            if reference_model_outcome == db_outcome {
                return;
            }
        }
        panic!(
            "Serializability violation:
Scenario: {scenario:?}
Outcome:  {db_outcome:?}"
        );
    }

    println!("{scenario:?}");

    #[cfg(shuttle)]
    shuttle::check_random(move || inner(&scenario, Pessimistic::new()), NUM_ITERATIONS);

    #[cfg(not(shuttle))]
    inner(&scenario, Optimistic::new());
}

fn run_db<C: ConcurrencyControl>(
    scenario: &Scenario,
    concurrency_control: C,
) -> (ScenarioOutcome, BTreeMap<TransactionIndex, Epoch>) {
    let db = Arc::new(
        Database::options()
            .concurrency_control(concurrency_control)
            .open_temporary(),
    );
    let threads: Vec<_> = scenario
        .iter()
        .map(|ops| {
            let db = db.clone();
            let ops = (*ops).clone();
            thread::spawn(move || run_txn_on_db(&db, &ops))
        })
        .collect();

    let mut read_histories = BTreeMap::new();
    let mut commit_epochs = BTreeMap::new();
    for (i, handle) in threads.into_iter().enumerate() {
        match handle.join().unwrap() {
            Ok((commit_epoch, read_history)) => {
                commit_epochs.insert(i, commit_epoch);
                read_histories.insert(i, read_history);
            }
            Err(Error::TransactionNotSerializable) => {}
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }

    let mut records = BTreeMap::new();
    {
        let mut worker = db.worker().unwrap();
        for key in 0..Key::MAX {
            let mut txn = worker.transaction();
            if let Some(value) = txn.get(to_bytes(key)).unwrap() {
                records.insert(key, to_u8(value));
            }
            txn.commit().unwrap();
        }
    }

    (
        ScenarioOutcome {
            records,
            read_histories,
        },
        commit_epochs,
    )
}

fn run_txn_on_db<C: ConcurrencyControl>(
    db: &Arc<Database<C>>,
    ops: &[Operation],
) -> qwerk::Result<(Epoch, ReadHistory)> {
    let mut worker = db.worker().unwrap();
    let mut txn = worker.transaction();
    let mut read_history = Vec::new();
    for op in ops {
        match op {
            Operation::Get(key) => {
                let value = txn.get(to_bytes(*key))?.map(to_u8);
                read_history.push((*key, value));
            }
            Operation::Insert(key, value) => {
                txn.insert(to_bytes(*key), to_bytes(*value))?;
            }
            Operation::Remove(key) => {
                txn.remove(to_bytes(*key))?;
            }
        }
    }
    let commit_epoch = txn.commit()?;
    Ok((commit_epoch, read_history))
}

fn run_txn_on_reference_model<'a>(
    scenario: impl Iterator<Item = (TransactionIndex, &'a [Operation])>,
) -> ScenarioOutcome {
    let mut records = BTreeMap::new();
    let mut read_histories = BTreeMap::new();
    for (i, ops) in scenario {
        let mut read_history = Vec::new();
        for op in ops {
            match op {
                Operation::Get(key) => {
                    let value = records.get(key);
                    read_history.push((*key, value.copied()));
                }
                Operation::Insert(key, value) => {
                    records.insert(*key, *value);
                }
                Operation::Remove(key) => {
                    records.remove(key);
                }
            }
        }
        read_histories.insert(i, read_history);
    }
    ScenarioOutcome {
        records,
        read_histories,
    }
}

fn to_bytes(v: u8) -> [u8; 1] {
    [v]
}

fn to_u8(bytes: &[u8]) -> u8 {
    u8::from_ne_bytes(bytes.try_into().unwrap())
}
