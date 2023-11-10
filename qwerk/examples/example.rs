use qwerk::{
    concurrency_control::{ConcurrencyControl, Optimistic, Pessimistic},
    Database, Result,
};

fn main() -> Result<()> {
    run::<Pessimistic>()?;
    run::<Optimistic>()?;
    Ok(())
}

fn run<C: ConcurrencyControl>() -> Result<()> {
    let db = Database::<C>::new();
    let mut worker = db.spawn_worker();

    let mut txn = worker.begin_transaction();
    assert!(txn.get(b"alice")?.is_none());
    txn.insert(b"alice", b"1")?;
    txn.insert(b"bob", b"2")?;
    txn.commit()?;

    let mut txn = worker.begin_transaction();
    assert_eq!(txn.get(b"alice")?, Some(b"1".as_slice()));
    assert_eq!(txn.get(b"bob")?, Some(b"2".as_slice()));
    txn.insert(b"alice", b"2")?;
    txn.remove(b"bob")?;
    txn.abort();

    let mut txn = worker.begin_transaction();
    assert_eq!(txn.get(b"alice")?, Some(b"1".as_slice()));
    assert_eq!(txn.get(b"bob")?, Some(b"2".as_slice()));
    txn.commit()?;

    Ok(())
}
