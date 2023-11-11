use qwerk::{ConcurrencyControl, Database, Optimistic, Pessimistic, Result};

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
    txn.insert(b"alice", b"foo")?;
    assert_eq!(txn.get(b"alice")?, Some(b"foo".as_slice()));
    txn.insert(b"bob", b"bar")?;
    txn.insert(b"carol", b"baz")?;
    txn.remove(b"carol")?;
    assert!(txn.get(b"carol")?.is_none());
    txn.commit()?;

    let mut txn = worker.begin_transaction();
    assert_eq!(txn.get(b"alice")?, Some(b"foo".as_slice()));
    assert_eq!(txn.get(b"bob")?, Some(b"bar".as_slice()));
    assert!(txn.get(b"carol")?.is_none());
    txn.commit()?;

    let mut txn = worker.begin_transaction();
    txn.insert(b"alice", b"qux")?;
    txn.remove(b"bob")?;
    txn.abort();

    let mut txn = worker.begin_transaction();
    assert_eq!(txn.get(b"alice")?, Some(b"foo".as_slice()));
    assert_eq!(txn.get(b"bob")?, Some(b"bar".as_slice()));
    txn.insert(b"alice", b"quux")?;
    txn.remove(b"bob")?;
    txn.commit()?;

    let mut txn = worker.begin_transaction();
    assert_eq!(txn.get(b"alice")?, Some(b"quux".as_slice()));
    assert!(txn.get(b"bob")?.is_none());
    txn.commit()?;

    Ok(())
}
