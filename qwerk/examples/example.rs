use anyhow::Result;
use qwerk::{ConcurrencyControl, Database, Optimistic, Pessimistic};

fn main() -> Result<()> {
    // You can choose either pessimistic or optimistic concurrency control.
    // Pessimistic concurrency control is better for write-heavy workloads,
    // while optimistic concurrency control is better for read-heavy workloads.

    run::<Pessimistic>()?;
    run::<Optimistic>()?;
    Ok(())
}

fn run<C: ConcurrencyControl>() -> Result<()> {
    let db = Database::<C>::open("data")?;

    // You need to spawn workers to perform operations on the database.
    let mut worker = db.spawn_worker();

    // All the operations (insert, remove, get, and commit) can fail due to
    // conflicts with other concurrent transactions.
    // On failure, the transaction is aborted and the error is returned.
    // You can retry the transaction when this happens.
    // In this example, the operations can't fail because there are no other
    // concurrent transactions.

    let mut txn = worker.begin_transaction();
    txn.remove(b"alice")?;
    txn.remove(b"bob")?;
    txn.remove(b"carol")?;
    txn.commit()?;

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

    // You can also abort a transaction.
    // When a transaction is aborted, all the changes made in the transaction
    // are rolled back.
    let mut txn = worker.begin_transaction();
    txn.insert(b"alice", b"foobar")?;
    txn.remove(b"bob")?;
    txn.abort();

    // When committing multiple transactions in a batch, you can make use of
    // `precommit` that commits the transaction but does not wait for
    // durability. In this way, writes to the disk can be batched and
    // the performance is improved.
    // Transactions are made durable periodically by background threads
    // or when `flush` is called.
    // You can check if the transaction became durable by comparing
    // the commit epoch returned by `precommit` and the durable epoch of
    // the database.

    let mut txn = worker.begin_transaction();
    assert_eq!(txn.get(b"alice")?, Some(b"foo".as_slice()));
    assert_eq!(txn.get(b"bob")?, Some(b"bar".as_slice()));
    txn.insert(b"alice", b"qux")?;
    txn.remove(b"bob")?;
    let commit_epoch1 = txn.precommit()?;

    let mut txn = worker.begin_transaction();
    assert_eq!(txn.get(b"alice")?, Some(b"qux".as_slice()));
    assert!(txn.get(b"bob")?.is_none());
    txn.insert(b"carol", b"quux")?;
    let commit_epoch2 = txn.precommit()?;

    assert!(commit_epoch1 <= commit_epoch2);

    let durable_epoch = db.flush()?;
    assert!(durable_epoch >= commit_epoch2);

    Ok(())
}
