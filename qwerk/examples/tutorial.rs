use anyhow::Result;
use qwerk::Database;

fn main() -> Result<()> {
    let db = Database::open("data")?;

    // By default, optimistic concurrency control is used.
    // Alternatively, you can use pessimistic concurrency control with:
    // let db = DatabaseOptions::with_concurrency_control(Pessimistic::new()).open("data")?;

    // Optimistic concurrency control is better for read-heavy workloads.
    // while pessimistic concurrency control is better for write-heavy
    // workloads.

    // You need to spawn workers to execute transactions.
    let mut worker = db.spawn_worker();

    // All the operations (insert, remove, get, and commit) can fail due to
    // conflicts with other concurrent transactions.
    // On failure, the transaction is aborted and the error is returned.
    // You can retry the transaction when this happens.
    // In this example, the operations can't fail because there are no other
    // concurrent transactions.

    let mut txn = worker.begin_transaction();
    assert!(txn.get(b"key1")?.is_none());
    txn.insert(b"key1", b"foo")?;
    txn.commit()?;

    let mut txn = worker.begin_transaction();
    assert_eq!(txn.get(b"key1")?, Some(b"foo".as_slice()));
    txn.remove(b"key1")?;
    txn.commit()?;

    // You can also abort a transaction.
    // When a transaction is aborted, all the changes made in the transaction
    // are rolled back.
    let mut txn = worker.begin_transaction();
    txn.insert(b"key1", b"foo")?;
    txn.insert(b"key2", b"bar")?;
    txn.abort();

    // When committing multiple transactions in a batch, you can make use of
    // `precommit` that commits the transaction but does not wait for
    // durability. In this way, writes to the disk can be batched and
    // the performance is improved.

    let mut txn = worker.begin_transaction();
    assert!(txn.get(b"key1")?.is_none());
    txn.insert(b"key3", b"baz")?;
    let commit_epoch1 = txn.precommit()?;

    let mut txn = worker.begin_transaction();
    assert_eq!(txn.get(b"key3")?, Some(b"baz".as_slice()));
    txn.insert(b"key4", b"qux")?;
    let commit_epoch2 = txn.precommit()?;

    assert!(commit_epoch1 <= commit_epoch2);

    // Transactions committed with `precommit` are made durable periodically
    // by background threads or when `flush` is called.
    // You can check if the transaction became durable by comparing
    // the commit epoch returned by `precommit` and the durable epoch of
    // the database.

    let durable_epoch = db.flush()?;
    assert!(durable_epoch >= commit_epoch2);

    Ok(())
}
