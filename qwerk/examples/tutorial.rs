use anyhow::Result;
use qwerk::Database;

fn main() -> Result<()> {
    let db = Database::open("data")?;

    // By default, optimistic concurrency control is used.
    // Alternatively, you can use pessimistic concurrency control with:
    // let db = Database::options()
    //     .concurrency_control(Pessimistic::new())
    //     .open("data")?;

    // Optimistic concurrency control is better for read-heavy workloads,
    // while pessimistic concurrency control is better for write-heavy
    // workloads.

    // You need to spawn workers to execute transactions.
    let mut worker = db.worker()?;

    // All the operations (insert, remove, get, and commit) can fail due to
    // conflicts with other concurrent transactions.
    // On failure, the transaction is aborted and the error is returned.
    // You can retry the transaction when this happens.
    // In this example, the operations can't fail because there are no other
    // concurrent transactions.

    let mut txn = worker.transaction();
    assert!(txn.get(b"key1")?.is_none());
    txn.insert(b"key1", b"foo")?;
    txn.commit()?;

    let mut txn = worker.transaction();
    assert_eq!(txn.get(b"key1")?, Some(b"foo".as_slice()));
    txn.remove(b"key1")?;
    txn.commit()?;

    // You can also abort a transaction.
    // When a transaction is aborted, all the changes made in the transaction
    // are rolled back.
    let mut txn = worker.transaction();
    txn.insert(b"key1", b"foo")?;
    txn.insert(b"key2", b"bar")?;
    txn.abort();

    // When committing multiple transactions in a batch, you can choose to
    // commit them asynchronously. In this way, transactions are committed
    // in the background and the latencies of the commits are hidden.

    let mut txn = worker.transaction();
    assert!(txn.get(b"key1")?.is_none());
    txn.insert(b"key3", b"baz")?;
    txn.set_async_commit(true);
    let commit_epoch1 = txn.commit()?;

    let mut txn = worker.transaction();
    assert_eq!(txn.get(b"key3")?, Some(b"baz".as_slice()));
    txn.insert(b"key4", b"qux")?;
    txn.set_async_commit(true);
    let commit_epoch2 = txn.commit()?;

    assert!(commit_epoch1 <= commit_epoch2);

    // When you commit transactions asynchronously, the transactions are
    // committed periodically by background threads or when `commit_pending` is
    // called.
    // You can check if the transaction has been committed by comparing
    // the commit epoch returned by `commit` and the committed epoch of the
    // database.

    let committed_epoch = db.commit_pending()?;
    assert!(committed_epoch >= commit_epoch2);
    assert!(db.committed_epoch() >= committed_epoch);

    Ok(())
}
