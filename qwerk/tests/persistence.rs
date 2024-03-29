use qwerk::{Database, Error};
use std::fs::File;
use tempfile::tempdir;

#[test]
fn open_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data");
    File::create(&path).unwrap();
    assert!(Database::open(&path).is_err());
}

#[test]
fn corrupted_database() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data");
    std::fs::create_dir(&path).unwrap();

    // Empty durable_epoch file.
    File::create(path.join("durable_epoch")).unwrap();

    assert!(Database::open(path).is_err());
}

#[test]
fn concurrent_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data");
    {
        let _db = Database::open(&path).unwrap();
        assert!(matches!(
            Database::open(&path),
            Err(Error::DatabaseAlreadyOpen)
        ));
    }
    Database::open(path).unwrap();
}

#[test]
fn commit_pending() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path().join("data")).unwrap();
    let mut worker1 = db.worker().unwrap();
    let mut worker2 = db.worker().unwrap();

    let mut txn = worker1.transaction();
    txn.insert(b"foo", b"bar").unwrap();
    let commit_epoch1 = txn.commit().unwrap();
    assert!(db.committed_epoch() >= commit_epoch1);

    let mut txn = worker2.transaction();
    txn.insert(b"baz", b"qux").unwrap();
    txn.set_async_commit(true);
    let commit_epoch2 = txn.commit().unwrap();
    assert!(commit_epoch1 <= commit_epoch2);

    let committed_epoch = db.commit_pending().unwrap();
    assert!(committed_epoch >= commit_epoch2);
    assert!(db.committed_epoch() >= committed_epoch);
}

#[test]
fn commit_pending_on_drop() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data");

    let db = Database::open(&path).unwrap();
    let mut worker = db.worker().unwrap();
    let mut txn = worker.transaction();
    txn.insert(b"foo", b"bar").unwrap();
    txn.set_async_commit(true);
    let commit_epoch = txn.commit().unwrap();
    drop(worker);
    drop(db);

    let db = Database::open(path).unwrap();
    assert!(db.committed_epoch() >= commit_epoch);
}

#[test]
fn recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data");

    let db = Database::open(&path).unwrap();
    let mut worker = db.worker().unwrap();

    let mut txn = worker.transaction();
    assert!(txn.get(b"foo").unwrap().is_none());
    txn.commit().unwrap();

    let mut txn = worker.transaction();
    txn.insert(b"foo", b"bar").unwrap();
    txn.insert(b"baz", b"qux").unwrap();
    txn.commit().unwrap();

    let mut txn = worker.transaction();
    txn.remove(b"baz").unwrap();
    txn.set_async_commit(true);
    txn.commit().unwrap();

    let committed_epoch1 = db.commit_pending().unwrap();

    drop(worker);
    drop(db);

    let db = Database::open(path).unwrap();
    let committed_epoch2 = db.committed_epoch();
    assert!(committed_epoch1 <= committed_epoch2);

    let mut worker = db.worker().unwrap();
    let mut txn = worker.transaction();
    assert_eq!(txn.get(b"foo").unwrap(), Some(b"bar".as_ref()));
    assert!(txn.get(b"baz").unwrap().is_none());
    let commit_epoch = txn.commit().unwrap();
    assert!(commit_epoch > committed_epoch2);
}
