use qwerk::{ConcurrencyControl, Database, Optimistic, Pessimistic};

#[test]
fn pessimistic() {
    test(Pessimistic::new());
}

#[test]
fn optimistic() {
    test(Optimistic::new());
}

fn test<C: ConcurrencyControl>(concurrency_control: C) {
    let db = Database::options()
        .concurrency_control(concurrency_control)
        .open_temporary();

    let mut worker = db.worker().unwrap();

    let mut txn = worker.transaction();
    assert!(txn.get(b"alice").unwrap().is_none());
    txn.insert(b"alice", b"foo").unwrap();
    assert_eq!(txn.get(b"alice").unwrap(), Some(b"foo".as_slice()));
    txn.insert(b"bob", b"bar").unwrap();
    txn.insert(b"carol", b"baz").unwrap();
    txn.remove(b"carol").unwrap();
    assert!(txn.get(b"carol").unwrap().is_none());
    txn.commit().unwrap();

    let mut txn = worker.transaction();
    assert_eq!(txn.get(b"alice").unwrap(), Some(b"foo".as_slice()));
    assert_eq!(txn.get(b"bob").unwrap(), Some(b"bar".as_slice()));
    assert!(txn.get(b"carol").unwrap().is_none());
    txn.commit().unwrap();

    let mut txn = worker.transaction();
    txn.insert(b"alice", b"foobar").unwrap();
    txn.remove(b"bob").unwrap();
    txn.abort();

    let mut txn = worker.transaction();
    assert_eq!(txn.get(b"alice").unwrap(), Some(b"foo".as_slice()));
    assert_eq!(txn.get(b"bob").unwrap(), Some(b"bar".as_slice()));
    txn.insert(b"alice", b"qux").unwrap();
    txn.remove(b"bob").unwrap();
    let commit_epoch1 = txn.commit().unwrap();

    let mut txn = worker.transaction();
    assert_eq!(txn.get(b"alice").unwrap(), Some(b"qux".as_slice()));
    assert!(txn.get(b"bob").unwrap().is_none());
    txn.insert(b"carol", b"quux").unwrap();
    let commit_epoch2 = txn.commit().unwrap();

    assert!(commit_epoch1 <= commit_epoch2);
}

#[test]
#[should_panic(expected = "Transaction dropped without being committed or aborted")]
fn drop_without_commit_or_abort() {
    let db = Database::open_temporary();
    let mut worker = db.worker().unwrap();
    let _txn = worker.transaction();
}

#[test]
#[should_panic(expected = "foobar")]
fn drop_with_panic() {
    let db = Database::open_temporary();
    let mut worker = db.worker().unwrap();
    let _txn = worker.transaction();
    panic!("foobar"); // Does not result in double panic.
}
