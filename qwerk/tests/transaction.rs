use qwerk::{ConcurrencyControl, DatabaseOptions, Optimistic, Pessimistic};

#[test]
fn pessimistic() {
    test(Pessimistic::new());
}

#[test]
fn optimistic() {
    test(Optimistic::new());
}

fn test<C: ConcurrencyControl>(concurrency_control: C) {
    let db = DatabaseOptions::with_concurrency_control(concurrency_control).open_temporary();

    let mut worker = db.worker();

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
    txn.remove(b"alice").unwrap();
    drop(txn); // abort

    let mut txn = worker.transaction();
    assert_eq!(txn.get(b"alice").unwrap(), Some(b"foo".as_slice()));
    assert_eq!(txn.get(b"bob").unwrap(), Some(b"bar".as_slice()));
    txn.insert(b"alice", b"qux").unwrap();
    txn.remove(b"bob").unwrap();
    let commit_epoch1 = txn.precommit().unwrap();

    let mut txn = worker.transaction();
    assert_eq!(txn.get(b"alice").unwrap(), Some(b"qux".as_slice()));
    assert!(txn.get(b"bob").unwrap().is_none());
    txn.insert(b"carol", b"quux").unwrap();
    let commit_epoch2 = txn.precommit().unwrap();

    assert!(commit_epoch1 <= commit_epoch2);
}
