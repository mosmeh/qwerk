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
