# qwerk

[![build](https://github.com/mosmeh/qwerk/workflows/build/badge.svg)](https://github.com/mosmeh/qwerk/actions)

An embedded transactional key-value store.

## Features

- ACID transactions with strict serializability
- Optimized for multi-threaded workloads with multiple concurrent readers and writers
- Persistence to the disk

## Example

```rust
use std::sync::Arc;
use qwerk::{Database, Result};

fn main() -> Result<()> {
    let db = Arc::new(Database::open_temporary());

    let mut worker = db.worker()?;
    let mut txn = worker.transaction();
    txn.insert(b"key", b"value")?;
    txn.commit()?;

    let db = db.clone();
    std::thread::spawn(move || {
        let mut worker = db.worker()?;
        let mut txn = worker.transaction();
        assert_eq!(txn.get(b"key")?, Some(b"value".as_slice()));
        txn.commit()
    }).join().unwrap()?;

    Ok(())
}
```
