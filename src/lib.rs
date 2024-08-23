use std::time::Duration;
use std::{fmt::Debug, sync::Arc};

use bytes::Bytes;
use color_eyre::eyre::{eyre, Result};
use moka::notification::RemovalCause;
use moka::sync::SegmentedCache;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

const SEQUENCE_TABLE_NAME: &str = "SEQUENCE";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub db_path: String,
    pub cache_num_segments: usize,
    pub cache_max_capacity: Option<u64>,
    pub cache_time_to_live: Option<u64>,
    pub cache_time_to_idle: Option<u64>,
    pub recover_table_vec: Vec<String>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            db_path: "default.db".to_string(),
            cache_num_segments: 1,
            cache_max_capacity: None,
            cache_time_to_live: None,
            cache_time_to_idle: None,
            recover_table_vec: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct Storage {
    cache: SegmentedCache<String, Bytes>,
    db: Arc<Database>,
}

unsafe impl Send for Storage {}
unsafe impl Sync for Storage {}

impl Default for Storage {
    fn default() -> Self {
        // By default, This cache will hold up to 1GiB of values.
        Self::new(&StorageConfig::default())
    }
}

impl Storage {
    pub fn new(config: &StorageConfig) -> Self {
        let db = Arc::new(Database::create(&config.db_path).unwrap());
        let db_clone = db.clone();

        let mut builder = SegmentedCache::builder(config.cache_num_segments)
            .weigher(|k: &String, v: &Bytes| (k.len() + v.len()) as u32)
            .eviction_listener(move |key, value, cause| {
                debug!(
                    "eviction event: ({:?},{:?}) , cause: {:?}",
                    key, value, cause
                );
                if cause == RemovalCause::Expired && !key.contains("@@/") {
                    let (table_name, key) = key.split_once('/').unwrap_or(("DEFAULT", &key));
                    let write_txn = db_clone.begin_write().unwrap();
                    {
                        let mut table = write_txn
                            .open_table(TableDefinition::<&str, Vec<u8>>::new(table_name))
                            .unwrap();
                        table.remove(key).unwrap();
                    }
                    write_txn.commit().unwrap();
                    debug!(
                        "Evicted event: ({:?},{:?}) , cause: {:?}",
                        key, value, cause
                    );
                }
            });
        if let Some(v) = config.cache_max_capacity {
            builder = builder.max_capacity(v)
        }
        if let Some(v) = config.cache_time_to_live {
            builder = builder.time_to_live(Duration::from_secs(v))
        }
        if let Some(v) = config.cache_time_to_idle {
            builder = builder.time_to_idle(Duration::from_secs(v))
        }

        let cache = builder.build();

        Self { cache, db }
    }

    pub fn recover(&self, table_name: &str) {
        let read_txn = self.db.begin_read().unwrap();
        if let Ok(table) = read_txn.open_table(TableDefinition::<&str, Vec<u8>>::new(table_name)) {
            let mut range = table.range::<&str>(..).unwrap();
            while let Some(Ok((k, v))) = range.next() {
                let k = k.value();
                let v = v.value();
                info!("Recover cache for table({}): {:?}", table_name, k);
                self.cache.insert(ckey(table_name, k), Bytes::from(v));
            }
        }
    }

    pub fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks();
    }
}

// SEQUENCE
impl Storage {
    pub fn next(&self, name: &str) -> u32 {
        let write_txn = self.db.begin_write().unwrap();
        let next = {
            let mut table = write_txn
                .open_table(TableDefinition::<&str, u32>::new(SEQUENCE_TABLE_NAME))
                .unwrap();
            let next = match table.get(name) {
                Ok(Some(current)) => current.value() + 1,
                _ => 1,
            };
            if table.insert(name, next).is_ok() {
                next
            } else {
                0
            }
        };
        write_txn.commit().unwrap();
        next
    }

    pub fn current(&self, name: &str) -> u32 {
        let read_txn = self.db.begin_read().unwrap();
        if let Ok(table) =
            read_txn.open_table(TableDefinition::<&str, u32>::new(SEQUENCE_TABLE_NAME))
        {
            if let Ok(Some(v)) = table.get(name) {
                v.value()
            } else {
                0
            }
        } else {
            0
        }
    }
}

// key in cache
fn ckey(table_name: &str, key: &str) -> String {
    let ckey = format!("{}/{}", table_name, key);
    ckey
}

// structured data
impl Storage {
    pub fn contains_key(&self, path: &str) -> bool {
        if self.cache.contains_key(path) {
            return true;
        }

        let (table_name, key) = path.split_once('/').unwrap_or(("DEFAULT", path));

        let read_txn = self.db.begin_read().unwrap();
        if let Ok(table) = read_txn.open_table(TableDefinition::<&str, Vec<u8>>::new(table_name)) {
            matches!(table.get(key), Ok(Some(_)))
        } else {
            false
        }
    }

    pub fn get(&self, path: &str) -> Option<Bytes> {
        if let Some(v) = self.cache.get(path) {
            return Some(v);
        }

        let (table_name, key) = path.split_once('/').unwrap_or(("DEFAULT", path));
        debug!("table_name: {:?}, key: {:?}", table_name, key);

        let read_txn = self.db.begin_read().unwrap();
        if let Ok(table) = read_txn.open_table(TableDefinition::<&str, Vec<u8>>::new(table_name)) {
            if let Ok(Some(v)) = table.get(key) {
                let value = Bytes::from(v.value());
                self.cache.insert(ckey(table_name, key), value.clone());
                return Some(value);
            }
        }

        None
    }

    pub fn insert(&self, path: &str, value: Vec<u8>) -> Result<Bytes> {
        let (table_name, key) = path.split_once('/').unwrap_or(("DEFAULT", path));
        debug!("table_name: {:?}, key: {:?}", table_name, key);

        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table =
                write_txn.open_table(TableDefinition::<&str, Vec<u8>>::new(table_name))?;
            table.insert(key, value.clone())?;
        }
        write_txn.commit()?;
        let value = Bytes::from(value);
        self.cache.insert(ckey(table_name, key), value.clone());
        Ok(value)
    }

    pub fn batch(&self, dir_path: &str, kvs: Vec<(String, Option<Vec<u8>>)>) -> Result<()> {
        let (table_name, prefix) = dir_path.split_once('/').unwrap_or(("DEFAULT", dir_path));
        debug!("table_name: {:?}, prefix: {:?}", table_name, prefix);

        let write_txn = self.db.begin_write()?;
        {
            let mut table =
                write_txn.open_table(TableDefinition::<&str, Vec<u8>>::new(table_name))?;

            for (k, v) in kvs {
                let k = &format!("{}/{}", prefix, k);
                if let Some(v) = v {
                    table.insert(k.as_str(), v.clone())?;
                    let value = Bytes::from(v);
                    self.cache.insert(ckey(table_name, k), value);
                } else {
                    table.remove(k.as_str())?;
                    self.cache.invalidate(&ckey(table_name, k));
                }
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn remove(&self, path: &str) {
        let (table_name, key) = path.split_once('/').unwrap_or(("DEFAULT", path));

        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn
                .open_table(TableDefinition::<&str, Vec<u8>>::new(table_name))
                .unwrap();
            table.remove(key).unwrap();
        }
        write_txn.commit().unwrap();

        self.cache.invalidate(&ckey(table_name, key));
    }

    pub fn cas(&self, path: &str, old: Option<Vec<u8>>, new: Option<Vec<u8>>) -> Result<()> {
        let (table_name, key) = path.split_once('/').unwrap_or(("DEFAULT", path));

        let write_txn = self.db.begin_write()?;
        {
            let mut table =
                write_txn.open_table(TableDefinition::<&str, Vec<u8>>::new(table_name))?;
            let r = if let Ok(current) = table.get(key) {
                current.map(|v| v.value()) == old
            } else {
                return Err(eyre!("cas failed"));
            };
            if r {
                match new {
                    Some(new) => {
                        table.insert(key, new.clone())?;
                        self.cache
                            .insert(ckey(table_name, key), Bytes::from(new.to_vec()));
                    }
                    None => {
                        table.remove(key)?;
                        self.cache.invalidate(&ckey(table_name, key));
                    }
                }
            }
        }
        write_txn.commit()?;

        Ok(())
    }

    pub fn scan(&self, path: &str, max_num: usize) -> Vec<(String, Bytes)> {
        let (table_name, prefix) = path.split_once('/').unwrap_or(("DEFAULT", path));
        debug!("scan table_name: {:?}, prefix: {:?}", table_name, prefix);

        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn
            .open_table(TableDefinition::<&str, Vec<u8>>::new(table_name))
            .unwrap();
        let mut scan = table.range(prefix..).unwrap();
        let mut r = vec![];
        for _ in 0..max_num {
            match scan.next() {
                Some(Ok((k, v))) => {
                    let k = k.value();
                    if k.starts_with(prefix) {
                        r.push((k.to_string(), Bytes::from(v.value())));
                    } else {
                        break;
                    }
                }
                Some(Err(e)) => {
                    warn!("sled scan error: {:?}", e);
                    break;
                }
                _ => break,
            }
        }
        r
    }

    pub fn scan_key(&self, path: &str, max_num: usize) -> Vec<String> {
        let (table_name, prefix) = path.split_once('/').unwrap_or(("DEFAULT", path));
        debug!(
            "scan_key table_name: {:?}, prefix: {:?}",
            table_name, prefix
        );

        let read_txn = self.db.begin_read().unwrap();
        let mut r = vec![];
        if let Ok(table) = read_txn.open_table(TableDefinition::<&str, Vec<u8>>::new(table_name)) {
            let mut scan = table.range(prefix..).unwrap();
            for _ in 0..max_num {
                match scan.next() {
                    Some(Ok((k, _))) => {
                        let k = k.value();
                        if k.starts_with(prefix) {
                            r.push(k.to_string());
                        } else {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        warn!("sled scan error: {:?}", e);
                        break;
                    }
                    _ => break,
                }
            }
        }
        r
    }
}

#[test]
fn sequence() {
    // Re-operation requires replacing db_path or cleaning up db_path
    let store: Storage = Storage::new(&StorageConfig {
        db_path: "test_sequence.db".to_string(),
        ..Default::default()
    });
    // The first run starts at 0
    assert_eq!(0, store.current("test"));
    for i in 0..100 {
        assert_eq!(i + 1, store.next("test"));
    }
}

#[test]
fn eviction() {
    let store: Storage = Storage::new(&StorageConfig {
        db_path: "test_eviction.db".to_string(),
        cache_time_to_live: Some(1),
        cache_max_capacity: Some(1024 * 1024 * 1024),
        cache_num_segments: 10,
        ..Default::default()
    });
    let store_clone = store.clone();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.spawn(async move {
        for i in 0..10000u32 {
            println!("insert: {}", i);
            store_clone
                .insert(
                    format!("test/{}", i).as_str(),
                    i.to_string().as_bytes().to_vec(),
                )
                .unwrap();
        }

        println!("{:?}", store_clone.cache.get(&9999u32.to_string()));
        println!("{:?}", store_clone.get(&9999u32.to_string()));
        tokio::time::sleep(Duration::from_millis(700)).await;
        println!("{:?}", store.cache.get(&9999u32.to_string()));
        println!("{:?}", store.get(&9999u32.to_string()));
        tokio::time::sleep(Duration::from_millis(300)).await;
        println!("{:?}", store.cache.get(&9999u32.to_string()));
        println!("{:?}", store.get(&9999u32.to_string()));
        tokio::time::sleep(Duration::from_millis(10)).await;
        println!("{:?}", store.cache.get(&9999u32.to_string()));
        println!("{:?}", store.get(&9999u32.to_string()));
    });
    rt.block_on(async {
        tokio::time::sleep(Duration::from_millis(2000)).await;
    });
}

#[test]
fn structured() {
    #[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
    struct Test {
        a: u32,
        b: String,
    }

    let store: Storage = Storage::new(&StorageConfig {
        db_path: "test_structured.db".to_string(),
        ..Default::default()
    });
    let test = Test {
        a: 1,
        b: "test".to_string(),
    };
    let bytes = serde_json::to_vec(&test).unwrap();
    store.insert("table_name/test", bytes).ok();
    let test_db = store.get("table_name/test").unwrap_or_default();
    let test_db = serde_json::from_slice::<Test>(&test_db).unwrap();
    assert_eq!(test, test_db);
    store.remove("table_name/test");
    assert_eq!(None, store.get("table_name/test"));
}
