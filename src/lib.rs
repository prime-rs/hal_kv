use std::time::Duration;
use std::{fmt::Debug, sync::Arc};

use bytes::Bytes;
use color_eyre::eyre::{eyre, Result};
use moka::notification::RemovalCause;
use moka::sync::SegmentedCache;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sled::{CompareAndSwapSuccess, Db};
use tracing::{debug, info, warn};

const SEQUENCE_TREE_NAME: &str = "SEQUENCE";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub db_path: String,
    pub cache_num_segments: usize,
    pub cache_max_capacity: Option<u64>,
    pub cache_time_to_live: Option<u64>,
    pub cache_time_to_idle: Option<u64>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            db_path: "default.db".to_string(),
            cache_num_segments: 1,
            cache_max_capacity: None,
            cache_time_to_live: None,
            cache_time_to_idle: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Storage {
    cache: SegmentedCache<String, Bytes>,
    db: Db,
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
        let db = sled::open(&config.db_path).unwrap();
        let db_clone = Arc::new(Mutex::new(db.clone()));

        let mut builder = SegmentedCache::builder(config.cache_num_segments)
            .weigher(|k: &String, v: &Bytes| (k.len() + v.len()) as u32)
            .eviction_listener(move |key, value, cause| {
                debug!(
                    "eviction event: ({:?},{:?}) , cause: {:?}",
                    key, value, cause
                );
                match cause {
                    RemovalCause::Explicit | RemovalCause::Expired => {
                        if !key.contains("@@/") {
                            let (tree_name, key) = key.split_once('/').unwrap();
                            let tree = db_clone.lock().open_tree(tree_name).unwrap();
                            tree.remove(key).unwrap();
                            debug!(
                                "Evicted event: ({:?},{:?}) , cause: {:?}",
                                key, value, cause
                            );
                        }
                    }
                    _ => {}
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

    pub fn recover_root(&self) {
        self.db.iter().for_each(|r| {
            if let Ok((k, v)) = r {
                let key = String::from_utf8_lossy(&k).to_string();
                debug!("Recover cache for root: {:?}", key);
                self.cache.insert(key, Bytes::from(v.to_vec()));
            }
        });
    }

    pub fn recover(&self, tree_name: &str) {
        if let Ok(tree) = self.db.open_tree(tree_name) {
            tree.iter().for_each(|r| {
                if let Ok((k, v)) = r {
                    let key = String::from_utf8_lossy(&k);
                    info!("Recover cache for tree({}): {:?}", tree_name, key);
                    self.cache
                        .insert(ckey(tree_name, &key), Bytes::from(v.to_vec()));
                }
            });
        }
    }

    pub fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks();
        self.db.flush().unwrap();
    }
}

// SEQUENCE
impl Storage {
    pub fn next(&self, name: &str) -> u32 {
        let tree = self.db.open_tree(SEQUENCE_TREE_NAME).unwrap();
        match tree.get(name).ok().and_then(|v| {
            v.and_then(|v| match v.to_vec().try_into() {
                Ok(v) => Some(u32::from_be_bytes(v)),
                Err(_) => None,
            })
        }) {
            Some(next) => {
                if tree.insert(name, (next + 1).to_be_bytes().to_vec()).is_ok() {
                    next + 1
                } else {
                    0
                }
            }
            None => {
                if tree.insert(name, 1u32.to_be_bytes().to_vec()).is_ok() {
                    1
                } else {
                    0
                }
            }
        }
    }

    pub fn current(&self, name: &str) -> u32 {
        let tree = self.db.open_tree(SEQUENCE_TREE_NAME).unwrap();
        if let Ok(Some(v)) = tree.get(name) {
            if let Ok(v) = v.to_vec().try_into() {
                return u32::from_be_bytes(v);
            }
        }
        0
    }
}

// key in cache
fn ckey(tree_name: &str, key: &str) -> String {
    let ckey = format!("{}/{}", tree_name, key);
    ckey
}

// structured data
impl Storage {
    pub fn contains_key(&self, path: &str) -> bool {
        if self.cache.contains_key(path) {
            return true;
        }

        let (tree_name, key) = path.split_once('/').unwrap();
        let tree = self.db.open_tree(tree_name).unwrap();
        if let Ok(r) = tree.contains_key(key) {
            return r;
        }

        false
    }

    pub fn get(&self, path: &str) -> Option<Bytes> {
        if let Some(v) = self.cache.get(path) {
            return Some(v);
        }

        let (tree_name, key) = path.split_once('/').unwrap();
        debug!("tree_name: {:?}, key: {:?}", tree_name, key);
        let tree = self.db.open_tree(tree_name).unwrap();
        if let Ok(Some(v)) = tree.get(key) {
            let value = Bytes::from(v.to_vec());
            self.cache.insert(ckey(tree_name, key), value.clone());
            return Some(value);
        }

        None
    }

    pub fn insert(&self, path: &str, value: Vec<u8>) -> Result<Bytes> {
        let (tree_name, key) = path.split_once('/').unwrap();
        debug!("tree_name: {:?}, key: {:?}", tree_name, key);
        let tree = self.db.open_tree(tree_name)?;
        tree.insert(key, value.clone())?;
        let value = Bytes::from(value);
        self.cache.insert(ckey(tree_name, key), value.clone());
        Ok(value)
    }

    pub fn batch(&self, dir_path: &str, kvs: Vec<(String, Option<Vec<u8>>)>) -> Result<()> {
        let (tree_name, prefix) = dir_path.split_once('/').unwrap();
        debug!("tree_name: {:?}, prefix: {:?}", tree_name, prefix);
        let tree = self.db.open_tree(tree_name)?;
        let mut batch = sled::Batch::default();
        for (k, v) in kvs {
            let k = &format!("{}/{}", prefix, k);
            if let Some(v) = v {
                batch.insert(k.clone(), v.clone());
                let value = Bytes::from(v);
                self.cache.insert(ckey(tree_name, k), value);
            } else {
                self.cache.remove(&ckey(tree_name, k));
                batch.remove(k);
            }
        }
        tree.apply_batch(batch)?;
        Ok(())
    }

    pub fn remove(&self, path: &str) {
        let (tree_name, key) = path.split_once('/').unwrap();
        let tree = self.db.open_tree(tree_name).unwrap();
        tree.remove(key).unwrap();
        self.cache.remove(&ckey(tree_name, key));
    }

    pub fn cas(&self, path: &str, old: Option<Vec<u8>>, new: Option<Vec<u8>>) -> Result<()> {
        let (tree_name, key) = path.split_once('/').unwrap();
        let tree = self.db.open_tree(tree_name)?;
        let cas_result = tree.compare_and_swap(key, old, new);
        match cas_result {
            Ok(Ok(CompareAndSwapSuccess { new_value, .. })) => match new_value {
                Some(new) => {
                    self.cache
                        .insert(ckey(tree_name, key), Bytes::from(new.to_vec()));
                }
                None => {
                    self.cache.remove(&ckey(tree_name, key));
                }
            },
            _ => {
                return Err(eyre!("cas failed"));
            }
        }
        Ok(())
    }

    pub fn scan(&self, path: &str, max_num: usize) -> Vec<(String, Bytes)> {
        let (tree_name, prefix) = path.split_once('/').unwrap();
        debug!("tree_name: {:?}, prefix: {:?}", tree_name, prefix);
        let tree = self.db.open_tree(tree_name).unwrap();
        let mut scan = tree.scan_prefix(prefix);
        let mut r = vec![];
        for _ in 0..max_num {
            match scan.next() {
                Some(Ok((k, v))) => r.push((
                    String::from_utf8_lossy(&k).to_string(),
                    Bytes::from(v.to_vec()),
                )),
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
        let (tree_name, prefix) = path.split_once('/').unwrap();
        debug!("tree_name: {:?}, prefix: {:?}", tree_name, prefix);
        let tree = self.db.open_tree(tree_name).unwrap();
        let mut scan = tree.scan_prefix(prefix);
        let mut r = vec![];
        for _ in 0..max_num {
            match scan.next() {
                Some(Ok((k, _))) => r.push(String::from_utf8_lossy(&k).to_string()),
                Some(Err(e)) => {
                    warn!("sled scan error: {:?}", e);
                    break;
                }
                _ => break,
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
            store_clone
                .cache
                .insert(i.to_string(), Bytes::from(i.to_string()));
            store_clone
                .db
                .insert(i.to_string(), i.to_string().as_bytes())
                .unwrap();
        }

        println!("{:?}", store_clone.cache.get(&9999u32.to_string()));
        println!("{:?}", store_clone.db.get(9999u32.to_string()));
        tokio::time::sleep(Duration::from_millis(700)).await;
        println!("{:?}", store.cache.get(&9999u32.to_string()));
        println!("{:?}", store.db.get(9999u32.to_string()));
        tokio::time::sleep(Duration::from_millis(300)).await;
        println!("{:?}", store.cache.get(&9999u32.to_string()));
        println!("{:?}", store.db.get(9999u32.to_string()));
        tokio::time::sleep(Duration::from_millis(10)).await;
        println!("{:?}", store.cache.get(&9999u32.to_string()));
        println!("{:?}", store.db.get(9999u32.to_string()));
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
    store.insert("tree_name/test", bytes).ok();
    let test_db = store.get("tree_name/test").unwrap_or_default();
    let test_db = serde_json::from_slice::<Test>(&test_db).unwrap();
    assert_eq!(test, test_db);
    store.remove("tree_name/test");
    assert_eq!(None, store.get("tree_name/test"));
}
