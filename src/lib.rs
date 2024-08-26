use std::time::Duration;
use std::{fmt::Debug, sync::Arc};

use bytes::Bytes;
use color_eyre::eyre::{eyre, Result};
use moka::notification::RemovalCause;
use moka::sync::SegmentedCache;
use serde::{Deserialize, Serialize};
use surrealkv::{Options, Store};
use tracing::debug;

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

#[derive(Clone)]
pub struct Storage {
    cache: SegmentedCache<Bytes, Bytes>,
    db: Arc<Store>,
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
        let mut opts = Options::new();
        opts.dir = config.db_path.clone().into();
        let db = Arc::new(Store::new(opts).unwrap());
        let db_clone = db.clone();

        let mut builder = SegmentedCache::builder(config.cache_num_segments)
            .weigher(|k: &Bytes, v: &Bytes| (k.len() + v.len()) as u32)
            .eviction_listener(move |key, value, cause| {
                debug!(
                    "eviction event: ({:?},{:?}) , cause: {:?}",
                    key, value, cause
                );
                match cause {
                    RemovalCause::Explicit | RemovalCause::Expired => {
                        let db_clone = db_clone.clone();
                        tokio::spawn(async move {
                            let mut txn = db_clone.begin().unwrap();
                            txn.delete(&key).unwrap();
                            debug!(
                                "Evicted event: ({:?},{:?}) , cause: {:?}",
                                key, value, cause
                            );
                            txn.commit().await.unwrap();
                        });
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

    pub fn recover(&self, tree_name: &str) {
        let txn = self.db.begin().unwrap();
        let iter = txn.scan(tree_name.as_bytes().., None).unwrap();
        iter.iter().for_each(|(k, v, _, _)| {
            self.cache
                .insert(Bytes::from(k.to_vec()), Bytes::from(v.to_vec()));
        });
    }

    pub fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks();
    }
}

// structured data
impl Storage {
    pub fn contains_key(&self, path: &str) -> bool {
        if self.cache.contains_key(path.as_bytes()) {
            return true;
        }

        let txn = self.db.begin().unwrap();
        txn.get(path.as_bytes()).is_ok()
    }

    pub fn get(&self, path: &str) -> Option<Bytes> {
        let key: Bytes = path.as_bytes().to_vec().into();
        if let Some(v) = self.cache.get(&key) {
            return Some(v);
        }

        let txn = self.db.begin().unwrap();
        if let Ok(Some(v)) = txn.get(&key) {
            let value = Bytes::from(v.to_vec());
            self.cache.insert(key, value.clone());
            return Some(value);
        }

        None
    }

    pub fn insert(&self, path: &str, value: Vec<u8>) -> Result<Bytes> {
        let key: Bytes = path.as_bytes().to_vec().into();
        let mut txn = self.db.begin()?;
        txn.set(&key, &value.clone())?;
        let value = Bytes::from(value);
        self.cache.insert(key, value.clone());
        Ok(value)
    }

    pub async fn batch(&self, prefix: &str, kvs: Vec<(String, Option<Vec<u8>>)>) -> Result<()> {
        let mut txn = self.db.begin()?;
        for (k, v) in kvs {
            let k: Bytes = format!("{}/{}", prefix, k).as_bytes().to_vec().into();
            if let Some(v) = v {
                txn.set(&k, &v).ok();
                let value = Bytes::from(v);
                self.cache.insert(k, value);
            } else {
                self.cache.remove(&k);
                txn.delete(&k).ok();
            }
        }
        txn.commit().await?;
        Ok(())
    }

    pub async fn remove(&self, path: &str) {
        let key = path.as_bytes();
        let mut txn = self.db.begin().unwrap();
        txn.delete(key).unwrap();
        txn.commit().await.unwrap();
        self.cache.remove(key);
    }

    pub async fn cas(&self, path: &str, old: Option<Vec<u8>>, new: Option<Vec<u8>>) -> Result<()> {
        let key: Bytes = path.as_bytes().to_vec().into();
        let mut txn = self.db.begin()?;
        let current = txn.get(&key)?;
        if current != old {
            return Err(eyre!("cas failed"));
        } else {
            match new {
                Some(new) => {
                    txn.set(&key, &new)?;
                }
                None => {
                    txn.delete(&key)?;
                }
            }
            txn.commit().await?;
        }
        Ok(())
    }

    pub fn scan(&self, path: &str, max_num: usize) -> Vec<(String, Bytes)> {
        let txn = self.db.begin().unwrap();
        let mut r = vec![];
        if let Ok(results) = txn.scan(path.as_bytes().., Some(max_num)) {
            r = results
                .iter()
                .map(|(k, v, _, _)| {
                    (
                        String::from_utf8_lossy(k).to_string(),
                        Bytes::from(v.to_vec()),
                    )
                })
                .collect();
        }
        r
    }

    pub fn scan_key(&self, path: &str, max_num: usize) -> Vec<String> {
        let txn = self.db.begin().unwrap();
        let mut r = vec![];
        if let Ok(results) = txn.scan(path.as_bytes().., Some(max_num)) {
            for (k, _, _, _) in results {
                r.push(String::from_utf8_lossy(&k).to_string());
            }
        }
        r
    }
}

#[tokio::test]
async fn eviction() {
    let store: Storage = Storage::new(&StorageConfig {
        db_path: "test_eviction.db".to_string(),
        cache_time_to_live: Some(1),
        cache_max_capacity: Some(1024 * 1024 * 1024),
        cache_num_segments: 10,
        ..Default::default()
    });
    let store_clone = store.clone();
    {
        let mut txn = store_clone.db.begin().unwrap();
        let key: Bytes = b"1".to_vec().into();
        txn.set(&key, &key).unwrap();
        store_clone.cache.insert(key.clone(), key);
        txn.commit().await.unwrap();
    }
    {
        let txn = store_clone.db.begin().unwrap();
        println!("{:?}", store_clone.cache.get(1u32.to_string().as_bytes()));
        println!("{:?}", txn.get(1u32.to_string().as_bytes()));
    }
    tokio::time::sleep(Duration::from_millis(700)).await;
    {
        let txn = store_clone.db.begin().unwrap();
        println!("{:?}", store_clone.cache.get(1u32.to_string().as_bytes()));
        println!("{:?}", txn.get(1u32.to_string().as_bytes()));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    {
        let txn = store_clone.db.begin().unwrap();
        println!("{:?}", store_clone.cache.get(1u32.to_string().as_bytes()));
        println!("{:?}", txn.get(1u32.to_string().as_bytes()));
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    {
        let txn = store_clone.db.begin().unwrap();
        println!("{:?}", store_clone.cache.get(1u32.to_string().as_bytes()));
        println!("{:?}", txn.get(1u32.to_string().as_bytes()));
    }
}

#[tokio::test]
async fn structured() {
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
    store.remove("tree_name/test").await;
    assert_eq!(None, store.get("tree_name/test"));
}
