use std::time::Duration;

use hal_kv::{Storage, StorageConfig};

fn main() {
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
        for i in 0..100u32 {
            println!("insert: {}", i);
            store_clone
                .insert(&format!("test/{}", i), i.to_string().as_bytes().to_vec())
                .unwrap();
        }

        println!("{:?}", store_clone.get(&format!("test/{}", 99u32)));
        tokio::time::sleep(Duration::from_millis(700)).await;
        println!("{:?}", store.get(&format!("test/{}", 99u32)));
        tokio::time::sleep(Duration::from_millis(300)).await;
        println!("{:?}", store.get(&format!("test/{}", 99u32)));
        tokio::time::sleep(Duration::from_millis(10)).await;
        println!("{:?}", store.get(&format!("test/{}", 99u32)));
    });
    rt.block_on(async {
        tokio::time::sleep(Duration::from_millis(2000)).await;
    });
}
