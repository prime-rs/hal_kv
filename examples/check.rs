use hal_kv::{Storage, StorageConfig};

fn main() {
    let store: Storage = Storage::new(&StorageConfig {
        db_path: "test_eviction.db".to_string(),
        cache_time_to_live: None,
        cache_max_capacity: Some(1024 * 1024 * 1024),
        cache_num_segments: 10,
        ..Default::default()
    });
    let now = std::time::Instant::now();
    for i in 0..10000u32 {
        if store.get(&format!("test/{}", i)).is_none() {
            println!("not found: {}", i);
        }
    }

    println!("elapsed: {:?}", now.elapsed());
}
