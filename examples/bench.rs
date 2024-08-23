use hal_kv::{Storage, StorageConfig};

fn main() {
    let store: Storage = Storage::new(&StorageConfig {
        db_path: "test_bench.db".to_string(),
        cache_time_to_live: Some(1),
        cache_max_capacity: Some(1024 * 1024 * 1024),
        cache_num_segments: 10,
        ..Default::default()
    });
    let now = std::time::Instant::now();
    for i in 0..100u32 {
        println!("insert: {}", i);
        store
            .insert(&format!("test/{}", i), i.to_string().as_bytes().to_vec())
            .unwrap();
    }

    println!("elapsed: {:?}", now.elapsed());
}
