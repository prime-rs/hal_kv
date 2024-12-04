use hal_kv::{Storage, StorageConfig};

#[tokio::main]
async fn main() {
    let store: Storage = Storage::new(&StorageConfig {
        db_path: "test_bench.db".to_string(),
        cache_time_to_live: None,
        cache_max_capacity: Some(1024 * 1024 * 1024),
        cache_num_segments: 10,
        ..Default::default()
    });
    let now = std::time::Instant::now();
    let mut datas = Vec::with_capacity(10000);
    for i in 0..10000u32 {
        datas.push((i.to_string(), Some(i.to_string().as_bytes().to_vec())));
    }
    store.batch("test", datas).await.unwrap();

    println!("elapsed: {:?}", now.elapsed());
}
