use std::time::Instant;
use redlock::{RedLock, Lock};
use std::thread::sleep;
use chrono::prelude::Utc;
use futures::*;

pub async fn get_redis_lock(redis_uris: String) -> anyhow::Result<RedLock> {
    return Ok(RedLock::new(redis_uris.split(",").collect()));
}

pub async fn lock(red_lock: &RedLock, redis_lock_name: String, expired_time: usize) -> anyhow::Result<Lock<'_>> {
    let instance = Instant::now();
    loop {
        if let Some(_lock) = red_lock.lock(redis_lock_name.as_bytes(), expired_time) {
            return Ok(_lock);
        }
        if instance.elapsed().as_secs() >= (expired_time as u64 / 1000) {
            return Err(anyhow::Error::msg(format!("Get redis lock failed, lock_name:{}, expired_time: {}", redis_lock_name, expired_time)));
        }
        sleep(std::time::Duration::from_millis(500));
    }
}

pub async fn unlock(red_lock: &RedLock, lock: &Lock<'_>) {
    red_lock.unlock(lock)
}


pub async fn do_redis() {
    // let urls_url = "redis://redis.dev-7.sinnet.huobiidc.com:6379".to_string();
    let urls_url = "redis://sentinel-26381-1.huobiidc.com:26381/1,\
    redis://sentinel-26381-2.huobiidc.com:26381/1,\
    redis://sentinel-26381-3.huobiidc.com:26381/1,\
    redis://sentinel-26381-4.huobiidc.com:26381/1,\
    redis://sentinel-26381-5.huobiidc.com:26381/1".to_string();
    let mut multiple_future = Vec::new();
    let mut i = 0;
    while i < 5 {
        let urls_url_clone = urls_url.clone();
        let task_future = tokio::task::spawn(async move {
            let job_cast = tokio::time::Duration::from_secs(2+i);
            println!("Try to get lock: {}, {}", i, Utc::now());
            let red_lock = get_redis_lock(urls_url_clone).await.expect("get failed");
            let lock = lock(&red_lock, "l2_lock_redis_test".to_string(), 500000).await.expect("lock fail");
            println!("Start to do something {}, {}, {}", i, job_cast.as_secs(), Utc::now());
            tokio::time::delay_for(job_cast).await;
            println!("Unlock:{}, {}",i , Utc::now());
            unlock(&red_lock, &lock).await;
        });
        multiple_future.push(task_future);
        i+= 1;
    }
    let res = future::join_all(multiple_future).await;
    println!("All end: {:?}", res);
}

