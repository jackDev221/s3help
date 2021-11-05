pub mod s3_utils;
pub mod redis_utils;

use s3_utils::get_object_use_range;

#[tokio::main]
async fn main() {
    do_s3_download().await;
}

async fn do_s3_download() {
    get_object_use_range(
        9940,
        10,
        "cnnorth1".to_string(),
        "heco-manager-s3-test".to_string(),
        "zkdex/account_tree/test-2/718".to_string(),
        "data.txt".to_string(),
    ).await;
}