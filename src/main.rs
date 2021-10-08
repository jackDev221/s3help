pub mod s3_utils;
pub mod redis_utils;

use s3_utils::{if_multipart_then_upload_multiparts_dicom, get_object_use_range, calc_md5};

#[tokio::main]
async fn main() {
    redis_utils::do_redis().await;
    // do_s3_task().await;
}

async fn do_s3_task() {
    println!("upload file");
    if_multipart_then_upload_multiparts_dicom().await;
    println!("calc md5");
    calc_md5();
    println!("download");
    get_object_use_range(1122221, 10).await;
}