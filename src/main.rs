pub mod s3_utils;
pub mod redis_utils;

use s3_utils::{if_multipart_then_upload_multiparts_dicom, get_object_use_range, calc_md5};

#[tokio::main]
async fn main() {
    // redis_utils::do_redis().await;
    do_s3_task_n().await;
}

async fn do_s3_task() {
    println!("upload file");
    // if_multipart_then_upload_multiparts_dicom().await;
    println!("calc md5");
    // calc_md5();
    println!("download");
    get_object_use_range(1122221, 10).await;
}

async fn do_s3_task_n() {
    // let file =  "/Users/lvbin/Person/code/rust/myrust/data.txt".to_string();
    get_object_use_range(641694, 10).await;
    // if_multipart_then_upload_multiparts_dicom(file.clone()).await;
    // calc_md5(file.clone()).await;
}