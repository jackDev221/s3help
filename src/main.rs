extern crate dotenv;
extern crate tokio;

use bytes::Bytes;
use dotenv::dotenv;
use futures::executor;
use futures::*;
use rusoto_core::credential::{EnvironmentProvider, ProvideAwsCredentials};
use rusoto_core::{Region, RusotoError};
use rusoto_s3::util::{PreSignedRequest, PreSignedRequestOption};
use rusoto_s3::PutObjectRequest;
use rusoto_s3::StreamingBody;
use rusoto_s3::{
    CompleteMultipartUploadRequest, CompletedMultipartUpload, CompletedPart,
    CreateMultipartUploadRequest, UploadPartRequest, S3, S3Client, GetObjectRequest,
};
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;

use crypto::digest::Digest;
use crypto::md5::Md5;
// use std::ops::Index;
// use tokio::{fs::File, io};
use futures::stream::TryStreamExt;
use time;


#[tokio::main]
async fn main() {
    // println!("==========");
    // if_multipart_then_upload_multiparts_dicom().await;
    // println!("==========");
    // get_object().await;
    // upload().await;
    calc_md5().await;
}


async fn calc_md5() {
    let local_filename = "./witness";
    let mut file = std::fs::File::open(local_filename).unwrap();
    let mut buffer = String::new();
    let res = file.read_to_string(&mut buffer);
    if res.is_ok() {
        let mut md5 = Md5::new();
        md5.input_str(buffer.as_str());
        println!("md5:{}", md5.result_str())
    }
}

async fn get_object() {
    let now = Instant::now();
    let destination_filename = "test_witness_2";
    let bucket_name = "zkdex-prod-xingchen-files";
    // let bucket_name = "heco-manager-s3-test";
    // let client = S3Client::new(Region::ApNortheast1);
    let client = S3Client::new(Region::ApNortheast1);
    let get_object_request = GetObjectRequest {
        bucket: bucket_name.to_owned(),
        key: destination_filename.to_owned(),
        ..Default::default()
    };
    let mut object = client.get_object(get_object_request).await.expect("get object failed");
    let body = object.body.take().expect("The object has no body");

    // to string
    let body = body.map_ok(|b| b.to_vec()).try_concat().await.expect("ff");
    let res_str = std::str::from_utf8(&body).expect("fail to str");
    let mut md5 = Md5::new();
    md5.input_str(res_str);
    println!("md5:{}", md5.result_str());

    // write to file
    // let mut body_read = body.into_async_read();
    // let mut file = tokio::fs::File::create("./ww_new").await.expect("fc");
    // tokio::io::copy(&mut body_read, &mut file).await;

    println!("task taken : {}", now.elapsed().as_secs());
}

async fn upload() {
    let local_filename = "/Users/lvbin/Desktop/a";
    let destination_filename = "test_witness_2";
    // let bucket_name = "heco-manager-s3-test";
    let bucket_name = "zkdex-prod-xingchen-files";
    let destination_filename_clone = destination_filename.clone();
    let mut file = std::fs::File::open(local_filename).unwrap();
    let mut buffer = String::new();
    let res = file.read_to_string(&mut buffer);
    let body = buffer.into_bytes();
    let client = S3Client::new(Region::ApNortheast1);
    let por = PutObjectRequest {
        body: Some(body.into()),
        bucket: bucket_name.to_string().to_owned(),
        key: destination_filename_clone.to_owned(),
        ..Default::default()
    };
    let res = client.put_object(por).await.expect("fail ");
    println!("{:?}", res);
}

async fn if_multipart_then_upload_multiparts_dicom() {
    let now = Instant::now();
    dotenv().ok();
    let local_filename = "./witness";
    let destination_filename = "test_witness_1";
    // let bucket_name = "heco-manager-s3-test";
    let bucket_name = "zkdex-prod-xingchen-files";
    let destination_filename_clone = destination_filename.clone();
    let mut file = std::fs::File::open(local_filename).unwrap();
    let mut buffer = String::new();
    let res = file.read_to_string(&mut buffer);
    let data_send_base = buffer.into_bytes();


    const CHUNK_SIZE: usize = 6_000_000;
    // let mut buffer = Vec::with_capacity(CHUNK_SIZE);

    let client = S3Client::new(Region::ApNortheast1);
    // let client = S3Client::new(Region::ApNortheast1);
    let create_multipart_request = CreateMultipartUploadRequest {
        bucket: bucket_name.to_owned(),
        key: destination_filename.to_owned(),
        ..Default::default()
    };

    // Start the multipart upload and note the upload_id generated
    let response = client.create_multipart_upload(create_multipart_request)
        .await
        .expect("Couldn't create multipart upload");
    let upload_id = response.upload_id.unwrap();

    let upload_id_clone = upload_id.clone();
    // Create upload parts
    let create_upload_part = move |body: Vec<u8>, part_number: i64| -> UploadPartRequest {
        UploadPartRequest {
            body: Some(body.into()),
            bucket: bucket_name.to_string().to_owned(),
            key: destination_filename_clone.to_owned(),
            upload_id: upload_id_clone.to_owned(),
            part_number: part_number,
            ..Default::default()
        }
    };

    let create_upload_part_arc = Arc::new(create_upload_part);
    let completed_parts = Arc::new(Mutex::new(vec![]));

    let mut part_number = 1;

    let mut multiple_parts_futures = Vec::new();
    loop {
        // let maximum_bytes_to_read = CHUNK_SIZE - buffer.len();
        // println!("maximum_bytes_to_read: {}", maximum_bytes_to_read);
        // file.by_ref()
        //     .take(maximum_bytes_to_read as u64)
        //     .read_to_end(&mut buffer)
        //     .unwrap();
        // println!("length: {}", buffer.len());
        // println!("part_number: {}", part_number);
        // if buffer.len() == 0 {
        //     println!("the file is end");
        //     // The file has ended.
        //     break;
        // }

        if (part_number - 1) * CHUNK_SIZE > data_send_base.len() {
            println!("the file is end");
            break;
        }
        let start = (part_number - 1) * CHUNK_SIZE;
        let mut end = part_number * CHUNK_SIZE;
        if end > data_send_base.len(){
            end = data_send_base.len();
        }
        // let next_buffer = Vec::with_capacity(CHUNK_SIZE);
        let data_to_send = Vec::from(&data_send_base[start..end]);
        println!("part_number: {}: len:{} ", part_number, data_to_send.len());
        let completed_parts_cloned = completed_parts.clone();
        let create_upload_part_arc_cloned = create_upload_part_arc.clone();
        let send_part_task_future = tokio::task::spawn(async move {
            let part = create_upload_part_arc_cloned(data_to_send, part_number as i64);
            {
                let part_number = part.part_number;
                // let client = super::get_client().await;
                let client = S3Client::new(Region::ApNortheast1);
                let response = client.upload_part(part).await;
                completed_parts_cloned.lock().unwrap().push(CompletedPart {
                    e_tag: response
                        .expect("Couldn't complete multipart upload")
                        .e_tag
                        .clone(),
                    part_number: Some(part_number),
                });
            }
        });
        multiple_parts_futures.push(send_part_task_future);
        // buffer = next_buffer;
        part_number = part_number + 1;
    }
    // let client = super::get_client().await;
    let client = S3Client::new(Region::ApNortheast1);
    println!("waiting for futures");
    let _results = futures::future::join_all(multiple_parts_futures).await;

    let mut completed_parts_vector = completed_parts.lock().unwrap().to_vec();
    completed_parts_vector.sort_by_key(|part| part.part_number);
    println!("futures done");
    let completed_upload = CompletedMultipartUpload {
        parts: Some(completed_parts_vector),
    };

    let complete_req = CompleteMultipartUploadRequest {
        bucket: bucket_name.to_owned(),
        key: destination_filename.to_owned(),
        upload_id: upload_id.to_owned(),
        multipart_upload: Some(completed_upload),
        ..Default::default()
    };

    client.complete_multipart_upload(complete_req)
        .await
        .expect("Couldn't complete multipart upload");
    println!(
        "time taken: {}, with chunk:: {}",
        now.elapsed().as_secs(),
        CHUNK_SIZE
    );
}