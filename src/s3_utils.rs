extern crate dotenv;
extern crate tokio;

use dotenv::dotenv;
use rusoto_core::Region;
use rusoto_s3::PutObjectRequest;
use rusoto_s3::{
    CompleteMultipartUploadRequest, CompletedMultipartUpload, CompletedPart,
    CreateMultipartUploadRequest, UploadPartRequest, S3, S3Client, GetObjectRequest,
};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crypto::digest::Digest;
use crypto::md5::Md5;
use futures::stream::TryStreamExt;
use anyhow::Error;
use std::str::FromStr;

pub async fn calc_md5(file: String) {
    let local_filename = file.as_str();
    let mut file = std::fs::File::open(local_filename).unwrap();
    let mut buffer = String::new();
    let res = file.read_to_string(&mut buffer);
    if res.is_ok() {
        let mut md5 = Md5::new();
        md5.input_str(buffer.as_str());
        println!("md5:{}", md5.result_str())
    }
}

pub async fn get_object(region_string: String,
                        bucket_name: String,
                        key: String) {
    let now = Instant::now();
    let region = Region::from_str(region_string.as_str()).expect("parse region failed");
    let client = S3Client::new(region);
    let get_object_request = GetObjectRequest {
        bucket: bucket_name.to_owned(),
        key: key.to_owned(),
        ..Default::default()
    };
    let mut object = client.get_object(get_object_request).await.expect("get object failed");
    let body = object.body.take().expect("The object has no body");

    // to string
    let body = body.map_ok(|b| b.to_vec()).try_concat().await.expect("ff");

    // let str_show = String::from_utf8(body).expect("fail to string");
    // println!("{}", str_show);
    let res_str = std::str::from_utf8(&body).expect("fail to str");
    println!("{}", res_str);
    let mut md5 = Md5::new();
    md5.input_str(res_str);
    println!("md5:{}", md5.result_str());

    // write to file
    // let mut body_read = body.into_async_read();
    // let mut file = tokio::fs::File::create("./ww_new").await.expect("fc");
    // tokio::io::copy(&mut body_read, &mut file).await;

    println!("task taken : {}", now.elapsed().as_secs());
}

#[derive(Default, Debug, Clone, PartialEq)]
pub struct CompletedObject {
    pub part_number: Option<i64>,
    pub data: Vec<u8>,
}


pub async fn get_object_use_range(sum_size: i64,
                                  threads: i64,
                                  region_string: String,
                                  bucket_name: String,
                                  key: String,
                                  des_file: String) {
    let now = Instant::now();
    dotenv().ok();
    let region = Region::from_str(region_string.as_str()).expect("parse region failed");
    let create_get_part_object = move |range: String| -> GetObjectRequest {
        GetObjectRequest {
            bucket: bucket_name.clone(),
            key: key.clone(),
            range: Some(range),
            ..Default::default()
        }
    };

    let create_get_part_object_arc = Arc::new(create_get_part_object);
    let completed_parts = Arc::new(Mutex::new(vec![]));
    let trunks = sum_size / threads;
    let mut counts = threads;
    if sum_size % threads != 0 {
        counts = counts + 1;
    }
    let mut part_number = 0;
    let mut multiple_parts_futures = Vec::new();
    loop {
        if part_number >= counts {
            println!("end break");
            break;
        }
        println!("get object for {}", part_number);
        let completed_parts_cloned = completed_parts.clone();
        let create_get_part_object_arc_cloned = create_get_part_object_arc.clone();
        let region_clone = region.clone();

        let send_part_task_future = tokio::task::spawn(async move {
            let range_str = format!("bytes={}-{}", (part_number) * trunks, (part_number + 1) * trunks - 1);
            println!("index:{}: {}", part_number, range_str);
            let part = create_get_part_object_arc_cloned(range_str.clone());
            {
                let part_index = Some(part_number);
                let client = S3Client::new(region_clone);
                let mut object = client.get_object(part).await.expect("get object failed");
                let body = object.body.take().expect("The object has no body");
                // to string
                let body = body.map_ok(|b| b.to_vec()).try_concat().await.expect("ff");
                //completed_parts.add
                completed_parts_cloned.lock().unwrap().push(CompletedObject {
                    data: body,
                    part_number: part_index,
                });
            }
        });
        multiple_parts_futures.push(send_part_task_future);
        part_number = part_number + 1;
    }
    let _results = futures::future::join_all(multiple_parts_futures).await;
    println!("futures done");
    let mut completed_parts_vector = completed_parts.lock().unwrap().to_vec();
    completed_parts_vector.sort_by_key(|part| part.part_number);
    let mut rec: Vec<u8> = Vec::new();
    for part in completed_parts_vector {
        rec.extend_from_slice(part.data.as_slice());
    }
    println!("task taken : {}", now.elapsed().as_secs());
    let res_str = std::str::from_utf8(&rec).expect("fail to str");
    let mut md5 = Md5::new();
    md5.input_str(res_str);
    println!("md5:{}", md5.result_str());
    println!("task taken : {}", now.elapsed().as_secs());
    let mut file1 = std::fs::File::create(des_file.as_str()).expect("create failed");
    file1.write_all(res_str.as_bytes()).expect("fail to write")
}


pub async fn get_pair_object(pairts: i64,
                             region_string: String,
                             key: String,
                             bucket_name: String) {
    // let now = Instant::now();
    let now = Instant::now();
    let region = Region::from_str(region_string.as_str()).expect("parse region failed");
    dotenv().ok();
    let create_get_part_object = move |part_number: i64| -> GetObjectRequest {
        GetObjectRequest {
            bucket: bucket_name.to_owned(),
            key: key.to_owned(),
            part_number: Some(part_number),
            ..Default::default()
        }
    };

    let create_get_part_object_arc = Arc::new(create_get_part_object);
    let completed_parts = Arc::new(Mutex::new(vec![]));
    let mut part_number = 1;
    let mut multiple_parts_futures = Vec::new();
    loop {
        if part_number > pairts {
            println!("end break");
            break;
        }
        println!("get object for {}", part_number);
        let completed_parts_cloned = completed_parts.clone();
        let create_get_part_object_arc_cloned = create_get_part_object_arc.clone();
        let region_clone = region.clone();

        let send_part_task_future = tokio::task::spawn(async move {
            let part = create_get_part_object_arc_cloned(part_number as i64);
            {
                let part_number = part.part_number;
                let client = S3Client::new(region_clone);
                let mut object = client.get_object(part).await.expect("get object failed");
                let body = object.body.take().expect("The object has no body");
                // to string
                let body = body.map_ok(|b| b.to_vec()).try_concat().await.expect("ff");
                //completed_parts.add
                completed_parts_cloned.lock().unwrap().push(CompletedObject {
                    data: body,
                    part_number: part_number,
                });
            }
        });
        multiple_parts_futures.push(send_part_task_future);
        part_number = part_number + 1;
    }
    let _results = futures::future::join_all(multiple_parts_futures).await;
    println!("futures done");
    let mut completed_parts_vector = completed_parts.lock().unwrap().to_vec();
    completed_parts_vector.sort_by_key(|part| part.part_number);
    let mut rec: Vec<u8> = Vec::new();
    for part in completed_parts_vector {
        rec.extend_from_slice(part.data.as_slice());
    }
    println!("task taken : {}", now.elapsed().as_secs());
    let res_str = std::str::from_utf8(&rec).expect("fail to str");
    let mut md5 = Md5::new();
    md5.input_str(res_str);
    println!("md5:{}", md5.result_str());
    println!("task taken : {}", now.elapsed().as_secs());
}


pub async fn upload(region_string: String,
                    local_file_name: String,
                    key: String,
                    bucket_name: String) {
    let region = Region::from_str(region_string.as_str()).expect("parse region failed");
    let mut file = std::fs::File::open(local_file_name).unwrap();
    let mut buffer = String::new();
    let res = file.read_to_string(&mut buffer);
    if res.is_err() {
        println!("read fail {}", res.err().unwrap())
    }
    let body = buffer.into_bytes();
    let client = S3Client::new(region);
    let por = PutObjectRequest {
        body: Some(body.into()),
        bucket: bucket_name.to_owned(),
        key: key.to_owned(),
        ..Default::default()
    };
    let res = client.put_object(por).await.expect("fail ");
    println!("{:?}", res);
}

pub async fn upload_multiparts(file: String,
                               region_string: String,
                               bucket_name: String,
                               key: String) -> anyhow::Result<bool> {
    let now = Instant::now();
    let region = Region::from_str(region_string.as_str()).expect("parse region failed");
    dotenv().ok();
    let local_filename = file.as_str();
    let mut file = std::fs::File::open(local_filename).unwrap();
    let mut buffer = String::new();
    let res = file.read_to_string(&mut buffer);
    if res.is_err() {
        println!("read fail {}", res.err().unwrap())
    }
    let data_send_base = buffer.into_bytes();
    const CHUNK_SIZE: usize = 8_000_000;
    let client = S3Client::new(region.clone());
    let create_multipart_request = CreateMultipartUploadRequest {
        bucket: bucket_name.to_owned(),
        key: key.to_owned(),
        ..Default::default()
    };

    // Start the multipart upload and note the upload_id generated
    let response = client.create_multipart_upload(create_multipart_request)
        .await?;
    let upload_id = response.upload_id.unwrap();

    let upload_id_clone = upload_id.clone();
    let bucket_name_clone = bucket_name.clone();
    let key_clone = key.clone();
    // Create upload parts
    let create_upload_part = move |body: Vec<u8>, part_number: i64| -> UploadPartRequest {
        UploadPartRequest {
            body: Some(body.into()),
            bucket: bucket_name_clone.to_owned(),
            key: key_clone.to_owned(),
            upload_id: upload_id_clone.to_owned(),
            part_number: part_number,
            ..Default::default()
        }
    };

    let create_upload_part_arc = Arc::new(create_upload_part);
    let completed_parts = Arc::new(Mutex::new(vec![]));
    let completed_md5_equal = Arc::new(Mutex::new(true));
    let mut part_number = 1;
    let mut multiple_parts_futures = Vec::new();
    loop {
        if (part_number - 1) * CHUNK_SIZE > data_send_base.len() {
            println!("the file is end");
            break;
        }
        let start = (part_number - 1) * CHUNK_SIZE;
        let mut end = part_number * CHUNK_SIZE;
        if end > data_send_base.len() {
            end = data_send_base.len();
        }
        // let next_buffer = Vec::with_capacity(CHUNK_SIZE);
        let data_to_send = Vec::from(&data_send_base[start..end]);
        println!("part_number: {}: len:{} ", part_number, data_to_send.len());
        let completed_parts_cloned = completed_parts.clone();
        let create_upload_part_arc_cloned = create_upload_part_arc.clone();
        let completed_md5_equal_cloned = completed_md5_equal.clone();
        let region_clone = region.clone();
        let send_part_task_future = tokio::task::spawn(async move {
            let part = create_upload_part_arc_cloned(data_to_send.clone(), part_number as i64);
            {
                let part_number = part.part_number;
                let client = S3Client::new(region_clone);
                let mut md5 = Md5::new();
                md5.input(&(data_to_send.clone()));
                let md5_ori = md5.result_str();
                let number = part_number;
                println!("{}:{}", number, md5.result_str());
                let response = client.upload_part(part).await;
                let completed_part = CompletedPart {
                    e_tag: response
                        .expect("Couldn't complete multipart upload")
                        .e_tag
                        .clone(),
                    part_number: Some(part_number),
                };
                let act_md5 = completed_part.clone().e_tag.unwrap().replace("\"", "");
                let mut res = completed_md5_equal_cloned.lock().unwrap();
                *res = *res & md5_ori.eq(act_md5.as_str());
                completed_parts_cloned.lock().unwrap().push(completed_part);
            }
        });
        multiple_parts_futures.push(send_part_task_future);
        part_number = part_number + 1;
    }
    let client = S3Client::new(region.clone());
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
        key: key.to_owned(),
        upload_id: upload_id.to_owned(),
        multipart_upload: Some(completed_upload),
        ..Default::default()
    };

    client.complete_multipart_upload(complete_req)
        .await?;
    println!(
        "time taken: {}, with chunk:: {}",
        now.elapsed().as_secs(),
        CHUNK_SIZE
    );
    let completed_parts_vector = completed_md5_equal.lock().unwrap();
    if !*completed_parts_vector {
        return Err(Error::msg("completed_parts_vector"));
    }
    Ok(*completed_parts_vector)
}