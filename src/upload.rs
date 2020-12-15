use std::path::{Path, PathBuf};
use crate::error::Error;
use crate::result::Result;
use std::cmp;
use rusoto_core::{ByteStream};
use rusoto_s3::{S3Client, S3, UploadPartRequest, CompletedPart, CreateMultipartUploadRequest, CompletedMultipartUpload, AbortMultipartUploadRequest, CompleteMultipartUploadRequest};
use tokio::fs;
use tokio::io::{reader_stream, AsyncReadExt, BufReader};
use glob;

static DEFAULT_BUFFER_SIZE: usize = 1000;

pub fn get_parts(src: &str) -> std::result::Result<Vec<PathBuf>, Error> {
    let mut parts = vec![];
    for entry in glob::glob(src).expect("read dir") {
        let f = entry?;
        if f.is_file() {
            parts.push(f);
        }
    }

    parts.sort_by(|a, b| compare_file_names(a, b));

    Ok(parts)
}

fn compare_file_names<A: AsRef<Path>, B: AsRef<Path>>(a: A, b: B) -> cmp::Ordering {
    return a.as_ref().partial_cmp(b.as_ref()).unwrap();
}

pub async fn start_upload(s3client: &S3Client, bucket: &str, key: &str) -> Result<String> {
    let multipart_upload = s3client
        .create_multipart_upload(CreateMultipartUploadRequest {
            acl: None,
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            ..Default::default()
        })
        .await
        .map_err(|err| format!("error creating multipart upload request: {}", err))?;

    let upload_id = multipart_upload.upload_id.ok_or_else(|| format!("no upload id returned by create multipart upload request"))?;

    Ok(upload_id)
}

pub async fn upload_or_abort<V: IntoIterator<Item = PathBuf>>(
    s3client: &S3Client,
    parts: V,
    bucket: &str,
    key: &str,
) -> std::result::Result<(), Error> {
    let upload_id = start_upload(s3client, bucket, key).await?;

    match upload(s3client, parts, bucket, key, &upload_id).await {
        Ok(()) => Ok(()),
        Err(err) => {
            println!("aborting upload");
            abort_upload(s3client, bucket, key, &upload_id).await?;

            Err(err)
        }
    }
}

pub async fn abort_upload(s3client: &S3Client, bucket: &str, key: &str, upload_id: &str) -> Result<()> {
    s3client
        .abort_multipart_upload(AbortMultipartUploadRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            upload_id: upload_id.to_owned(),
            ..Default::default()
        })
        .await
        .map_err(|err| format!("error aborting upload: {:?}", err))?;

    Ok(())
}

pub async fn upload<V: IntoIterator<Item = PathBuf>>(
    s3client: &S3Client,
    parts: V,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> std::result::Result<(), Error> {
    let completed_multipart_upload =
        upload_parts(&s3client, parts, &bucket, &key, &upload_id).await?;

    println!("completing upload");
    s3client
        .complete_multipart_upload(CompleteMultipartUploadRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            multipart_upload: Some(completed_multipart_upload),
            upload_id: upload_id.to_owned(),
            ..Default::default()
        })
        .await
        .map_err(|err| format!("error completing multipart upload: {}", err))?;

    Ok(())
}

pub async fn upload_parts<V: IntoIterator<Item = PathBuf>>(
    s3client: &S3Client,
    parts: V,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<CompletedMultipartUpload> {
    let mut uploads = vec![];

    let mut part_number = 1;

    for part in parts {
        log::info!("uploading part {} {:?}", part_number, part);
        uploads.push(upload_part(s3client, &part, bucket, key, upload_id, part_number).await?);

        part_number += 1;
    }

    Ok(CompletedMultipartUpload {
        parts: Some(uploads),
    })
}

pub async fn upload_part(
    s3client: &S3Client,
    part: &str,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i64,
) -> Result<CompletedPart> {
    let part = PathBuf::from(part);
    let (len, hash) = digest_file(&part).await?;
    let body = fs::File::open(&part)
        .await
        .map_err(|err| format!("error opening part file for upload: {}", err))?;
    let bufreader = BufReader::new(body);
    let stream = reader_stream(bufreader);
    let bytestream = ByteStream::new(stream);

    let upload = s3client
        .upload_part(UploadPartRequest {
            body: Some(bytestream),
            bucket: bucket.to_string(),
            content_md5: Some(hash),
            content_length: Some(len as i64),
            key: key.to_string(),
            part_number: part_number,
            upload_id: upload_id.to_string(),
            ..Default::default()
        })
        .await
        .map_err(|err| format!("error uploading part: {}", err))?;

    let part = CompletedPart {
        e_tag: upload.e_tag,
        part_number: Some(part_number),
    };

    log::debug!("uploaded {:?}", part);

    Ok(part)
}

pub async fn digest_file(part: &Path) -> Result<(u64, String)> {
    let mut f = fs::File::open(part)
        .await
        .map_err(|err| format!("error opening part file for hashing: {}", err))?;
    let mut digest = md5::Context::new();
    let mut buffer = vec![];
    buffer.resize(DEFAULT_BUFFER_SIZE, 0);
    let mut len = 0;

    loop {
        let count = f.read(&mut buffer[..]).await?;
        if count == 0 {
            break;
        }

        len += count as u64;

        digest.consume(&buffer[0..count]);
    }
    let hash: [u8; 16] = digest.compute().into();
    let b64hash = base64::encode(hash);

    log::debug!("hashed {} bytes as {} for part {:?}", len, b64hash, part);

    Ok((len, b64hash))
}
