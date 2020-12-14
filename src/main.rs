use clap::Clap;
use std::path::{PathBuf, Path};
use tokio::fs;
use std::io;
use std::cmp;
use glob::glob;
use rusoto_core::{ByteStream, Region};
use rusoto_s3::{S3Client, 
    S3, UploadPartRequest, UploadPartOutput, CreateMultipartUploadRequest, CreateMultipartUploadOutput, CompleteMultipartUploadRequest, CompleteMultipartUploadOutput, CompletedPart, CompletedMultipartUpload, AbortMultipartUploadRequest, AbortMultipartUploadOutput};
use std::str::FromStr;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader, reader_stream};

static DEFAULT_BUFFER_SIZE: usize = 1000;

type Error = Box<dyn std::error::Error + 'static>;
type Result<T> = std::result::Result<T,Error>;

#[derive(Clap)]
struct Opts {
    #[clap(short, long)]
    bucket: String,

    #[clap(short, long)]
    key: String,

    #[clap(short, long, default_value = "*")]
    pattern: String,

    #[clap(short, long)]
    region: Option<String>,

    #[clap(short, long)]
    endpoint: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let opts: Opts = Opts::parse();

    let mut parts = get_parts(&opts.pattern).map_err(|err| format!("get part files error: {}", err))?;
    let region = opts.region().map_err(|err| format!("get region error: {}", err))?;
    let s3client = S3Client::new(region);

    parts.sort_by(|a,b| compare_file_names(a,b));

    for f in parts.iter() {
        println!("{:?}", f);
    }

    upload_or_abort(&s3client, parts, &opts.bucket, &opts.key).await.map_err(|err| format!("upload error: {}", err))?;

    Ok(())
}

fn get_parts(src: &str) -> std::result::Result<Vec<PathBuf>, Error> {
    let mut parts = vec![];
    for entry in glob(src).expect("read dir") {
        let f = entry?;
        if f.is_file() {
            parts.push(f);
        }
    }
    Ok(parts)
}

fn compare_file_names<A: AsRef<Path>,B: AsRef<Path>>(a: A, b: B) -> cmp::Ordering {
    return a.as_ref().partial_cmp(b.as_ref()).unwrap()
}

async fn upload_or_abort<V: IntoIterator<Item=PathBuf>>(s3client: &S3Client, parts: V, bucket: &str, key: &str) -> std::result::Result<(), Error> {
    let multipart_upload = s3client.create_multipart_upload(CreateMultipartUploadRequest {
        acl: None,
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        ..Default::default()
    }).await
        .map_err(|err| format!("error creating multipart upload request: {}", err))?;

    let upload_id = multipart_upload.upload_id.ok_or_else(|| format!("no upload id returned by create multipart upload request"))?;

    match upload(s3client, parts, bucket, key, &upload_id).await {
        Ok(()) => {
            Ok(())
        },
        Err(err) => {
            println!("aborting upload");
            match s3client.abort_multipart_upload(AbortMultipartUploadRequest {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                upload_id: upload_id.clone(),
                ..Default::default()
            }).await {
                Err(err) => {
                    println!("error aborting multipart upload: {}", err);
                },
                Ok(_) => {}
            }

            Err(err)
        }
    }
}
async fn upload<V: IntoIterator<Item=PathBuf>>(s3client: &S3Client, parts: V, bucket: &str, key: &str, upload_id: &str) -> std::result::Result<(), Error> {
    let completed_multipart_upload = upload_parts(&s3client, parts, &bucket, &key, &upload_id).await?;
        
    println!("completing upload");
    s3client.complete_multipart_upload(CompleteMultipartUploadRequest {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        multipart_upload: Some(completed_multipart_upload),
        upload_id: upload_id.to_owned(),
        ..Default::default()
    }).await
        .map_err(|err| format!("error completing multipart upload: {}", err))?;

    Ok(())
}

async fn upload_parts<V: IntoIterator<Item=PathBuf>>(s3client: &S3Client, parts: V, bucket: &str, key: &str, upload_id: &str) -> Result<CompletedMultipartUpload> {
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

async fn upload_part(s3client: &S3Client, part: &Path, bucket: &str, key: &str, upload_id: &str, part_number: i64) -> Result<CompletedPart> {
    let (len, hash) = digest_file(part).await?;
    let body = fs::File::open(part).await.map_err(|err| format!("error opening part file for upload: {}", err))?;
    let bufreader = BufReader::new(body);
    let stream = reader_stream(bufreader);
    let bytestream = ByteStream::new(stream);

    let upload = s3client.upload_part(UploadPartRequest {
        body: Some(bytestream),
        bucket: bucket.to_string(),
        content_md5: Some(hash),
        content_length: Some(len as i64),
        key: key.to_string(),
        part_number: part_number,
        upload_id: upload_id.to_string(),
        ..Default::default()
    }).await
        .map_err(|err| format!("error uploading part: {}", err))?;

    let part = CompletedPart {
        e_tag: upload.e_tag,
        part_number: Some(part_number),
    };

    log::debug!("uploaded {:?}", part);

    Ok(part)
}

async fn digest_file(part: &Path) -> Result<(u64, String)> {
    let mut f = fs::File::open(part).await.map_err(|err| format!("error opening part file for hashing: {}", err))?;
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

impl Opts {
    fn region(&self) -> std::result::Result<Region, Error> {
        if let Some(ref endpoint) = self.endpoint {
            Ok(Region::Custom {
                name: self.region
                    .as_ref()
                    .map(|s| s.to_owned())
                    .unwrap_or_else(|| "custom".to_string()),
                endpoint: endpoint.to_owned(),
            })
        } else {
            self.region
                .as_ref()
                .map(|r| Region::from_str(&r))
                .unwrap_or_else(|| Ok(Region::default()))
                .map_err(|err| format!("region parse error: {}", err).into())
        }
    }
}