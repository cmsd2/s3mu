use clap::Clap;

use rusoto_core::{Region};
use rusoto_s3::{
    S3Client,
};




use std::path::{PathBuf};
use std::str::FromStr;



pub mod error;
pub mod actions;
pub mod wal;
pub mod state;
pub mod upload;
pub mod result;
pub mod app;

use error::Error;
use result::Result;


use app::App;

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

    #[clap(short, long)]
    log: PathBuf,

    #[clap(short, long, default_value="3")]
    retries: u32
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let opts: Opts = Opts::parse();

    let region = opts
        .region()
        .map_err(|err| format!("get region error: {}", err))?;
    let s3client = S3Client::new(region);

    let mut app = App::new(s3client, &opts.bucket, &opts.key, opts.retries, &opts.log).await?;

    let mut parts =
        upload::get_parts(&opts.pattern).map_err(|err| format!("get part files error: {}", err))?;

    for f in parts.iter() {
        println!("{:?}", f);
        let filepath = f.into_os_string().into_string()
            .map_err(|err| format!("error converting path to utf8: {:?}", err))?;
        let action = actions::Action::AddPart(actions::Part::new(filepath));
        log.append(wal::WalEntry::new(action.clone())).await?;
        state.apply(action)?;
    }

    upload::upload_or_abort(&s3client, parts, &opts.bucket, &opts.key)
        .await
        .map_err(|err| format!("upload error: {}", err))?;

    Ok(())
}

impl Opts {
    fn region(&self) -> std::result::Result<Region, Error> {
        if let Some(ref endpoint) = self.endpoint {
            Ok(Region::Custom {
                name: self
                    .region
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
