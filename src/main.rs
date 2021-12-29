#[macro_use]
extern crate rocket;

use std::env;
use std::env::VarError;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, ensure, Context, Result};
use rocket::data::{ByteUnit, Data, ToByteUnit};
use rocket::futures::FutureExt;
use rocket::tokio;
use rocket::tokio::io::AsyncReadExt as _;
use rocket::tokio::sync;
use rocket::State;
use s3::Region;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

struct Writer {
    inner: zstd::Encoder<'static, fs::File>,
    opened: Instant,
    marker: [u8; 8],
    docs_written: u64,
    uncompressed_bytes: u64,
}

struct Output {
    limit: ByteUnit,
    data_dir: OsString,
    out: Arc<sync::Mutex<Option<Writer>>>,
    tx: sync::mpsc::UnboundedSender<UploaderCommand>,
}

#[post("/store", data = "<data>")]
async fn store(data: Data<'_>, state: &State<Output>) -> io::Result<()> {
    let mut buf = Vec::new();
    data.open(state.limit).read_to_end(&mut buf).await?;

    match state.out.lock().await.as_mut() {
        Some(file) => {
            // if either of these fails, close the file and mark it as damaged, and rotate
            file.start_item(Some(buf.len()))?;
            file.inner.write_all(&buf)?;
        }
        None => unimplemented!("already shut down?"),
    };

    // ignore the error here (rx dropped); handled elsehow
    let _ = state.tx.send(UploaderCommand::Check);
    Ok(())
}

#[post("/api/flush")]
async fn flush(state: &State<Output>) -> io::Result<()> {
    let previous = state.out.lock().await.replace(new_file()?);

    if let Some(writer) = previous {
        writer.finish()?;
    }
    Ok(())
}

#[get("/healthcheck")]
async fn status(state: &State<Output>) -> io::Result<()> {
    Ok(())
}

fn path_for_now() -> String {
    let time = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .expect("static formatter");
    format!("{}.events.zstd", time)
}

fn new_file() -> io::Result<Writer> {
    let mut marker = [0u8; 8];
    getrandom::getrandom(&mut marker)?;
    Ok(Writer {
        inner: zstd::Encoder::new(
            fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open("current.events.zstd")?,
            3,
        )?,
        marker,
        opened: Instant::now(),
        docs_written: 0,
        uncompressed_bytes: 0,
    })
}

impl Writer {
    fn start_item(&mut self, data_len: Option<usize>) -> io::Result<()> {
        let now = OffsetDateTime::now_utc().unix_timestamp();

        let item_length = match data_len {
            Some(real) => u64::try_from(real).map_err(|_| io::ErrorKind::InvalidInput)? + 8 + 8 + 8,
            None => u64::MAX,
        };

        self.inner.write_all(&self.marker)?;
        self.inner.write_all(&item_length.to_le_bytes())?;
        self.inner.write_all(&now.to_le_bytes())?;

        self.docs_written += 1;
        self.uncompressed_bytes += item_length;

        Ok(())
    }

    fn finish(mut self) -> io::Result<()> {
        self.start_item(None)?;
        self.inner.finish()?.flush()?;
        Ok(())
    }
}

fn upload_dir(data_dir: impl AsRef<Path>) -> PathBuf {
    let mut buf = data_dir.as_ref().to_path_buf();
    buf.push("upload");
    buf
}

fn env_u64(name: &'static str, default: u64) -> Result<u64> {
    Ok(match env::var(name) {
        Ok(v) => v
            .parse()
            .with_context(|| anyhow!("parsing {}={:?} as an integer", name, v))?,
        Err(VarError::NotPresent) => default,
        Err(e) => Err(e).with_context(|| anyhow!("reading {}", name))?,
    })
}

fn s3_error_response(msg: &str) -> Option<String> {
    let doc = roxmltree::Document::parse(&msg).ok()?;
    if "Error" != doc.root_element().tag_name().name() {
        return None;
    }

    Some(
        doc.root_element()
            .children()
            .filter_map(|e| {
                if ["Code", "Message"].contains(&e.tag_name().name()) {
                    e.text().map(|v| v.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join(": "),
    )
}

#[derive(Copy, Clone, Debug)]
enum UploaderCommand {
    Check,
    Exit,
}

#[rocket::main]
async fn main() -> Result<()> {
    env_logger::init();

    let data_dir =
        env::var_os("BATCHY_DATA_DIR").unwrap_or_else(|| OsStr::new("/data").to_os_string());
    let limit_kb: u64 = env_u64("BATCHY_LIMIT_KB", 4096)?;

    let rotate_docs = env_u64("BATCHY_ROTATE_DOCS", 100_000)?;
    // 1000 * 1000kB = 1GB
    let rotate_data = env_u64("BATCHY_ROTATE_UNCOMPRESSED_KB", 1024 * 1024)?;
    let rotate_seconds = env_u64("BATCHY_ROTATE_SECONDS", 20 * 60)?;

    let bucket_name = env::var("BATCHY_BUCKET").unwrap_or_else(|_| "batchy3".to_string());
    let bucket_prefix = env::var("BATCHY_BUCKET_PREFIX").unwrap_or_default();

    let validate_credentials = env_u64("BATCHY_VALIDATE_CREDENTIALS", 1)? == 1;
    let s3region = env::var("AWS_DEFAULT_REGION").with_context(|| {
        anyhow!("AWS_DEFAULT_REGION is required; e.g. `us-east-1` or `http://localhost:9000`")
    })?;
    let is_minio = env_u64("BATCHY_MINIO_COMPAT", 0)? == 1;

    let s3region = if is_minio {
        Region::Custom {
            region: String::new(),
            endpoint: s3region,
        }
    } else {
        let parsed = s3region
            .parse()
            .with_context(|| anyhow!("parsing region {:?}", s3region))?;
        if match parsed {
            Region::Custom { .. } => true,
            _ => false,
        } {
            warn!(
                concat!(
                    "unrecognised region {:?}; if this is a (e.g. minio)",
                    " URL consider setting BATCHY_MINIO_COMPAT=1"
                ),
                s3region
            );
        }
        parsed
    };

    let s3creds = s3::creds::Credentials::default()
        .with_context(|| anyhow!("loading default AWS credentials"))?;

    let bucket = s3::bucket::Bucket::new_with_path_style(&bucket_name, s3region, s3creds)?;

    if validate_credentials {
        let path = format!("{}/.credentials-test", bucket_prefix);
        let (dat, code) = bucket
            .put_object(path.to_string(), b"")
            .await
            .with_context(|| anyhow!("validating credentials"))?;
        let resp = String::from_utf8_lossy(&dat);
        debug!("server said {}: {:?}", code, resp);
        ensure!(
            code == 200,
            "credentials test failed to create {:?} in {:?}; received a {}, server said: {:?}",
            path,
            bucket.name(),
            code,
            s3_error_response(&resp).unwrap_or_else(|| resp.chars().take(400).collect()),
        );
    }

    fs::create_dir_all(upload_dir(&data_dir))
        .with_context(|| anyhow!("preparing data directory BATCHY_DATA_DIR={:?}", &data_dir))?;

    let rc = Arc::new(sync::Mutex::new(Some(
        new_file().with_context(|| anyhow!("creating initial output file"))?,
    )));

    let (tx, mut rx) = sync::mpsc::unbounded_channel::<UploaderCommand>();

    let state = Output {
        out: Arc::clone(&rc),
        data_dir: data_dir.clone(),
        limit: limit_kb.kilobytes(),
        tx: tx.clone(),
    };

    let uploader_rc = Arc::clone(&rc);
    let uploader = tokio::task::spawn_blocking(move || async move {
        loop {
            match tokio::time::timeout(Duration::from_secs(10), rx.recv()).await {
                Ok(Some(UploaderCommand::Check)) => (),

                Ok(Some(UploaderCommand::Exit)) => return Ok::<(), anyhow::Error>(()),
                // everyone's disconnected
                Ok(None) => return Ok(()),

                // timeout (just do the full check; lazy)
                Err(_) => (),
            }

            {
                let mut rc = uploader_rc.lock().await;
                let writer = match rc.as_ref() {
                    Some(writer) => writer,
                    // no?
                    None => continue,
                };

                if writer.uncompressed_bytes <= rotate_data
                    && writer.docs_written <= rotate_docs
                    && writer.opened.elapsed().as_secs() <= rotate_seconds
                {
                    // all conditions are still okay
                    // no?
                    continue;
                }

                let writer = rc.take().unwrap();
                writer.finish()?;
                let mut dest = upload_dir(&data_dir);
                dest.push(path_for_now());
                std::fs::rename("current.events.zstd", dest)?;
                rc.replace(new_file()?);
            }

            for file in std::fs::read_dir(upload_dir(&data_dir))? {
                let file = file?;
                let name = file.file_name();
                let name = match name.to_str() {
                    Some(s) if !s.starts_with(".") => s,
                    Some(_) | None => continue,
                };

                if !file.file_type()?.is_file() {
                    continue;
                }

                let mut open = tokio::fs::File::open(file.file_name()).await?;
                let target = format!("{}/{}", bucket_prefix, name);
                let err = bucket.put_object_stream(&mut open, target).await?;
                ensure!(err == 200, "upload failinated");
            }
        }
    })
    .then(|e| async { e.with_context(|| anyhow!("butts")) });

    let web = rocket::build()
        .manage(state)
        .mount("/", routes![flush, store, status])
        .launch()
        .then(|e| async { e.with_context(|| anyhow!("webapp")) })
        .then(|e| async move {
            tx.send(UploaderCommand::Exit)?;
            e
        });

    let shutdown = tokio::try_join!(web, uploader);

    let mut guard = rc.lock().await;
    if let Some(writer) = guard.take() {
        writer.finish()?;
    }

    // .. what is try_join doing?
    let (web, uploader) = shutdown?;
    uploader.await?;

    Ok(())
}
