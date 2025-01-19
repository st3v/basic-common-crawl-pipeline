//! The worker(s) pull(s) messages from the RabbitMQ queue and downloads the WARC files that contain the actual content of the URLs.
//! Once the content has been downloaded, the worker extracts the text from the HTML file using the trafilatura Python package.
//!
//! After having downloaded and extracted the text from the HTML file, the worker could apply some filters to the extracted text.
//! We would also want to tokenize (for LLM training) the text and output it to a file.
//!
//! In its current implementation it does not refine or filter the extracted text in any way.
use std::io::Write;

use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::StreamExt;
use lapin::options::BasicAckOptions;
use pipeline::{
    commoncrawl::{download_and_unzip, CdxEntry}, metrics, object_store::ObjectStore, rabbitmq::{
        rabbitmq_channel_with_queue, rabbitmq_connection, rabbitmq_consumer, CC_QUEUE_NAME,
    }, trafilatura
};
use regex::Regex;
use warc::WarcHeader;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Specify port for metrics server
    #[arg(short = 'p', long, default_value_t = 9001)]
    metrics_port: u16,

    /// Specify bucket name for object storage
    #[arg(short = 'b', long, default_value = "data")]
    bucket: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    
    pipeline::tracing::setup();
    tokio::task::spawn(metrics::run_server(args.metrics_port));

    let rabbit_conn = rabbitmq_connection().await.unwrap();
    let (channel, _queue) = rabbitmq_channel_with_queue(&rabbit_conn, CC_QUEUE_NAME)
        .await
        .unwrap();
    let mut consumer = rabbitmq_consumer(&channel, CC_QUEUE_NAME, "worker")
        .await
        .unwrap();

    let store = ObjectStore::new(&args.bucket).await;

    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok(delivery) => {
                let batch = serde_json::from_slice::<Vec<CdxEntry>>(&delivery.data);
                tracing::info!(
                    "Received a batch of {} entries",
                    batch.as_ref().unwrap().len()
                );

                for entry in batch.unwrap() {
                    let url = &format!("https://data.commoncrawl.org/{}", entry.metadata.filename);
                    let res = download_and_unzip(url, entry.metadata.offset, entry.metadata.length).await;
                     
                    match res {
                        Ok(data) => {
                            for warc_entry in warc::WarcReader::new(data.as_slice()).iter_records() {
                                // no filtering yet, i.e. nothing is getting dropped
                                metrics::doc_filtered(false);

                                let warc_entry = warc_entry.unwrap();
                                if warc_entry.header(WarcHeader::WarcType).unwrap() != "response" {
                                    continue;
                                }

                                tracing::info!(
                                    "Successfully read WARC entry with URL {}",
                                    warc_entry.header(WarcHeader::TargetURI).unwrap()
                                );

                                let raw_content = String::from_utf8_lossy(warc_entry.body());
                                let html_begin_index = raw_content.find("\n\n");
                                let Some(html_begin_index) = html_begin_index else {
                                    tracing::warn!("Failed to find HTML content in WARC entry");
                                    continue;
                                };

                                tracing::debug!(
                                    "First 2000 characters of raw content: {}",
                                    &raw_content[..2000]
                                );

                                let opt = match trafilatura::extract(&raw_content[html_begin_index..]) {
                                    Ok(opt) => { opt },
                                    Err(e) => {
                                        tracing::warn!(err.msg = %e, err.details = ?e, "Failed to extract content from WARC entry");
                                        continue
                                    }
                                };
                                
                                let content = match opt {
                                    Some(content) => {
                                        tracing::info!("Extracted content of length {}", content.len());
                                        tracing::debug!("Extracted content: {}", &content);
                                        content
                                    },
                                    None => {
                                        continue
                                    }
                                };

                                let key = match get_key(&warc_entry) {
                                    Ok(k) => {
                                        format!("{}.gz", k)
                                    },
                                    Err(e) => {
                                        tracing::warn!(err.msg = %e, err.details = ?e);
                                        continue;
                                    }
                                };

                                let content = match compress(&content) {
                                    Ok(c) => { c },
                                    Err(e) => {
                                        tracing::warn!(err.msg = %e, err.details = ?e);
                                        continue;
                                    }   
                                };

                                match store.upload(key.as_str(), content.as_slice()).await {
                                    Ok(_) => {
                                        tracing::info!("Uploaded {} to object store", key);
                                        metrics::doc_stored();
                                    },
                                    Err(e) => {
                                        tracing::warn!(err.msg = %e, err.details = ?e, "Failed to upload {key} to object store");
                                    }
                                };
                            }
                        },
                        Err(e) => {
                            tracing::warn!(err.msg = %e, err.details = ?e, "Failed to download and unzip url {}", url);
                        }
                    }
                }
                delivery.ack(BasicAckOptions::default()).await.unwrap();
            }
            Err(e) => {
                tracing::warn!(err.msg = %e, err.details = ?e, "Failed to receive message from RabbitMQ. Reconnecting.");
                continue;
            }
        }
    }
}

fn get_key(record: &warc::Record<warc::BufferedBody>) -> Result<String, anyhow::Error> {
    // let id = record.header(WarcHeader::WarcInfoID).ok_or(anyhow::anyhow!("No WarcInfoID found"))?;
    let id = record.warc_id();
    let re = Regex::new(r"^<.*:.*:(.*)>$").unwrap();
    let Some(caps) = re.captures(&id) else { anyhow::bail!("No UUID found in {}", id) };
    Ok(caps[1].to_string())
}

fn compress(content: &str) -> Result<Vec<u8>, anyhow::Error> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(content.as_bytes())?;
    Ok(encoder.finish()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_key() {
        let mut record = warc::RecordBuilder::default().build().unwrap();

        // should return a valid UUID string
        let uuid = get_key(&record).unwrap();
        assert!(uuid::Uuid::parse_str(uuid.as_str()).is_ok());

        // should return the expected UUID string
        let expected = "4934abe4-a31b-49ac-b566-ea4e983b3291";
        record.set_warc_id(format!("<urn:uuid:{}>", expected));
        assert_eq!(get_key(&record).unwrap(), expected);
    }

    #[test]
    fn test_compress() {
        let content = "Hello, world!";
        let gzipped = compress(content).unwrap();

        let mut decoder = flate2::read::GzDecoder::new(&gzipped[..]);
        let mut buffer = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut buffer).unwrap();
        assert_eq!(buffer, content);
    }
}