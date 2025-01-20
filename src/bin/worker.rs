//! The worker(s) pull(s) messages from the RabbitMQ queue and downloads the WARC files that contain the actual content of the URLs.
//! Once the content has been downloaded, the worker extracts the text from the HTML file using the trafilatura Python package.
//!
//! After having downloaded and extracted the text from the HTML file, the worker could apply some filters to the extracted text.
//!
//! After extracting the test the worker tokenizes it (for LLM training) and stores the results in an object store.
//!
//! In its current implementation it does not refine or filter the extracted text in any way.
use std::sync::Arc;

use futures_util::StreamExt;
use lapin::options::BasicAckOptions;
use pipeline::{
    commoncrawl::{download_and_unzip, CdxEntry}, metrics, object_store::ObjectStore, rabbitmq::{
        rabbitmq_channel_with_queue, rabbitmq_connection, rabbitmq_consumer, CC_QUEUE_NAME,
    }, trafilatura, tokenizer::Tokenizer
};
use regex::Regex;
use tokio::sync::Mutex;
use warc::{BufferedBody, WarcHeader};
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

    /// Specify the name of a pre-trained tokenizer available on Hugging Face
    #[arg(short = 't', long, default_value = "bert-base-cased")]
    tokenizer: String,
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
    let tokenizer = Arc::new(Mutex::new(Tokenizer::new(&args.tokenizer).await.unwrap()));

    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok(delivery) => {
                let batch = serde_json::from_slice::<Vec<CdxEntry>>(&delivery.data);
                tracing::info!(
                    "Received a batch of {} entries",
                    batch.as_ref().unwrap().len()
                );

                // collect processed documents
                let processed_docs = Arc::new(Mutex::new(Collection::new()));

                // need to synchronize access to trafilatura to prevent deadlocks
                let trafilatura = Arc::new(Mutex::new(()));

                // process batch entries in parallel
                let mut tasks = tokio::task::JoinSet::new();
                for batch_entry in batch.unwrap() {
                    tasks.spawn(process_batch_entry(
                        batch_entry,
                        Arc::clone(&trafilatura),
                        Arc::clone(&tokenizer),
                        Arc::clone(&processed_docs)
                    ));
                }

                // wait for all tasks to finish
                tasks.join_all().await;

                // store processed documents
                let parquet = processed_docs.lock().await.to_parquet();
                // todo: use deterministic IDs for batches
                let batch_id = uuid::Uuid::new_v4().to_string();
                // todo: include language folder in path
                let key = format!("contents/batch={}/batch.parquet", batch_id);

                match store.upload(key.as_str(), parquet).await {
                    Ok(_) => {
                        tracing::info!("Successfully uploaded parquet file to object store");
                        delivery.ack(BasicAckOptions::default()).await.unwrap();
                    },
                    Err(e) => {
                        tracing::warn!(err.msg = %e, err.details = ?e, "Failed to upload parquet file to object store")
                    }
                }
            }
            Err(e) => {
                tracing::warn!(err.msg = %e, err.details = ?e, "Failed to receive message from RabbitMQ. Reconnecting.");
                continue;
            }
        }
    }
}

/// Takes a batch entry and downloads the corresponding WARC records.
/// For each such record, extracts the text from the HTML content and
/// tokenizes it. The corrresponding results are collected in a shared
/// collection from which they can be written to a Parquet file.
async fn process_batch_entry(entry: CdxEntry, trafilatura: Arc<Mutex<()>>, tokenizer: Arc<Mutex<Tokenizer>>, collection: Arc<Mutex<Collection>>) {
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

                let lock = trafilatura.lock().await;
                let opt = match trafilatura::extract(&raw_content[html_begin_index..]) {
                    Ok(opt) => { opt },
                    Err(e) => {
                        tracing::warn!(err.msg = %e, err.details = ?e, "Failed to extract content from WARC entry");
                        continue
                    }
                };
                drop(lock);

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

                match tokenizer.lock().await.encode(content.as_str()).await {
                    Ok(tokens) => {
                        tracing::info!(
                            "Successfully tokenized content: {} tokens",
                            tokens.len()
                        );
                        let mut guard = collection.lock().await;
                        guard.add(
                            warc_entry.id().unwrap(),
                            warc_entry.header(WarcHeader::TargetURI).unwrap().to_string(),
                            warc_entry.header(WarcHeader::Date).unwrap().to_string(),
                            tokens
                        );
                    },
                    Err(e) => {
                        tracing::warn!(err.msg = %e, err.details = ?e, "Failed to tokenize content");
                        continue;
                    }
                };
            }
        },
        Err(e) => {
            tracing::warn!(err.msg = %e, err.details = ?e, "Failed to download and unzip url {}", url);
        }
    }
}

// Collection used to store processed documents
struct Collection {
    ids: Vec<String>,
    urls: Vec<String>,
    timestamps: Vec<String>,
    token_lists: Vec<Vec<String>>
}

impl Collection {
    /// Returns an empty Collection.
    pub fn new() -> Self {
        Collection {
            ids: Vec::new(),
            urls: Vec::new(),
            timestamps: Vec::new(),
            token_lists: Vec::new()
        }
    }

    /// Adds a document to the Collection.
    pub fn add(&mut self, id: String, url: String, timestamp: String, tokens: Vec<String>) {
        self.ids.push(id);
        self.urls.push(url);
        self.timestamps.push(timestamp);
        self.token_lists.push(tokens);
    }

    /// Encodes the Collection as a Parquet file.
    pub fn to_parquet(&self) -> Vec<u8> {
        let ids = self.ids.iter().map(|s| s.as_str()).collect();
        let urls = self.urls.iter().map(|s| s.as_str()).collect();
        let timestamps = self.timestamps.iter().map(|s| s.as_str()).collect();
        let token_lists = self.token_lists.iter().map(|v| v.iter().map(|s| s.as_str()).collect()).collect();
        pipeline::parquet::encode(ids, urls, timestamps, token_lists)
    }
}
trait Id {
    fn id(&self) -> Result<String, anyhow::Error>;
}

impl Id for warc::Record<BufferedBody> {
    /// Returns the ID of the WARC record.
    fn id(&self) -> Result<String, anyhow::Error> {
        let id = self.warc_id();
        let re = Regex::new(r"^<.*:.*:(.*)>$").unwrap();
        let Some(caps) = re.captures(&id) else { anyhow::bail!("No UUID found in {}", id) };
        Ok(caps[1].to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_warc_record_id() {
        let mut record = warc::RecordBuilder::default().build().unwrap();

        // should return a valid UUID string
        let uuid = record.id().unwrap();
        assert!(uuid::Uuid::parse_str(uuid.as_str()).is_ok());

        // should return the expected UUID string
        let expected = "4934abe4-a31b-49ac-b566-ea4e983b3291";
        record.set_warc_id(format!("<urn:uuid:{}>", expected));
        assert_eq!(record.id().unwrap(), expected);
    }
}