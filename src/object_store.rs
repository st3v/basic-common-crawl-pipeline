use aws_sdk_s3::primitives::{ByteStream, SdkBody};

pub struct ObjectStore {
    client: aws_sdk_s3::Client,
    bucket: String
}

impl ObjectStore {
    /// Returns an ObjectStore that's configured for either:
    ///   * Minio: if MINIO_URL env var is set
    ///   * AWS S3: if MINIO_URL env var is not set
    /// 
    /// In case of Minio, MINIO_ACCESS_KEY_ID and MINIO_SECRET_ACCESS_KEY
    /// env vars can be used to set access key credentials.
    pub async fn new(bucket: &str) -> ObjectStore {
        let bucket = bucket.to_string();

        if let Ok(client) = minio_client().await {
            tracing::info!("Using Minio for object storage");
            return ObjectStore{client, bucket}
        }

        tracing::info!("Using AWS S3 for object storage");
        let client = s3_client().await;
        return ObjectStore{client, bucket}
    }

    /// upload to the object store
    pub async fn upload(&self, key: &str, contents: &[u8]) -> Result<(), anyhow::Error> {
        let res = self.client.put_object()
            .bucket(self.bucket.clone())
            .key(key)
            .body(ByteStream::new(SdkBody::from(contents)))
            .send()
            .await;
            
        match res {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow::anyhow!(e))
        }
    }
}

async fn minio_client() -> Result<aws_sdk_s3::Client, anyhow::Error> {
    let url = std::env::var("MINIO_URL")?;

    let mut config = aws_sdk_s3::config::Builder::new()
        .endpoint_url(url)
        .region(aws_sdk_s3::config::Region::new("eu-central-1"))
        .behavior_version_latest()
        .force_path_style(true);

    if let (Ok(key_id), Ok(key)) = (std::env::var("MINIO_ACCESS_KEY_ID"), std::env::var("MINIO_SECRET_ACCESS_KEY")) {
            let creds = aws_sdk_s3::config::Credentials::new(
                key_id, 
                key, 
                None, 
                None, 
                "custom-env"
            );
            config = config.credentials_provider(creds);
    } else {
        tracing::warn!("MINIO_URL has been specified but MINIO_ACCESS_KEY_ID and/or MINIO_SECRET_ACCESS_KEY are not set. Using anonymous access.");
    }

    Ok(aws_sdk_s3::Client::from_conf(config.build()))
}

async fn s3_client() -> aws_sdk_s3::Client {
    let config = aws_config::load_from_env().await;
    aws_sdk_s3::Client::new(&config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[tokio::test]
    #[serial] // run these tests serial to avoid conflicts with env vars
    async fn test_minio_client() {
        std::env::set_var("MINIO_URL", "http://localhost:9000");
        std::env::set_var("MINIO_ACCESS_KEY_ID", "minio");
        std::env::set_var("MINIO_SECRET_ACCESS_KEY", "minio123");

        let client = minio_client().await;
        assert!(client.is_ok());
    }

    #[tokio::test]
    #[serial] // run these tests serial to avoid conflicts with env vars
    async fn test_minio_client_anonymous() {
        std::env::set_var("MINIO_URL", "http://localhost:9000");

        let client = minio_client().await;
        assert!(client.is_ok());
    }

    #[tokio::test]
    #[serial] // run these tests serial to avoid conflicts with env vars
    async fn test_minio_client_not_configured() {
        std::env::remove_var("MINIO_URL");

        let client = minio_client().await;
        assert!(client.is_err());
    }

    #[tokio::test]
    #[serial] // run these tests serial to avoid conflicts with env vars
    async fn test_new_with_minio() {
        std::env::set_var("MINIO_URL", "http://localhost:9000");

        let store = ObjectStore::new("test").await;
        assert!(store.bucket == "test");
    }

    #[tokio::test]
    #[serial] // run these tests serial to avoid conflicts with env vars
    async fn test_new_with_s3() {
        std::env::remove_var("MINIO_URL");

        let store = ObjectStore::new("test").await;
        assert!(store.bucket == "test");
    }
}