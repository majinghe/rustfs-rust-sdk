use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use std::env;

pub struct Config {
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint_url: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let region = env::var("RUSTFS_REGION")?;
        let access_key_id = env::var("RUSTFS_ACCESS_KEY_ID")?;
        let secret_access_key = env::var("RUSTFS_SECRET_ACCESS_KEY")?;
        let endpoint_url = env::var("RUSTFS_ENDPOINT_URL")?;

        // let endpoint_url = format!("{}/{}", endpoint_base, region);

        Ok(Config {
            region,
            access_key_id,
            secret_access_key,
            endpoint_url,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env()?;
    println!("Config loaded:");
    println!("  Region: {}", config.region);
    println!("  Endpoint: {}", config.endpoint_url);

    let credentials = Credentials::new(
        config.access_key_id,
        config.secret_access_key,
        None,
        None,
        "rustfs",
    );

    let region = Region::new(config.region);

    let endpoint_url = config.endpoint_url;

    let shard_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region)
        .credentials_provider(credentials)
        .endpoint_url(endpoint_url)
        .load()
        .await;

    let rustfs_client = Client::new(&shard_config);

    // create bucket
    match rustfs_client
        .create_bucket()
        .bucket("rust-sdk-1")
        .send()
        .await
    {
        Ok(_) => {
            println!("Bucket created successfully");
        }
        Err(e) => {
            println!("Error creating bucket: {:?}", e);
            return Err(e.into());
        }
    }

    // delete bucket
    match rustfs_client
        .delete_bucket()
        .bucket("cn-east-1rust-sdk")
        .send()
        .await
    {
        Ok(_) => {
            println!("Bucket deleted successfully");
        }
        Err(e) => {
            println!("Error deleting bucket: {:?}", e);
            return Err(e.into());
        }
    };

    // list buckets
    match rustfs_client.list_buckets().send().await {
        Ok(res) => {
            println!("Total buckets number is {:?}", res.buckets().len());
            for bucket in res.buckets() {
                println!("Bucket: {:?}", bucket.name());
            }
        }
        Err(e) => {
            println!("Error listing buckets: {:?}", e);
            return Err(e.into());
        }
    }

    // list object
    match rustfs_client
        .list_objects_v2()
        .bucket("rust-sdk-1")
        .send()
        .await
    {
        Ok(res) => {
            println!("Total objects number is {:?}", res.contents().len());
            for object in res.contents() {
                println!("Object: {:?}", object.key());
            }
        }
        Err(e) => {
            println!("Error listing objects: {:?}", e);
            return Err(e.into());
        }
    }

    Ok(())
}
