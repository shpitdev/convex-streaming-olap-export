use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, Region};
use aws_sdk_s3::{primitives::ByteStream, Client};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

use crate::errors::{AppError, AppResult};

const MANIFEST_VERSION: i64 = 1;

#[derive(Debug, Clone)]
pub struct PublishS3Options {
    pub staging_dir: PathBuf,
    pub bucket: String,
    pub prefix: Option<String>,
    pub region: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublishS3Summary {
    pub bucket: String,
    pub prefix: String,
    pub publish_id: String,
    pub manifest_key: String,
    pub latest_manifest_key: String,
    pub tables_total: usize,
    pub tables_uploaded: usize,
    pub tables_deleted: usize,
    pub tables_unchanged: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StagingPublishManifest {
    pub version: i64,
    pub publish_id: String,
    pub published_at_epoch_ms: u128,
    pub bucket: String,
    pub prefix: String,
    pub tables: BTreeMap<String, PublishedStagingTable>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublishedStagingTable {
    pub relative_path: String,
    pub current_key: String,
    pub versioned_key: String,
    pub sha256: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalStagingTable {
    relative_path: String,
    absolute_path: PathBuf,
    sha256: String,
    bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublishPlan {
    manifest: StagingPublishManifest,
    uploads: Vec<LocalStagingTable>,
    delete_current_keys: Vec<String>,
    unchanged: usize,
}

pub async fn publish_staging_to_s3(options: &PublishS3Options) -> AppResult<PublishS3Summary> {
    let prefix = normalize_prefix(options.prefix.as_deref());
    let tables = collect_local_staging_tables(&options.staging_dir)?;
    let publish_id = next_publish_id()?;
    let published_at_epoch_ms = publish_id
        .parse::<u128>()
        .map_err(|err| AppError::S3(format!("invalid publish id: {err}")))?;
    let region_provider = match options.region.as_deref() {
        Some(region) => RegionProviderChain::first_try(Region::new(region.to_string())),
        None => RegionProviderChain::default_provider(),
    };
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    let client = Client::new(&config);

    let latest_manifest_key = latest_manifest_key(&prefix);
    let previous_manifest =
        load_manifest_if_present(&client, &options.bucket, &latest_manifest_key).await?;
    let plan = build_publish_plan(
        previous_manifest.as_ref(),
        &options.bucket,
        &prefix,
        &publish_id,
        published_at_epoch_ms,
        tables,
    );

    for table in &plan.uploads {
        let versioned_key = versioned_table_key(&prefix, &publish_id, &table.relative_path);
        let current_key = current_table_key(&prefix, &table.relative_path);
        put_file(
            &client,
            &options.bucket,
            &versioned_key,
            &table.absolute_path,
        )
        .await?;
        put_file(&client, &options.bucket, &current_key, &table.absolute_path).await?;
    }

    let manifest_key = manifest_key(&prefix, &publish_id);
    put_json(
        &client,
        &options.bucket,
        &manifest_key,
        &serde_json::to_vec_pretty(&plan.manifest)?,
    )
    .await?;
    put_json(
        &client,
        &options.bucket,
        &latest_manifest_key,
        &serde_json::to_vec_pretty(&plan.manifest)?,
    )
    .await?;

    for key in &plan.delete_current_keys {
        delete_object_if_exists(&client, &options.bucket, key).await?;
    }

    Ok(PublishS3Summary {
        bucket: options.bucket.clone(),
        prefix,
        publish_id,
        manifest_key,
        latest_manifest_key,
        tables_total: plan.manifest.tables.len(),
        tables_uploaded: plan.uploads.len(),
        tables_deleted: plan.delete_current_keys.len(),
        tables_unchanged: plan.unchanged,
    })
}

fn build_publish_plan(
    previous_manifest: Option<&StagingPublishManifest>,
    bucket: &str,
    prefix: &str,
    publish_id: &str,
    published_at_epoch_ms: u128,
    local_tables: Vec<LocalStagingTable>,
) -> PublishPlan {
    let previous_tables = previous_manifest
        .map(|manifest| manifest.tables.clone())
        .unwrap_or_default();

    let local_map: BTreeMap<String, LocalStagingTable> = local_tables
        .into_iter()
        .map(|table| (table.relative_path.clone(), table))
        .collect();

    let mut delete_current_keys = Vec::new();
    for (relative_path, previous) in &previous_tables {
        if !local_map.contains_key(relative_path) {
            delete_current_keys.push(previous.current_key.clone());
        }
    }

    let mut uploads = Vec::new();
    let mut tables = BTreeMap::new();
    let mut unchanged = 0usize;

    for (relative_path, local) in local_map {
        let previous = previous_tables.get(&relative_path);
        let changed = match previous {
            Some(previous) => previous.sha256 != local.sha256 || previous.bytes != local.bytes,
            None => true,
        };

        if changed {
            uploads.push(local.clone());
            tables.insert(
                relative_path.clone(),
                PublishedStagingTable {
                    relative_path: relative_path.clone(),
                    current_key: current_table_key(prefix, &relative_path),
                    versioned_key: versioned_table_key(prefix, publish_id, &relative_path),
                    sha256: local.sha256.clone(),
                    bytes: local.bytes,
                },
            );
        } else if let Some(previous) = previous {
            unchanged += 1;
            tables.insert(relative_path, previous.clone());
        }
    }

    PublishPlan {
        manifest: StagingPublishManifest {
            version: MANIFEST_VERSION,
            publish_id: publish_id.to_string(),
            published_at_epoch_ms,
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            tables,
        },
        uploads,
        delete_current_keys,
        unchanged,
    }
}

fn collect_local_staging_tables(staging_dir: &Path) -> AppResult<Vec<LocalStagingTable>> {
    if !staging_dir.exists() {
        return Err(AppError::InvalidStagingPath(format!(
            "{} does not exist",
            staging_dir.display()
        )));
    }

    let mut tables = Vec::new();
    for entry in WalkDir::new(staging_dir).follow_links(false) {
        let entry = entry.map_err(|err| AppError::Io(std::io::Error::other(err)))?;
        if !entry.file_type().is_file() {
            continue;
        }
        if entry.path().extension().is_none_or(|ext| ext != "parquet") {
            continue;
        }

        let relative_path = to_relative_key(staging_dir, entry.path())?;
        let bytes = entry
            .metadata()
            .map_err(|err| AppError::Io(std::io::Error::other(err)))?
            .len();
        let sha256 = hex::encode(Sha256::digest(fs::read(entry.path())?));
        tables.push(LocalStagingTable {
            relative_path,
            absolute_path: entry.into_path(),
            sha256,
            bytes,
        });
    }

    tables.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(tables)
}

fn to_relative_key(root: &Path, path: &Path) -> AppResult<String> {
    let relative = path.strip_prefix(root).map_err(|err| {
        AppError::InvalidStagingPath(format!(
            "failed to strip prefix for {}: {err}",
            path.display()
        ))
    })?;
    let segments = relative
        .iter()
        .map(|segment| {
            segment.to_str().ok_or_else(|| {
                AppError::InvalidStagingPath(format!(
                    "non-utf8 staging path segment in {}",
                    path.display()
                ))
            })
        })
        .collect::<AppResult<Vec<_>>>()?;
    Ok(segments.join("/"))
}

fn normalize_prefix(prefix: Option<&str>) -> String {
    prefix
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.trim_matches('/').to_string())
        .unwrap_or_default()
}

fn current_table_key(prefix: &str, relative_path: &str) -> String {
    join_key(prefix, ["staging", "current", relative_path])
}

fn versioned_table_key(prefix: &str, publish_id: &str, relative_path: &str) -> String {
    join_key(prefix, ["staging", "versions", publish_id, relative_path])
}

fn manifest_key(prefix: &str, publish_id: &str) -> String {
    join_key(
        prefix,
        ["staging", "manifests", &format!("{publish_id}.json")],
    )
}

fn latest_manifest_key(prefix: &str) -> String {
    join_key(prefix, ["staging", "manifests", "latest.json"])
}

fn join_key<const N: usize>(prefix: &str, parts: [&str; N]) -> String {
    let mut segments = Vec::with_capacity(N + usize::from(!prefix.is_empty()));
    if !prefix.is_empty() {
        segments.push(prefix.to_string());
    }
    segments.extend(
        parts
            .into_iter()
            .filter(|part| !part.is_empty())
            .map(|part| part.trim_matches('/').to_string()),
    );
    segments.join("/")
}

fn next_publish_id() -> AppResult<String> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| AppError::S3(format!("system time before unix epoch: {err}")))?
        .as_millis()
        .to_string())
}

async fn load_manifest_if_present(
    client: &Client,
    bucket: &str,
    key: &str,
) -> AppResult<Option<StagingPublishManifest>> {
    match client.get_object().bucket(bucket).key(key).send().await {
        Ok(response) => {
            let bytes = response
                .body
                .collect()
                .await
                .map_err(|err| AppError::S3(format!("{err:?}")))?
                .into_bytes();
            Ok(Some(serde_json::from_slice(&bytes)?))
        },
        Err(err) if is_missing_key(&format!("{err:?}")) => Ok(None),
        Err(err) => Err(AppError::S3(format!("{err:?}"))),
    }
}

async fn put_file(client: &Client, bucket: &str, key: &str, path: &Path) -> AppResult<()> {
    let body = ByteStream::from_path(path.to_path_buf())
        .await
        .map_err(|err| AppError::S3(format!("{err:?}")))?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await
        .map_err(|err| AppError::S3(format!("{err:?}")))?;
    Ok(())
}

async fn put_json(client: &Client, bucket: &str, key: &str, bytes: &[u8]) -> AppResult<()> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(bytes.to_vec()))
        .content_type("application/json")
        .send()
        .await
        .map_err(|err| AppError::S3(format!("{err:?}")))?;
    Ok(())
}

async fn delete_object_if_exists(client: &Client, bucket: &str, key: &str) -> AppResult<()> {
    client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|err| AppError::S3(format!("{err:?}")))?;
    Ok(())
}

fn is_missing_key(message: &str) -> bool {
    message.contains("NoSuchKey")
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{
        build_publish_plan, collect_local_staging_tables, current_table_key, manifest_key,
        normalize_prefix, versioned_table_key, LocalStagingTable, PublishedStagingTable,
        StagingPublishManifest,
    };

    #[test]
    fn builds_publish_plan_with_changed_unchanged_and_deleted_tables() {
        let previous = StagingPublishManifest {
            version: 1,
            publish_id: "100".to_string(),
            published_at_epoch_ms: 100,
            bucket: "bucket".to_string(),
            prefix: "exports".to_string(),
            tables: BTreeMap::from([
                (
                    "_root/jobs.parquet".to_string(),
                    PublishedStagingTable {
                        relative_path: "_root/jobs.parquet".to_string(),
                        current_key: current_table_key("exports", "_root/jobs.parquet"),
                        versioned_key: versioned_table_key("exports", "100", "_root/jobs.parquet"),
                        sha256: "same".to_string(),
                        bytes: 10,
                    },
                ),
                (
                    "workflow/events.parquet".to_string(),
                    PublishedStagingTable {
                        relative_path: "workflow/events.parquet".to_string(),
                        current_key: current_table_key("exports", "workflow/events.parquet"),
                        versioned_key: versioned_table_key(
                            "exports",
                            "100",
                            "workflow/events.parquet",
                        ),
                        sha256: "old".to_string(),
                        bytes: 12,
                    },
                ),
            ]),
        };

        let plan = build_publish_plan(
            Some(&previous),
            "bucket",
            "exports",
            "200",
            200,
            vec![
                LocalStagingTable {
                    relative_path: "_root/jobs.parquet".to_string(),
                    absolute_path: PathBuf::from("/tmp/_root/jobs.parquet"),
                    sha256: "same".to_string(),
                    bytes: 10,
                },
                LocalStagingTable {
                    relative_path: "workflow/steps.parquet".to_string(),
                    absolute_path: PathBuf::from("/tmp/workflow/steps.parquet"),
                    sha256: "new".to_string(),
                    bytes: 5,
                },
            ],
        );

        assert_eq!(plan.unchanged, 1);
        assert_eq!(plan.uploads.len(), 1);
        assert_eq!(plan.delete_current_keys.len(), 1);
        assert!(plan.manifest.tables.contains_key("_root/jobs.parquet"));
        assert!(plan.manifest.tables.contains_key("workflow/steps.parquet"));
        assert!(!plan.manifest.tables.contains_key("workflow/events.parquet"));
    }

    #[test]
    fn collects_local_staging_tables_as_relative_keys() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("publish-staging-{nanos}"));
        fs::create_dir_all(root.join("workflow")).unwrap();
        fs::write(root.join("workflow").join("events.parquet"), b"hello").unwrap();
        fs::write(root.join("ignore.txt"), b"nope").unwrap();

        let tables = collect_local_staging_tables(&root).unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].relative_path, "workflow/events.parquet");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn normalizes_prefixes_and_manifest_keys() {
        assert_eq!(normalize_prefix(Some("/exports/")), "exports");
        assert_eq!(normalize_prefix(Some("   ")), "");
        assert_eq!(
            manifest_key("exports", "200"),
            "exports/staging/manifests/200.json"
        );
    }
}
