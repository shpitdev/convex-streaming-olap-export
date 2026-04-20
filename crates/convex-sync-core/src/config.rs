use std::path::PathBuf;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::errors::{AppError, AppResult};

#[derive(Debug, Clone)]
pub struct ConvexConnectionConfig {
    pub deployment_url: Url,
    pub deploy_key: String,
}

impl ConvexConnectionConfig {
    pub fn new(deployment_url: Url, deploy_key: String) -> AppResult<Self> {
        validate_deployment_url(&deployment_url)?;
        if deploy_key.trim().is_empty() {
            return Err(AppError::EmptyDeployKey);
        }

        Ok(Self {
            deployment_url,
            deploy_key,
        })
    }
}

fn validate_deployment_url(deployment_url: &Url) -> AppResult<()> {
    if deployment_url.host_str().is_none() {
        return Err(AppError::InvalidDeployUrl(
            "must contain a host".to_string(),
        ));
    }

    if deployment_url.path() != "/"
        || deployment_url.query().is_some()
        || deployment_url.username() != ""
        || deployment_url.password().is_some()
        || deployment_url.fragment().is_some()
        || (deployment_url.scheme() != "http" && deployment_url.scheme() != "https")
    {
        return Err(AppError::InvalidDeployUrl(
            "must be a root http(s) URL".to_string(),
        ));
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, ValueEnum, Default)]
#[serde(rename_all = "snake_case")]
pub enum OutputFormat {
    #[default]
    Json,
    Jsonl,
}

#[derive(Debug, Clone, Default)]
pub struct OutputConfig {
    pub output_path: Option<PathBuf>,
    pub format: OutputFormat,
}

#[cfg(test)]
mod tests {
    use url::Url;

    use super::ConvexConnectionConfig;

    #[test]
    fn accepts_root_url() {
        let config = ConvexConnectionConfig::new(
            Url::parse("https://happy-otter-123.convex.cloud").unwrap(),
            "prod:happy-otter-123|secret".to_string(),
        )
        .unwrap();

        assert_eq!(
            config.deployment_url.as_str(),
            "https://happy-otter-123.convex.cloud/"
        );
    }

    #[test]
    fn rejects_non_root_url() {
        let err = ConvexConnectionConfig::new(
            Url::parse("https://happy-otter-123.convex.cloud/api/list_snapshot").unwrap(),
            "prod:happy-otter-123|secret".to_string(),
        )
        .unwrap_err();

        assert!(err.to_string().contains("root http(s) URL"));
    }
}
