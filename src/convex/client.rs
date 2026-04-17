use reqwest::{
    header::{HeaderMap, HeaderValue, AUTHORIZATION},
    Client,
};
use serde::de::DeserializeOwned;
use url::Url;

use crate::{
    config::ConvexConnectionConfig,
    errors::{AppError, AppResult},
};

#[derive(Clone)]
pub struct ConvexClient {
    http: Client,
    deployment_url: Url,
}

impl ConvexClient {
    pub fn new(config: ConvexConnectionConfig) -> AppResult<Self> {
        let mut headers = HeaderMap::new();
        let authorization = format!("Convex {}", config.deploy_key);
        let authorization = HeaderValue::from_str(&authorization)
            .map_err(|err| AppError::TelemetryInit(err.to_string()))?;
        headers.insert(AUTHORIZATION, authorization);

        let http = Client::builder().default_headers(headers).build()?;
        Ok(Self {
            http,
            deployment_url: config.deployment_url,
        })
    }

    pub async fn get<T>(&self, path: &str, query: &[(String, String)]) -> AppResult<T>
    where
        T: DeserializeOwned,
    {
        let url = self.deployment_url.join(path)?;
        let response = self
            .http
            .get(url)
            .query(query)
            .send()
            .await?
            .error_for_status()?;

        Ok(response.json::<T>().await?)
    }
}
