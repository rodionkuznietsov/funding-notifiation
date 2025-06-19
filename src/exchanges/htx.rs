use reqwest::Error;
use serde::Deserialize;
use std::time::Duration;
use std::{collections::HashSet};
use tokio::sync::{Mutex, Semaphore};
use std::sync::Arc;

use crate::THRESHOLD_RATE_FUNDING;
use crate::convert_to_berlin_time;
use crate::FundingParameter;
use crate::api_limits::ApiLimits;

#[derive(Debug, Default, Deserialize)]
struct ApiResponse {
    #[serde(default)]
    data: Vec<DataResponse>
}

#[derive(Debug, Default, Deserialize)]
struct DataResponse {
    #[serde(default)]
    contract_code: String,
    #[serde(default)]
    funding_rate: String,
    #[serde(default)]
    funding_time: String,
    #[serde(default, rename="close")]
    mark_price: String
}

#[derive(Debug, Default, Deserialize)]
struct ApiResponse1 {
    #[serde(default)]
    data: DataResponse,
    #[serde(default)]
    tick: DataResponse
}

#[derive(Debug, Clone)]
pub struct FoundFundingHtx {
    pub exchange_name: String,
    pub symbol: String,
    pub mark_price: String,
    pub last_funding_rate: String,
    pub next_funding_time: String,
    pub unix_time: i64
}

impl FundingParameter for FoundFundingHtx {
    fn symbol(&self) -> &String {
        &self.symbol
    }
    fn mark_price(&self) -> &String {
        &self.mark_price
    }
    fn last_funding_rate(&self) -> &String {
        &self.last_funding_rate
    }
    fn exchange_name(&self) -> &String {
        &self.exchange_name
    }
    fn next_funding_time(&self) -> &String {
        &self.next_funding_time
    }
    fn unix_time(&self) -> &i64 {
        &self.unix_time
    }
}

pub async fn search_funding_htx() -> Result<Vec<FoundFundingHtx>, Error> {
    let url = "https://api.hbdm.com/linear-swap-api/v1/swap_contract_info";
    let client = reqwest::Client::new();
    let response = client.get(url)
        .send()
        .await?;
    let response_json: ApiResponse = response.json().await?;

    let limit_requests = ApiLimits::Htx.limit();
    let semaphore = Arc::new(Semaphore::new(limit_requests));
    let seen_symbols = Arc::new(Mutex::new(HashSet::new()));
    let mut futures_vec = Vec::new();

    for data in response_json.data {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let seen_symbols = Arc::clone(&seen_symbols);
        let client = client.clone();

        let fut = tokio::spawn(async move {
            let contract_code = data.contract_code.clone();

            let mut seen = seen_symbols.lock().await;
            if !seen.insert(contract_code.clone()) {
                return None;
            }

            drop(seen); // Освобождаем моментально токен из seen

            let (funding_rate, funding_time) = search_funding(
                contract_code.clone(),
                &client,
                &permit
            ).await.unwrap();
            
            if funding_rate.abs() < THRESHOLD_RATE_FUNDING {
                return None;
            }

            let mark_price = search_mark_price(
                contract_code.clone(),
                &client,
                &permit
            ).await.unwrap();
                
            let next_funding_datetime = convert_to_berlin_time(funding_time);

            let found_funding= FoundFundingHtx {
                exchange_name: "Htx".to_string(),
                symbol: contract_code.replace("-", ""),
                mark_price: mark_price.to_string(),
                last_funding_rate: format!("{:.4}", funding_rate),
                next_funding_time: next_funding_datetime,
                unix_time: funding_time,
            };


            Some(found_funding)
        }); 

        futures_vec.push(fut);
    }

    let results = futures::future::join_all(futures_vec).await;
    let rr: Vec<FoundFundingHtx> = results.into_iter()
        .filter_map(|outer| outer.ok().flatten())
        .collect();
        
    Ok(rr)
}

async fn search_funding(
    contract_code: String,
    client: &reqwest::Client,
    permit: &tokio::sync::OwnedSemaphorePermit
) -> Option<(f64, i64)> {
    let _permit = permit;
    tokio::time::sleep(Duration::from_millis(1)).await;

    let url = "https://api.hbdm.com/linear-swap-api/v1/swap_funding_rate";
    let response = client.get(url)
        .query(&[("contract_code", contract_code)])
        .send()
        .await.unwrap();
    let response_json: ApiResponse1 = response.json().await.unwrap();
    
    let funding_rate = response_json.data.funding_rate.parse::<f64>().unwrap_or(0.0) * 100.00;
    let funding_time = response_json.data.funding_time.parse::<i64>().unwrap_or(0);
    
    Some((funding_rate, funding_time))
}

async fn search_mark_price(
    contract_code: String,
    client: &reqwest::Client,
    permit: &tokio::sync::OwnedSemaphorePermit
) -> Option<f64> {
    let _permit = permit;
    tokio::time::sleep(Duration::from_millis(1)).await;

    let url = "https://api.hbdm.com/linear-swap-ex/market/detail/merged";
    let response = client.get(url)
        .query(&[("contract_code", contract_code)])
        .send()
        .await.unwrap();
    let response_json: ApiResponse1 = response.json().await.unwrap();

    let mark_price = response_json.tick.mark_price.parse::<f64>().unwrap_or(0.0);

    Some(mark_price)
}