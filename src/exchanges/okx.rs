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
    data: Vec<DataResponse>
}

#[derive(Debug, Default, Deserialize)]
struct DataResponse {
    #[serde(default, rename="instId")]
    inst_id: String,
    #[serde(default, rename="fundingRate")]
    funding_rate: String,
    #[serde(default, rename="markPx")]
    mark_price: String,
    #[serde(default, rename="nextFundingTime")]
    next_funding_time: String
}

#[derive(Debug, Clone)]
pub struct FoundFundingOkx {
    pub exchange_name: String,
    pub symbol: String,
    pub mark_price: String,
    pub last_funding_rate: String,
    pub next_funding_time: String,
    pub unix_time: i64
}

impl FundingParameter for FoundFundingOkx {
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

pub async fn search_funding_okx() -> Result<Vec<FoundFundingOkx>, Error> {
    let url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP";
    let client = reqwest::Client::new();
    let response = client.get(url)
        .send()
        .await?;
    let response_json: ApiResponse = response.json().await?;

    let limit_requests = ApiLimits::Okx.limit();
    let semaphore = Arc::new(Semaphore::new(limit_requests));
    let seen_symbols = Arc::new(Mutex::new(HashSet::new()));
    let mut futures_vec = Vec::new();

    for data in response_json.data {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let seen_symbols = Arc::clone(&seen_symbols);
        let client = client.clone();

        let fut = tokio::spawn(async move {
            let inst_id = data.inst_id.clone();

            // Проверка на уникальность токенов
            let mut seen = seen_symbols.lock().await;
            if !seen.insert(data.inst_id.clone()) {
                return None;
            }

            drop(seen); // Освобождаем seen

            let (funding_rate, next_funding_time) = search_funding(
                inst_id.clone(),
                &client, 
                &permit
            ).await.unwrap();

            if funding_rate.abs() < THRESHOLD_RATE_FUNDING {
                return None;
            }

            let mark_price = search_mark_price(
                inst_id.clone(),
                &client,
                &permit
            ).await.unwrap();

            let next_funding_datetime = convert_to_berlin_time(next_funding_time);

            let found_funding = FoundFundingOkx {
                exchange_name: "Okx".to_string(),
                symbol: inst_id.trim_end_matches("-SWAP").replace("-", "").to_string(),
                mark_price: mark_price.to_string(),
                last_funding_rate: funding_rate.to_string(),
                next_funding_time: next_funding_datetime,
                unix_time: next_funding_time,
            };

            // println!("{:?}", found_funding);

            Some(found_funding)
        });

        futures_vec.push(fut);
    }

    let results = futures::future::join_all(futures_vec).await;
    let rr: Vec<FoundFundingOkx> = results.into_iter()
        .filter_map(|outer| outer.ok().flatten())
        .collect();

    Ok(rr)
}

async fn search_funding(
    inst_id: String,
    client: &reqwest::Client,
    permit: &tokio::sync::OwnedSemaphorePermit
) -> Option<(f64, i64)> {
    let _permit = permit;
    tokio::time::sleep(Duration::from_millis(1)).await;

    let url = format!("https://www.okx.com/api/v5/public/funding-rate?instId={}", inst_id);
    let response = client.get(url)
        .send()
        .await.ok()?;
    let response_json: ApiResponse = response.json().await.ok()?;

    for data in response_json.data {
        let funding_rate = data.funding_rate.parse::<f64>().unwrap_or(0.0) * 100.00;
        let next_funding_time = data.next_funding_time.parse::<i64>().unwrap_or(0);

        // println!("{}: {}", data.inst_id, funding_rate);

        return Some((funding_rate, next_funding_time))
    }

    None
}

async fn search_mark_price(
    inst_id: String,
    client: &reqwest::Client,
    permit: &tokio::sync::OwnedSemaphorePermit
) -> Option<f64> {

    let _permit = permit;
    tokio::time::sleep(Duration::from_millis(1)).await;

    let url = format!("https://www.okx.com/api/v5/public/mark-price?instType=SWAP&instId={}", inst_id);
    let response = client.get(url)
        .send()
        .await.ok()?;
    let response_json: ApiResponse = response.json().await.ok()?;

    for data in response_json.data {
        let mark_price = data.mark_price.parse::<f64>().unwrap_or(0.0);

        // println!("MarkPRICE: {}: {}", data.inst_id, mark_price);

        return Some(mark_price);
    }

    None
}