use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;

use crate::THRESHOLD_RATE_FUNDING;
use crate::convert_to_berlin_time;
use crate::FundingParameter;
use crate::api_limits::ApiLimits;

#[derive(Debug, Deserialize, Default)]
struct ApiResponse { 
    #[serde(default)]
    data: Vec<DataResponse>
}

#[derive(Debug, Deserialize, Default)]
struct DataResponse {
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    #[serde(default)]
    #[serde(rename = "markPrice")]
    mark_price: String,
    #[serde(default)]
    #[serde(rename = "nextFundingTime")]
    next_funding_time: String
}

#[derive(Debug, Clone)]
pub struct FoundFundingBitget {
    pub exchange_name: String,
    pub symbol: String,
    pub mark_price: String,
    pub last_funding_rate: String,
    pub next_funding_time: String,
    pub unix_time: i64
}

impl FundingParameter for FoundFundingBitget {
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

pub async fn search_funding_bitget() -> Result<Vec<FoundFundingBitget>, reqwest::Error> {
    let url = "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES";

    let client = reqwest::Client::new();
    let response = client.get(url)
        .send()
        .await?;

    let response_json: ApiResponse = response.json().await?;
    let seen_symbols = Arc::new(Mutex::new(HashSet::new()));
    let mut futures_vec = Vec::new();

    let limit_requests = ApiLimits::Bitget.limit();

    let semaphore = Arc::new(Semaphore::new(limit_requests));

    for json_data in response_json.data {
        let seen_symbols = Arc::clone(&seen_symbols);
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let client = client.clone();

        let fut = tokio::spawn(async move {
            let funding_rate = json_data.funding_rate.parse::<f64>().unwrap_or(0.0) * 100.0;

            // Проверка на уникальность
            {
                let mut seen = seen_symbols.lock().await;
                if !seen.insert(json_data.symbol.clone()) {
                    return None;
                }
            }

            if funding_rate.abs() >= THRESHOLD_RATE_FUNDING {
                next_funding_time_search(json_data.symbol, json_data.mark_price, funding_rate, permit, &client).await
            } else {
                None
            }
        });

        futures_vec.push(fut);
    }

    let results = futures::future::join_all(futures_vec).await;
    let mut rr = Vec::new();

    rr.extend(
        results.into_iter().filter_map(|outer1| outer1.ok()?)
    );

    Ok(rr)
}

async fn next_funding_time_search(
    symbol_name: String, 
    mark_price: String, 
    funding_rate: f64, 
    permit: tokio::sync::OwnedSemaphorePermit,
    client: &reqwest::Client
) -> Option<FoundFundingBitget> {

    let _permit = permit;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let url = "https://api.bitget.com/api/v2/mix/market/funding-time";

    let response = client.get(url)
        .query(&[("symbol", &symbol_name), ("productType", &"usdt-futures".to_string())])
        .send()
        .await;

    match response {
        Ok(res) => {
            let response_json: ApiResponse = res.json().await.ok()?;

            if let Some(json_data) = response_json.data.into_iter().next() {
                let next_funding_time_parse = json_data.next_funding_time.parse::<i64>().unwrap_or(0);
                let next_funding_time = convert_to_berlin_time(next_funding_time_parse);

                let found_funding: FoundFundingBitget = FoundFundingBitget {
                    symbol: symbol_name.to_string(),
                    mark_price: mark_price.to_string(),
                    exchange_name: "Bidget".to_string(),
                    last_funding_rate: format!("{:.4}", funding_rate),
                    next_funding_time: next_funding_time,
                    unix_time: next_funding_time_parse
                };

                // println!("{:?}", found_funding); 

                Some(found_funding)
            } else {
                None
            }
        },
        Err(_) => {
            return None;
        }
    }
}