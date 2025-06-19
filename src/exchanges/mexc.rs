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

#[derive(Debug, Deserialize, Default)]
struct SymbolResponse {
    #[serde(default)]
    data: Vec<DataResponse>
}

#[derive(Debug, Deserialize, Default)]
struct FundingResponse {
    #[serde(default)]
    data: DataResponse
}

#[derive(Debug, Deserialize, Default)]
struct DataResponse {
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    #[serde(rename="lastPrice")]
    mark_price: f64,
    #[serde(default)]
    #[serde(rename = "fundingRate")]
    funding_rate: f64,
    #[serde(default)]
    #[serde(rename="nextSettleTime")]
    next_settle_time: i64
}

#[derive(Debug)]
struct FoundFundingSymbols {
    symbol: String,
    mark_price: f64
}

#[derive(Debug, Clone)]
pub struct FoundFundingMexc {
    pub exchange_name: String,
    pub symbol: String,
    pub mark_price: String,
    pub last_funding_rate: String,
    pub next_funding_time: String,
    pub unix_time: i64
}

impl FundingParameter for FoundFundingMexc {
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

pub async fn search_funding_mexc() -> Result<Vec<FoundFundingMexc>, Error> {
    let url = "https://contract.mexc.com/api/v1/contract/ticker";

    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .send()
        .await?;

    let response_json: SymbolResponse = response.json().await?;

    let limit_requests = ApiLimits::Mexc.limit();
    let semaphore = Arc::new(Semaphore::new(limit_requests));

    let seen_symbols = Arc::new(Mutex::new(HashSet::new()));
    let mut futures_vec = Vec::new();

    for data in response_json.data {
        let seen_symbols = Arc::clone(&seen_symbols);
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let client = client.clone();

        let fut = tokio::spawn(async move {
            let _permit = permit;
            tokio::time::sleep(Duration::from_millis(1)).await;
            
            let funding_rate = data.funding_rate * 100.00;
            let symbol = data.symbol.clone();  

            // Проверка на уникальность токенов
            let mut seen = seen_symbols.lock().await;
            if !seen.insert(symbol.clone()) {
                return None;
            }

            drop(seen); // Освобождаем seen

            if funding_rate.abs() < THRESHOLD_RATE_FUNDING {
                return None;
            }

            let url = format!("https://contract.mexc.com/api/v1/contract/funding_rate/{}", symbol);

            // println!("{:?}", url);
            let response = client.get(url)
                .send()
                .await;

            match response {
                Ok(resp) => {
                    let response_json: FundingResponse = resp.json().await.ok()?;
                    let next_funding_time = response_json.data.next_settle_time;
                    let next_funding_datetime = convert_to_berlin_time(next_funding_time);

                    let found_funding = FoundFundingMexc {
                        exchange_name: "Mexc".to_string(),
                        symbol: symbol.replace("_", ""),
                        mark_price: data.mark_price.to_string(),
                        last_funding_rate: format!("{:.4}", funding_rate),
                        next_funding_time: next_funding_datetime,
                        unix_time: next_funding_time,
                    };

                    Some(found_funding)
                },
                Err(e) => {
                    println!("[MEXC-ERROR]: {}", e);
                    None
                }
            }
        });

        futures_vec.push(fut);
    }

    let results = futures::future::join_all(futures_vec).await;

    let rr: Vec<FoundFundingMexc> = results
        .into_iter()
        .filter_map(|outer| outer.ok().flatten())   // получаем Vec<FoundFundingMexc>
        .collect();                                                                              // собираем в один вектор

    Ok(rr)
    
}
