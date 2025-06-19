use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use reqwest::Error;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use serde::Deserialize;
use tokio::time::timeout;

use crate::THRESHOLD_RATE_FUNDING;
use crate::convert_to_berlin_time;
use crate::FundingParameter;
use crate::api_limits::ApiLimits;

#[derive(Debug, Default, Deserialize)]
struct ApiResponse {
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    #[serde(rename = "markPrice")]
    mark_price: String,
    #[serde(default)]
    #[serde(rename = "lastFundingRate")]
    last_funding_rate: String,
    #[serde(default)]
    #[serde(rename = "nextFundingTime")]
    next_funding_time: i64
}

#[derive(Debug, Clone)]
pub struct FoundFundingBinance {
    pub exchange_name: String,
    pub symbol: String,
    pub mark_price: String,
    pub last_funding_rate: String,
    pub next_funding_time: String,
    pub unix_time:  i64
}

impl FundingParameter for FoundFundingBinance {
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

pub async fn search_funding_binance() -> Result<Vec<FoundFundingBinance>, Error> {

    let (tx_process, rx_process) = mpsc::channel::<(String, String, String, i64)>(200);
    let (tx_found_funding, rx_found_funding) = mpsc::channel::<(String, String, String, i64)>(200);
    let (tx_results, mut rx_results) = mpsc::channel::<FoundFundingBinance>(200);
    
    let _process_data = tokio::spawn(proccess_data(rx_process, tx_found_funding));
    let _get_results = tokio::spawn(_found_funding(rx_found_funding, tx_results));

    let url = "https://fapi.binance.com/fapi/v1/premiumIndex";

    let client = reqwest::Client::new();
    let response = client.get(url)
        .send()
        .await?;
    let response_json: Vec<ApiResponse> = response.json().await?;

    let limit_requests = ApiLimits::Binance.limit();
    let semaphore = Arc::new(Semaphore::new(limit_requests));
    let seen_symbols = Arc::new(DashMap::new());
    let mut futures_vec = Vec::new();

    for data in response_json {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let seen = Arc::clone(&seen_symbols);
        let tx_process = tx_process.clone();

        let fut = tokio::spawn(async move {
            // Проверка на уникальность токенов
            {
                if seen.insert(data.symbol.clone(), 0).is_none() {
                    drop(seen);
                    tx_process.send((
                        data.symbol.clone(),
                        data.mark_price.clone(),
                        data.last_funding_rate.clone(),
                        data.next_funding_time
                    )).await.ok();
                }

                drop(permit)
            }
        });

        futures_vec.push(fut);
    }

    let results = futures::future::join_all(futures_vec).await;
    let mut rr = Vec::new();

    loop {
        match timeout(Duration::from_millis(1), rx_results.recv()).await {
            Ok(Some(item)) => {
                rr.push(item);
            }
            _ => {
                break;
            }
        }
    }

    // println!("{:?}", rr);

    Ok(rr)

}

async fn proccess_data(
    mut rx: Receiver<(String, String, String, i64)>,
    tx_found_funding: Sender<(String, String, String, i64)>
) {
    while let Some((
        symbol, 
        mark_price, mut funding_rate, 
        next_funding_time
    )) = rx.recv().await {
        let funding_rate = funding_rate.parse::<f64>().unwrap_or(0.00) * 100.00;

        if funding_rate.abs() >= THRESHOLD_RATE_FUNDING {
            tx_found_funding.send((
                symbol,
                mark_price,
                funding_rate.to_string(),
                next_funding_time
            )).await.ok();
        }
    }
}

async fn _found_funding(
    mut rx: Receiver<(String, String, String, i64)>,
    tx_results: Sender<FoundFundingBinance>
) {
    while let Some((
        symbol, 
        mark_price, 
        funding_rate, 
        next_funding_time
    )) = rx.recv().await {
        let next_funding_datetime = convert_to_berlin_time(next_funding_time);

        let found_funding = FoundFundingBinance {
            exchange_name: "Binance".to_string(),
            symbol: symbol,
            mark_price: mark_price,
            next_funding_time: next_funding_datetime,
            last_funding_rate: funding_rate,
            unix_time: next_funding_time
        };

        // println!("{:?}", found_funding);

        tx_results.send(found_funding).await.ok();
    }
}