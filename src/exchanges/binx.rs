use core::sync;
use std::collections::HashSet;
use std::io::Error;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::time::timeout;

use crate::THRESHOLD_RATE_FUNDING;
use crate::convert_to_berlin_time;
use crate::FundingParameter;
use crate::api_limits::ApiLimits;

#[derive(Deserialize, Debug, Default)]
struct ApiResponse {
    #[serde(default)]
    data: Option<Vec<FundingData>>
}

#[derive(Deserialize, Debug, Default)]
struct FundingData {
    symbol: String,
    #[serde(rename = "markPrice")]
    #[serde(default)]
    mark_price: String,
    #[serde(rename = "lastFundingRate")]
    #[serde(default)]
    last_funding_rate: String,
    #[serde(rename = "nextFundingTime")]
    #[serde(default)]
    next_funding_time: i64
}

#[derive(Debug, Clone)]
pub struct FoundFundingBinX {
    pub exchange_name: String,
    pub symbol: String,
    pub mark_price: String,
    pub last_funding_rate: String,
    pub next_funding_time: String,
    pub unix_time:  i64
}

impl FundingParameter for FoundFundingBinX {
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

pub async fn search_funding_binx() -> Result<Vec<FoundFundingBinX>, reqwest::Error>{

    let (tx_funding, rx_funding) = mpsc::channel::<(String, String, String, i64)>(200);
    let (tx_found_funding, rx_found_funding) = mpsc::channel::<(String, String, f64, i64)>(200);
    let (tx_results, mut rx_results) = mpsc::channel::<FoundFundingBinX>(200);

    let _process_data = tokio::spawn(process_data(rx_funding, tx_found_funding));
    let _found_funding = tokio::spawn(found_funding(rx_found_funding, tx_results));

    let url = "https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex";

    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .send()
        .await?;

    let json_datas: ApiResponse = response.json().await?;

    let limit_requests = ApiLimits::BinX.limit();
    let semaphore = Arc::new(Semaphore::new(limit_requests));
    
    let seen_symbols = Arc::new(DashMap::new());
    let mut futures_vec = Vec::new();

    if let Some(json_data) = json_datas.data {
        for data in json_data {
            let seen = Arc::clone(&seen_symbols);
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let tx_funding = tx_funding.clone();

            let fut = tokio::spawn(async move {
                // Проверка на уникальность внутри потока
                if seen.insert(data.symbol.clone(), 0).is_none() {
                    drop(seen);
                    tx_funding.send((
                        data.symbol,
                        data.mark_price,
                        data.last_funding_rate,
                        data.next_funding_time
                    )).await.ok();
                }

                drop(permit);
            });

            futures_vec.push(fut);
        }
    }

    let _results = futures::future::join_all(futures_vec).await;

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

async fn process_data(
    mut rx_funding: Receiver<(String, String, String, i64)>,
    tx_found_funding: Sender<(String, String, f64, i64)>
) {
    while let Some((
        symbol, 
        mark_price, 
        funding_rate, 
        next_funding_time
    )) = rx_funding.recv().await {
        let funding_rate = funding_rate.parse::<f64>().unwrap_or(0.0) * 100.00;

        if funding_rate.abs() >= THRESHOLD_RATE_FUNDING {
            tx_found_funding.send((
                symbol,
                mark_price,
                funding_rate,
                next_funding_time
            )).await.ok();
        }
    }
}

async fn found_funding(
    mut rx_found_funding: Receiver<(String, String, f64, i64)>,
    tx_results: Sender<FoundFundingBinX>
) {
    while let Some((
        symbol,
        mark_price,
        funding_rate,
        next_funding_time
    )) = rx_found_funding.recv().await {
        let next_funding_datetime = convert_to_berlin_time(next_funding_time);

        let found_funding = FoundFundingBinX {
            exchange_name: "BinX".to_string(),
            symbol: symbol,
            mark_price,
            last_funding_rate: format!("{:.4}", funding_rate),
            next_funding_time: next_funding_datetime,
            unix_time: next_funding_time
        };

        tx_results.send(found_funding).await.ok();
    }
}