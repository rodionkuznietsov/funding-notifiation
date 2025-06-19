use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::format::Item;
use dashmap::DashSet;
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

#[derive(Debug, Deserialize, Default)]
struct ApiResponse {
    #[serde(default)]
    data: DataResponse
}

#[derive(Debug, Deserialize, Default)]
struct DataResponse {
    symbols: Vec<SymbolsResponse>
}

#[derive(Debug, Deserialize, Default)]
struct SymbolsResponse {
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    last_price: String,
    #[serde(default)]
    expected_funding_rate: String,
    #[serde(default)]
    funding_time: i64
}

#[derive(Debug, Clone)]
pub struct FoundFundingBitMart {
    pub exchange_name: String,
    pub symbol: String,
    pub mark_price: String,
    pub last_funding_rate: String,
    pub next_funding_time: String,
    pub unix_time:  i64
}

impl FundingParameter for FoundFundingBitMart {
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

pub async fn search_funding_bitmart() -> Result<Vec<FoundFundingBitMart>, Error> {
    let (tx_funding, rx_funding) = mpsc::channel::<(String, String, String, i64)>(100);
    let (tx_data, rx_data) = mpsc::channel::<(String, f64, String, String, i64)>(100);
    let (tx_results, mut rx_results) = mpsc::channel::<FoundFundingBitMart>(200);

    tokio::spawn(process_funding(rx_funding, tx_data));
    tokio::spawn(proccess_data(rx_data, tx_results));

    let url = "https://api-cloud-v2.bitmart.com/contract/public/details";

    let client = reqwest::Client::new();
    let response = client.get(url)
        .send()
        .await?;
    let response_json: ApiResponse = response.json().await?;

    let limit_requests = ApiLimits::BitMart.limit();
    let semaphore = Arc::new(Semaphore::new(limit_requests));
    let seen_symbols = Arc::new(DashSet::new());
    let mut futures_vec = Vec::new();

    for data in response_json.data.symbols {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let seen_symbols = Arc::clone(&seen_symbols);
        let tx_funding = tx_funding.clone();

        let fut = tokio::spawn(async move {
            if seen_symbols.insert(data.symbol.clone()) {
                drop(seen_symbols);
                tx_funding.send((
                    data.symbol,
                    data.expected_funding_rate,
                    data.last_price,
                    data.funding_time
                )).await.ok();
            }

            drop(permit);
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

    drop(tx_funding);

    Ok(rr)
}

async fn process_funding(
    mut rx_funding: Receiver<(String, String, String, i64)>,
    tx_data: Sender<(String, f64, String, String, i64)>
) {
    while let Some((
        symbol,
        fr,
        mp,
        nft
    )) = rx_funding.recv().await {
        let fr = fr.parse::<f64>().unwrap_or(0.0) * 100.00;

        if fr.abs() >= THRESHOLD_RATE_FUNDING {
            let nfdt = convert_to_berlin_time(nft);
            tx_data.send((
                symbol, 
                fr,
                mp,
                nfdt,
                nft
            )).await.ok();
        }
    }

    drop(tx_data);
}

async fn proccess_data(
    mut rx_data: Receiver<(String, f64, String, String, i64)>,
    tx_results: Sender<FoundFundingBitMart>
) {
    while let Some((
        symbol,
        fr,
        mp,
        nfdt,
        nft
    )) = rx_data.recv().await {
        let found_funding = FoundFundingBitMart {
            exchange_name: "BitMart".to_string(),
            symbol,
            mark_price: mp,
            last_funding_rate: format!("{:.4}", fr),
            next_funding_time: nfdt,
            unix_time: nft
        };

        tx_results.send(found_funding).await.ok();
    }
}