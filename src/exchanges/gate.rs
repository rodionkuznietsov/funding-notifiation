use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::format::Item;
use dashmap::DashSet;
use futures::SinkExt;
use reqwest::Error;
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

#[derive(Debug, Deserialize, Default)]
struct ApiResponse {
    #[serde(default)]
    name: String,
    #[serde(default)]
    funding_rate: String,
    #[serde(default)]
    funding_next_apply: i64,
    #[serde[default]]
    mark_price: String
}

#[derive(Debug, Clone)]
pub struct FoundFundingGate {
    pub exchange_name: String,
    pub symbol: String,
    pub mark_price: String,
    pub last_funding_rate: String,
    pub next_funding_time: String,
    pub unix_time: i64
}

impl FundingParameter for FoundFundingGate {
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

pub async fn search_funding_gate() -> Result<Vec<FoundFundingGate>, Error> {
    let (tx_funding, rx_funding) = mpsc::channel::<(String, String, String, i64)>(200);
    let (tx_data, rx_data) = mpsc::channel::<(String, f64, String, String, i64)>(200);
    let (tx_results, mut rx_results) = mpsc::channel::<FoundFundingGate>(200);

    tokio::spawn(process_funding(rx_funding, tx_data));
    tokio::spawn(process_data(rx_data, tx_results));

    let url = "https://api.gateio.ws/api/v4/futures/usdt/contracts/";
    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .send()
        .await?;

    let json_datas: Vec<ApiResponse> = response.json().await?;

    let limit_requests = ApiLimits::Gate.limit();
    let semaphore = Arc::new(Semaphore::new(limit_requests));

    let seen_symbols = Arc::new(DashSet::new());
    let mut futures_vec = Vec::new();

    for json_data in json_datas {
        let seen_symbols = Arc::clone(&seen_symbols);
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let tx_funding = tx_funding.clone();

        let fut = tokio::spawn(async move {
            if seen_symbols.insert(json_data.name.clone()) {
                drop(seen_symbols);
                tx_funding.send((
                    json_data.name,
                    json_data.funding_rate,
                    json_data.mark_price,
                    json_data.funding_next_apply
                )).await.ok();
            }
            drop(permit);
        });

        futures_vec.push(fut);
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

    drop(tx_funding);

    Ok(rr)
}

async fn process_funding(
    mut rx_funding: Receiver<(String, String, String, i64)>,
    tx_data: Sender<(String, f64, String, String, i64)>
) {
    while let Some((
        symbol,
        funding_rate,
        mark_price,
        next_funding_time
    )) = rx_funding.recv().await {
        let funding_rate = funding_rate.parse::<f64>().unwrap_or(0.0) * 100.00;

        if funding_rate.abs() >= THRESHOLD_RATE_FUNDING {
            let nfdt = convert_to_berlin_time(next_funding_time);
            tx_data.send((
                symbol,
                funding_rate,
                mark_price,
                nfdt,
                next_funding_time
            )).await.ok();
        }
    }

    drop(tx_data);
}

async fn process_data(
    mut rx_data: Receiver<(String, f64, String, String, i64)>,
    tx_results: Sender<FoundFundingGate>
) {
    while let Some((
        symbol,
        fr,
        mp,
        nfdt,
        nft
    )) = rx_data.recv().await {
        let found_funding = FoundFundingGate {
            exchange_name: "Gate".to_string(),
            symbol: symbol.replace("_", "").to_string(),
            mark_price: mp,
            last_funding_rate: format!("{:.4}", fr),
            next_funding_time: nfdt,
            unix_time: nft
        };

        tx_results.send(found_funding).await.ok();
    }
}