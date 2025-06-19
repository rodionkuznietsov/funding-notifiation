use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

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

#[derive(Debug, Default, Deserialize)]
struct ApiResponse {
    data: Vec<DataResponse>
}

#[derive(Debug, Default, Deserialize)]
struct DataResponse {
    #[serde(default)]
    symbol: String,
    #[serde(default, rename="markPrice")]
    mark_price: f64,
    #[serde(default, rename="fundingFeeRate")]
    funding_fee_rate: Option<f64>,
    #[serde(default, rename="nextFundingRateDateTime")]
    next_funding_time: Option<i64>
}

#[derive(Debug, Clone)]
pub struct FoundFundingKuCoin {
    pub exchange_name: String,
    pub symbol: String,
    pub mark_price: String,
    pub last_funding_rate: String,
    pub next_funding_time: String,
    pub unix_time: i64
}

impl FundingParameter for FoundFundingKuCoin {
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

pub async fn search_funding_kucoin() -> Result<Vec<FoundFundingKuCoin>, Error> {
    let (tx_funding, rx_funding) = mpsc::channel::<(String, f64, Option<f64>, Option<i64>)>(100);
    let (tx_data, rx_data) = mpsc::channel::<(String, f64, f64, String, i64)>(100);
    let (tx_results, mut rx_results) = mpsc::channel::<FoundFundingKuCoin>(250);

    tokio::spawn(process_funding(rx_funding, tx_data));
    tokio::spawn(process_data(rx_data, tx_results));

    let url = "https://api-futures.kucoin.com/api/v1/contracts/active";
    let client = reqwest::Client::new();
    let response = client.get(url)
        .send()
        .await?;
    let response_json: ApiResponse = response.json().await?;

    let limit_request = ApiLimits::Kucoin.limit();
    let semaphore = Arc::new(Semaphore::new(limit_request));
    let seen_symbols = Arc::new(DashSet::new());
    let mut futures_vec = Vec::new();

    for data in response_json.data {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let seen_symbols = Arc::clone(&seen_symbols);
        let tx_funding = tx_funding.clone();

        let fut = tokio::spawn(async move {
            if seen_symbols.insert(data.symbol.clone()) {
                drop(seen_symbols);
                tx_funding.send((
                    data.symbol.clone(),
                    data.mark_price,
                    data.funding_fee_rate,
                    data.next_funding_time
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
    mut rx_funding: Receiver<(String, f64, Option<f64>, Option<i64>)>,
    tx_data: Sender<(String, f64, f64, String, i64)>
) {
    while let Some((
        symbol, 
        mark_price,
        Some(mut funding_rate),
        Some(nft)
    )) = rx_funding.recv().await {
        funding_rate *= 100.00;

        if funding_rate.abs() >= THRESHOLD_RATE_FUNDING {
            let nfdt = convert_to_berlin_time(nft);
            tx_data.send((
                symbol,
                mark_price,
                funding_rate,
                nfdt,
                nft
            )).await.ok();
        }
    }

    drop(tx_data);
}

async fn process_data( 
    mut rx_data: Receiver<(String, f64, f64, String, i64)>,
    tx_results: Sender<FoundFundingKuCoin>
) {
    while let Some((
        symbol,
        mp,
        fr,
        nfdt,
        nft
    )) = rx_data.recv().await {
        let found_funding = FoundFundingKuCoin {
            exchange_name: "KuCoin".to_string(),
            symbol: symbol.trim_end_matches("M").to_string(),
            mark_price: mp.to_string(),
            last_funding_rate: format!("{:.4}", fr),
            next_funding_time: nfdt,
            unix_time: nft
        };

        // println!("{:?}", found_funding);

        tx_results.send(found_funding).await.ok();
    }
}