use std::sync::Arc;
use std::time::Duration;

use dashmap::DashSet;
use reqwest::Error;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::Semaphore;
use tokio::time::timeout;

use crate::THRESHOLD_RATE_FUNDING;
use crate::convert_to_berlin_time;
use crate::FundingParameter;
use crate::api_limits::ApiLimits;

#[derive(Debug, Deserialize, Default)]
struct ApiResponse {
    #[serde(default)]
    result: Option<ApiResult>
}

#[derive(Debug, Deserialize, Default)]
struct ApiResult {
    list: Option<Vec<SymbolData>>
}

#[derive(Debug, Deserialize, Default)]
struct SymbolData {
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    #[serde(rename="markPrice")]
    mark_price: String,
    #[serde(default)]
    #[serde(rename="fundingRate")]
    funding_rate: String,
    #[serde(default)]
    #[serde(rename="nextFundingTime")]
    next_funding_time: String
}

#[derive(Debug, Clone)]
pub struct FoundFundingBybit {
    pub exchange_name: String,
    pub symbol: String,
    pub mark_price: String,
    pub last_funding_rate: String,
    pub next_funding_time: String,
    pub unix_time: i64
}

impl FundingParameter for FoundFundingBybit {
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

pub async fn search_funding_bybit() -> Result<Vec<FoundFundingBybit>, Error> {
    let (tx_funding, rx_funding) = mpsc::channel::<(String, String, String, String)>(100);
    let (tx_data, rx_data) = mpsc::channel::<(String, f64, String, String, i64)>(100);
    let (tx_results, mut rx_results) = mpsc::channel::<FoundFundingBybit>(200);

    tokio::spawn(process_funding(rx_funding, tx_data));
    tokio::spawn(process_data(rx_data, tx_results));

    let url = "https://api.bybit.com/v5/market/tickers?category=linear";

    let client = reqwest::Client::new();
    let response = client.get(url).send().await?;
    let json_data: ApiResponse = response.json().await?;

    let limit_requests = ApiLimits::Bybit.limit();
    let semaphore = Arc::new(Semaphore::new(limit_requests));
    
    let seen_symbols = Arc::new(DashSet::new());
    let mut futures_vec = Vec::new();

    if let Some(result) = json_data.result {
        if let Some(data) = result.list {
            for symbol in data {
                let tx_funding = tx_funding.clone();
                let seen_symbols = Arc::clone(&seen_symbols);
                let permit: tokio::sync::OwnedSemaphorePermit = semaphore.clone().acquire_owned().await.unwrap();

                let fut = tokio::spawn(async move {
                    if seen_symbols.insert(symbol.symbol.clone()) {
                        drop(seen_symbols);
                        tx_funding.send((
                            symbol.symbol,
                            symbol.funding_rate,
                            symbol.mark_price,
                            symbol.next_funding_time
                        )).await.ok();
                    }

                    drop(permit);
                });

                futures_vec.push(fut);
            }
        }
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
    mut rx_funding: Receiver<(String, String, String, String)>,
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
            let nft = nft.parse::<i64>().unwrap_or(0);
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

async fn process_data(
    mut rx_data: Receiver<(String, f64, String, String, i64)>,
    tx_results: Sender<FoundFundingBybit>
) {
    while let Some((
        symbol,
        fr,
        mp,
        nfdt,
        nft
    )) = rx_data.recv().await {
        let found_funding = FoundFundingBybit {
            exchange_name: "Bybit".to_string(),
            symbol,
            mark_price: mp,
            last_funding_rate: format!("{:.4}", fr),
            next_funding_time: nfdt,
            unix_time: nft
        };

        tx_results.send(found_funding).await.ok();
    }
}