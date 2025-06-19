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
    #[serde(default)]
    result: Vec<ResultResponse>
}

#[derive(Debug, Default, Deserialize)]
struct ResultResponse {
    #[serde(default)]
    stock_currency: String,
    #[serde(default)]
    last_price: String,
    #[serde(default)]
    funding_rate: String,
    #[serde(default)]
    next_funding_rate_timestamp: String
}

#[derive(Debug, Clone)]
pub struct FoundFundingWhiteBit {
    pub exchange_name: String,
    pub symbol: String,
    pub mark_price: String,
    pub last_funding_rate: String,
    pub next_funding_time: String,
    pub unix_time:  i64
}

impl FundingParameter for FoundFundingWhiteBit {
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

pub async fn search_funding_whitebit() -> Result<Vec<FoundFundingWhiteBit>, Error> {

    let (tx_funding, rx_funding) = mpsc::channel::<(String, String, String, String)>(100);
    let (tx_data, rx_data) = mpsc::channel::<(String, f64, String, String, i64)>(100);
    let (tx_results, mut rx_results) = mpsc::channel::<FoundFundingWhiteBit>(100);

    tokio::spawn(process_funding(rx_funding, tx_data));
    tokio::spawn(process_data(rx_data, tx_results));

    let url = "https://whitebit.com/api/v4/public/futures";

    let client = reqwest::Client::new();
    let response = client.get(url)
        .send()
        .await?;
    let response_json: ApiResponse = response.json().await?;

    let limit_requests = ApiLimits::WhiteBit.limit();
    let semaphore = Arc::new(Semaphore::new(limit_requests));
    let seen_symbols = Arc::new(DashSet::new());
    let mut futures_vec = Vec::new();

    for data in response_json.result {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let seen_symbols = Arc::clone(&seen_symbols);

        let tx_funding = tx_funding.clone();
        let symbol = data.stock_currency.clone();

        let fut = tokio::spawn(async move {
            if seen_symbols.insert(symbol.clone()) {
                drop(seen_symbols);
                tx_funding.send((
                    symbol, 
                    data.funding_rate,
                    data.last_price,
                    data.next_funding_rate_timestamp
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

    Ok(rr)
}   

async fn process_funding(
    mut rx_funding: Receiver<(String, String, String, String)>,
    tx_data: Sender<(String, f64, String, String, i64)>
) {
    while let Some((symbol, funding_rate, mark_price, nft)) = rx_funding.recv().await {
        let funding_rate = funding_rate.parse::<f64>().unwrap_or(0.0) * 100.00;
        if funding_rate.abs() >= THRESHOLD_RATE_FUNDING {
            let nft = nft.parse::<i64>().unwrap_or(0);
            let next_funding_datetime = convert_to_berlin_time(nft); 

            tx_data.send((
                symbol,
                funding_rate,
                mark_price,
                next_funding_datetime,
                nft
            )).await.ok();
        }
    }
}

async fn process_data(
    mut rx_data: Receiver<(String, f64, String, String, i64)>,
    tx_results: Sender<FoundFundingWhiteBit>
) {
    while let Some((symbol, funding_rate, mark_price, nft, unix_time)) = rx_data.recv().await {
        let found_funding = FoundFundingWhiteBit {
            exchange_name: "WhiteBit".to_string(),
            symbol: symbol + "USDT",
            mark_price: mark_price,
            last_funding_rate: format!("{:.4}", funding_rate),
            next_funding_time: nft,
            unix_time: unix_time
        };

        tx_results.send(found_funding).await.ok();
    }
}