use std::{collections::{HashMap, HashSet}, sync::Arc, time::Duration};

use async_tungstenite::{tokio::connect_async, tungstenite::Message, WebSocketStream};
use dashmap::DashMap;
use futures::{stream::{SplitSink, SplitStream}, AsyncRead, AsyncWrite, SinkExt, StreamExt};
use reqwest::Error;
use serde::Deserialize;
use serde_json::Value;
use tokio::{sync::{mpsc::{self, Receiver, Sender}, Mutex, Semaphore}, time::timeout};
use url::Url;

use crate::{api_limits::ApiLimits, time::convert_to_berlin_time};
use crate::THRESHOLD_RATE_FUNDING;
use crate::FundingParameter;
use crate::convert_datetime_str_to_unix;

#[derive(Debug, Default, Deserialize)]
struct ApiResponse {
    #[serde(default)]
    data: Vec<DataResponse>
}

#[derive(Debug, Default, Deserialize)]
struct WApiResponse {
    data: DataResponse
}

#[derive(Debug, Default, Deserialize)]
struct DataResponse {
    #[serde(default)]
    symbol: String,
    #[serde(default, rename="markPrice")]
    mark_price: String,
    #[serde(default, rename="fundingRatePredict")]
    funding_rate_predict: String,
    #[serde(default, rename="fundingAt")]
    funding_at: String
}

#[derive(Debug, Clone)]
pub struct FoundFundingBitunix {
    pub exchange_name: String,
    pub symbol: String,
    pub mark_price: String,
    pub last_funding_rate: String,
    pub next_funding_time: String,
    pub unix_time:  i64
}

impl FundingParameter for FoundFundingBitunix {
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

pub async fn search_funding_bitunix() -> Result<Vec<FoundFundingBitunix>, Error> {
    let websocket_url = Url::parse("wss://fapi.bitunix.com/public/").unwrap();
    let (ws_stream, _) = connect_async(websocket_url).await.expect("Не удалось подключится к Websocket Bitunix");
    let (ws_sink, ws_stream) = ws_stream.split();

    // Каналы
    let (tx_symbol, rx_symbol) = mpsc::channel::<String>(200);
    let (tx_data, rx_data) = mpsc::channel::<WApiResponse>(200);
    let (tx_results, mut rx_resuts) = mpsc::channel::<FoundFundingBitunix>(200);

    let _send_sub_msg_to_wss = tokio::spawn(send_sub_msg(rx_symbol, ws_sink));
    let _get_data_from_wss = tokio::spawn(get_data(ws_stream, tx_data));
    let _process_data= tokio::spawn(process_data(rx_data, tx_results));

    let symbol_url = "https://fapi.bitunix.com/api/v1/futures/market/tickers";
    let client = reqwest::Client::new();
    let response = client.get(symbol_url)
        .send()
        .await?;
    let response_json: ApiResponse = response.json().await?;
    
    let limit_requests = ApiLimits::Bitunix.limit();
    let semaphore = Arc::new(Semaphore::new(limit_requests));
    let seen_symbols = Arc::new(DashMap::new());
    let mut futures_vec = Vec::new();

    for data in response_json.data {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let seen = Arc::clone(&seen_symbols);
        let symbol = data.symbol.clone();

        let tx_symbol = tx_symbol.clone();

        let fut = tokio::spawn(async move {
            if seen.insert(symbol.clone(), 0).is_none() {
                drop(seen);
                tx_symbol.send(symbol.clone()).await.ok();
            }
            drop(permit);
        });

        futures_vec.push(fut);
    }

    let _results = futures::future::join_all(futures_vec).await;
    let mut rr = Vec::new();

    loop {
        match timeout(Duration::from_secs(2), rx_resuts.recv()).await {
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

async fn get_data<S>(
    mut ws_stream: SplitStream<WebSocketStream<S>>,
    tx_data: Sender<WApiResponse>
)
where 
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static
{   
    while let Some(Ok(msg)) = ws_stream.next().await {
        let tx_data= tx_data.clone();
        if let Message::Text(raw) = msg {

            // let parsed: Value = serde_json::from_str(&raw).unwrap();

            // println!("{:?}", parsed);

            let parsed: WApiResponse = serde_json::from_str(&raw).unwrap();
            if !parsed.data.symbol.is_empty() {
                // println!("{:?}", parsed);

                tx_data.send(parsed).await.ok();
            }
        }
    }
}

async fn process_data(
    mut rx: Receiver<WApiResponse>,
    tx_results: Sender<FoundFundingBitunix>
) {
    while let Some(data) = rx.recv().await {
        if !data.data.funding_rate_predict.is_empty() {
            let funding_rate = data.data.funding_rate_predict.parse::<f64>().unwrap_or(0.0);
            if funding_rate.abs() >= THRESHOLD_RATE_FUNDING {
                let funding_at = convert_datetime_str_to_unix(data.data.funding_at);
                let next_funding_datetime = convert_to_berlin_time(funding_at);

                let found_funding = FoundFundingBitunix {
                    exchange_name: "Bitunix".to_string(),
                    symbol: data.data.symbol,
                    mark_price: data.data.mark_price,
                    last_funding_rate: format!("{:.4}", funding_rate.to_string()),
                    next_funding_time: next_funding_datetime,
                    unix_time: funding_at
                };

                // println!("{:?}", found_funding)

                tx_results.send(found_funding).await.ok();
            }
        }
    }
}

async fn send_sub_msg<S>(
    mut rx: Receiver<String>,
    ws_sink: SplitSink<WebSocketStream<S>, Message>
) where 
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static
{
    let semaphore = Arc::new(Semaphore::new(50));
    let ws_sink = Arc::new(Mutex::new(ws_sink));

    while let Some(symbol) = rx.recv().await {
        let permit =semaphore.clone().acquire_owned().await.unwrap();
        let ws_sink = ws_sink.clone();

        tokio::spawn(async move {
            let subscribe_msg = serde_json::json!({
                "op": "subscribe",
                "args": [
                    {
                        "symbol": symbol,
                        "ch": "price"
                    }
                ]
            });
            let mut sink = ws_sink.lock().await;
            if sink.send(Message::Text(subscribe_msg.to_string())).await.is_err() {
                eprintln!("Ошибка при отправке подписки")
            }

            tokio::time::sleep(Duration::from_millis(1)).await;
            drop(permit);
        });
    }
}