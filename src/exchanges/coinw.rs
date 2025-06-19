use async_tungstenite::WebSocketStream;

use futures::{AsyncRead, AsyncWrite, StreamExt};
use reqwest::{Error};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::timeout;
use std::collections::HashMap;
use std::time::Duration;
use std::{collections::HashSet};
use tokio::sync::{mpsc, Mutex, Semaphore};
use std::sync::Arc;
use futures::SinkExt;
use futures::stream::{SplitSink, SplitStream};
use url::Url;

use async_tungstenite::tokio::{connect_async};
use async_tungstenite::tungstenite::Message;

use crate::THRESHOLD_RATE_FUNDING;
use crate::convert_to_berlin_time;
use crate::FundingParameter;
use crate::api_limits::ApiLimits;

#[derive(Debug, Default, Deserialize)]
struct ApiResponse {
    #[serde(default)]
    data: Vec<DataResponse>
}

#[derive(Debug, Default, Deserialize)]
struct DataResponse {
    #[serde(default)]
    base: String,
    #[serde(default)]
    _base_coin: String,
    #[serde(default, rename="settledAt")]
    next_funding_time: i64,
    #[serde(default, rename="last_price")]
    mark_price: f64
}

#[derive(Debug, Clone)]
pub struct FoundFundingCoinW {
    pub exchange_name: String,
    pub symbol: String,
    pub mark_price: String,
    pub last_funding_rate: String,
    pub next_funding_time: String,
    pub unix_time: i64
}

impl FundingParameter for FoundFundingCoinW {
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

pub async fn search_funding_coinw() -> Result<Vec<FoundFundingCoinW>, Error> {
    let websocket_url = Url::parse("wss://ws.futurescw.com/perpum").unwrap();
    let (ws_stream, _) = connect_async(websocket_url).await.expect("Не удалось подключиться к Websocket CoinW");
    let (ws_sink, ws_stream) = ws_stream.split();

    // Каналы
    let (tx_funding, rx_funding) = mpsc::channel::<(String, i64)>(100);
    let (tx_next_data, rx_next_data) = mpsc::channel::<(String, f64)>(100);
    let (tx_next_funding_time, rx_next_funding_time) = mpsc::channel::<(String, i64)>(100);
    let (tx_result, mut rx_result) = mpsc::channel::<FoundFundingCoinW>(100);

    let _handler_funding_sub = tokio::spawn(send_sub_msg(rx_funding, ws_sink, tx_next_funding_time));
    let _handler_funding = tokio::spawn(get_funding_rate(ws_stream, tx_next_data));
    let _handler_next_data = tokio::spawn(get_next_data(rx_next_data, rx_next_funding_time, tx_result));
    
    let url = "https://api.coinw.com/v1/perpum/instruments";
    let client = reqwest::Client::new();
    let response = client.get(url)
        .send()
        .await?;
    let response_json: ApiResponse = response.json().await?;

    let limit_requests = ApiLimits::CoinW.limit();
    let semaphore = Arc::new(Semaphore::new(limit_requests));
    let seen_symbols = Arc::new(Mutex::new(HashSet::new()));
    let mut futures_vec = Vec::new();

    for data in response_json.data {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let seen_symbols = Arc::clone(&seen_symbols);
        let symbol = data.base.clone();
        let next_funding_time = data.next_funding_time;

        let tx_funding = tx_funding.clone();

        let fut = tokio::spawn(async move {
            let mut seen = seen_symbols.lock().await;
            if seen.insert(symbol.clone()) {
                drop(seen);
                tx_funding.send((symbol.clone(), next_funding_time)).await.ok();
            }
            drop(permit);
        });

        futures_vec.push(fut);
    }

    let _results = futures::future::join_all(futures_vec).await;
    let mut rr = Vec::new();

    loop {
        match timeout(Duration::from_secs(2), rx_result.recv()).await {
            Ok(Some(item)) => {
                rr.push(item);
            }
            _ => {
                break;
            }
        }
    }

    // println!("{:?}", rr);

    tokio::time::sleep(Duration::from_secs(5)).await;

    Ok(rr)
}

async fn get_next_data(
    mut rx_funding_rate: Receiver<(String, f64)>,
    mut rx_next_funding_time: Receiver<(String, i64)>,
    tx_result: Sender<FoundFundingCoinW>
) {
    let mut funding_map: HashMap<String, f64> = HashMap::new();
    let mut funding_time_map: HashMap<String, i64> = HashMap::new();
    let mut last_request_map: HashMap<String, (f64, i64)> = HashMap::new();

    let url = "https://api.coinw.com/v1/perpumPublic/ticker".to_string();
    let client = reqwest::Client::new();

    loop {
        tokio::select! {
            res = rx_funding_rate.recv() => {
                if let Some((symbol, rate)) = res {
                    funding_map.insert(symbol.clone(), rate);

                    if let Some(&time) = funding_time_map.get(&symbol) {
                        let last_req = last_request_map.get(&symbol);
                        if last_req != Some(&(rate, time)) {
                            last_request_map.insert(symbol.clone(), (rate, time));

                            let client = client.clone();
                            let url = url.clone();
                            let symbol = symbol.clone();
                            let tx_result = tx_result.clone();

                            tokio::spawn(async move {
                                let response = client.get(url)
                                .query(&[("instrument", symbol.clone())])
                                .send()
                                .await.unwrap();

                                let response_json: ApiResponse = response.json().await.unwrap();

                                let next_funding_datetime = convert_to_berlin_time(time);

                                if !response_json.data.is_empty() {
                                    for data in response_json.data {
                                        let found_response = FoundFundingCoinW { 
                                            exchange_name: "CoinW".to_string(), 
                                            symbol: symbol.to_uppercase() + "USDT", 
                                            mark_price: data.mark_price.to_string(), 
                                            last_funding_rate: format!("{:.4}", rate), 
                                            next_funding_time: next_funding_datetime.clone(), 
                                            unix_time: time 
                                        };

                                        let _ = tx_result.send(found_response.clone()).await.ok();
                                    }
                                }    
                            });

                        }
                    }
                } else {
                    // канал закрыт — выходим из внешнего цикла
                    break;
                }
            }
            res = rx_next_funding_time.recv() => {
                if let Some((symbol, time)) = res {
                    funding_time_map.insert(symbol.clone(), time);

                    if let Some(&rate) = funding_map.get(&symbol) {
                        let last_req = last_request_map.get(&symbol);
                        if last_req != Some(&(rate, time)) {
                            last_request_map.insert(symbol.clone(), (rate, time));

                            let client = client.clone();
                            let url = url.clone();
                            let symbol = symbol.clone();
                            let tx_result = tx_result.clone();

                            tokio::spawn(async move {
                                let response = client.get(url)
                                .query(&[("instrument", symbol.clone())])
                                .send()
                                .await.unwrap();

                                let response_json: ApiResponse = response.json().await.unwrap();

                                let next_funding_datetime = convert_to_berlin_time(time);

                                if !response_json.data.is_empty() {
                                    for data in response_json.data {
                                        let found_response = FoundFundingCoinW { 
                                            exchange_name: "CoinW".to_string(), 
                                            symbol: symbol.to_uppercase() + "USDT", 
                                            mark_price: data.mark_price.to_string(), 
                                            last_funding_rate: format!("{:.4}", rate), 
                                            next_funding_time: next_funding_datetime.clone(), 
                                            unix_time: time 
                                        };

                                        let _ = tx_result.send(found_response.clone()).await.ok();

                                    }
                                }    
                            });
                        }   
                    }
                } else {
                    // канал закрыт — выходим из внешнего цикла
                    break;
                }
            }
        }
    }
}

async fn get_funding_rate<S>(
    mut ws_stream: SplitStream<WebSocketStream<S>>,
    tx_next_data: Sender<(String, f64)>
)
where 
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static
{
    while let Some(Ok(msg)) = ws_stream.next().await {
        if let Message::Text(raw) = msg {
            let parsed: Value = serde_json::from_str(&raw).unwrap();
            let symbol = parsed["pairCode"].as_str();
            let funding_rate = parsed["data"]["r"].as_f64();

            if let (Some(s), Some(mut r)) = (symbol, funding_rate) {
                r *= 100.00;
                if r.abs() >= THRESHOLD_RATE_FUNDING {
                    tx_next_data.send((s.to_string().clone(), r)).await.ok();
                }
            }
        }
    }
}

async fn send_sub_msg<S>(
    mut rx: Receiver<(String, i64)>,
    mut ws_sink: SplitSink<WebSocketStream<S>, Message>,
    tx_next_funding_time: Sender<(String, i64)>
)
where 
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static
{
    while let Some((symbol, next_funding_time)) = rx.recv().await {
        let subscribe_msg = serde_json::json!({
            "event": "sub",
            "params": {
                "biz": "futures",
                "pairCode": symbol,
                "type": "funding_rate"
            }
        });

        tx_next_funding_time.send((symbol.clone(), next_funding_time)).await.unwrap();

        if ws_sink.send(Message::Text(subscribe_msg.to_string())).await.is_err() {
            eprintln!("Ошибка при отправке подписки")
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
