use std::time::Instant;
use std::{fmt::Debug, time::Duration, vec};
use tokio;
use std::collections::HashMap;
use chrono::Utc;

mod exchanges;

mod ton_bot;
mod api_limits;
mod db;
mod time;

use time::{convert_to_normal_time, 
    convert_to_minutes, 
    normalize_timestamp_to_second, 
    convert_to_berlin_time, 
    convert_datetime_str_to_unix
};

use db::{search_symbol_ten_minute, delete_symbol, add_symbol};

use exchanges::binx::{search_funding_binx, FoundFundingBinX};
use exchanges::bitget::{search_funding_bitget, FoundFundingBitget};
use exchanges::bybit::{search_funding_bybit, FoundFundingBybit};
use exchanges::gate::{search_funding_gate, FoundFundingGate};
use exchanges::mexc::{search_funding_mexc, FoundFundingMexc};
use exchanges::whitebit::{search_funding_whitebit, FoundFundingWhiteBit};
use exchanges::bitmart::{search_funding_bitmart, FoundFundingBitMart};
use exchanges::binance::{search_funding_binance, FoundFundingBinance};
use exchanges::coinex::{search_funding_coinex, FoundFundingCoinex};
use exchanges::kucoin::{search_funding_kucoin, FoundFundingKuCoin};
use exchanges::okx::{search_funding_okx, FoundFundingOkx};
use exchanges::htx::{search_funding_htx, FoundFundingHtx};
use exchanges::coinw::{search_funding_coinw, FoundFundingCoinW};
use exchanges::bitunix::{search_funding_bitunix, FoundFundingBitunix};

use ton_bot::{send_message_to_group};

const THRESHOLD_RATE_FUNDING: f64 = 1.0; // в %

#[derive(Debug)]
enum MapType<'a> {
    BinX(&'a HashMap<String, FoundFundingBinX>),
    Bitget(&'a HashMap<String, FoundFundingBitget>),
    Bybit(&'a HashMap<String, FoundFundingBybit>),
    Gate(&'a HashMap<String, FoundFundingGate>),
    Mexc(&'a HashMap<String, FoundFundingMexc>),
    WhiteBit(&'a HashMap<String, FoundFundingWhiteBit>),
    BitMart(&'a HashMap<String, FoundFundingBitMart>),
    Binance(&'a HashMap<String, FoundFundingBinance>),
    Coinex(&'a HashMap<String, FoundFundingCoinex>),
    KuCoin(&'a HashMap<String, FoundFundingKuCoin>),
    Okx(&'a HashMap<String, FoundFundingOkx>),
    Htx(&'a HashMap<String, FoundFundingHtx>),
    CoinW(&'a HashMap<String, FoundFundingCoinW>),
    Bitunix(&'a HashMap<String, FoundFundingBitunix>)
}

trait FundingParameter: std::fmt::Debug + Send + Sync {
    fn symbol(&self) -> &String;
    fn mark_price(&self) -> &String;
    fn last_funding_rate(&self) -> &String;
    fn exchange_name(&self) -> &String;
    fn next_funding_time(&self) -> &String;
    fn unix_time(&self) -> &i64;
}


#[tokio::main]
async fn main() {
    println!("Пары(а) с пороговым фандингом: {}%", THRESHOLD_RATE_FUNDING);
    search_funding_okx().await.unwrap();
    search_funding().await;
}

async fn search_funding() {
    loop {
        let (binx_result, 
            bybit_result, 
            mexc_result, 
            gate_result, 
            bitget_result,
            whitebit_result,
            bitmart_result,
            binance_result,
            coinex_result,
            kucoin_result,
            okx_result,
            htx_result,
            coinw_result,
            bitunix_result
        ) = tokio::join!(
            search_funding_binx(),
            search_funding_bybit(),
            search_funding_mexc(),
            search_funding_gate(),
            search_funding_bitget(),
            search_funding_whitebit(),
            search_funding_bitmart(),
            search_funding_binance(),
            search_funding_coinex(),
            search_funding_kucoin(),
            search_funding_okx(),
            search_funding_htx(),
            search_funding_coinw(),
            search_funding_bitunix()
        );

        if let (
            Ok(binx_data), 
            Ok(bybit_data), 
            Ok(mexc_data), 
            Ok(gate_data),
            Ok(bitget_data),
            Ok(whitebit_data),
            Ok(bitmart_data),
            Ok(binance_data),
            Ok(coinex_data),
            Ok(kucoin_data),
            Ok(okx_data),
            Ok(htx_data),
            Ok(coinw_data),
            Ok(bitunix_data)
        ) = (
            binx_result, 
            bybit_result, 
            mexc_result, 
            gate_result,
            bitget_result,
            whitebit_result,
            bitmart_result,
            binance_result,
            coinex_result,
            kucoin_result,
            okx_result,
            htx_result,
            coinw_result,
            bitunix_result
        ) {
            let binx_map: HashMap<String, FoundFundingBinX> = binx_data.into_iter()
                .map(|item| (item.symbol.clone(), item))
                .collect();
            
            let bybit_map: HashMap<String, FoundFundingBybit> = bybit_data.into_iter()
                .map(|item| (item.symbol.clone(), item))
                .collect();

            let mexc_map: HashMap<String, FoundFundingMexc> = mexc_data.into_iter()
                .map(|item| (item.symbol.clone(), item))
                .collect();

            let gate_map: HashMap<String, FoundFundingGate> = gate_data.into_iter()
                .map(|item| (item.symbol.clone(), item))
                .collect();

            let bitget_map: HashMap<String, FoundFundingBitget> = bitget_data.into_iter()
                .map(|item| (item.symbol.clone(), item))
                .collect();

            let whitebit_map: HashMap<String, FoundFundingWhiteBit> = whitebit_data.into_iter()
                .map(|item| (item.symbol.clone(), item))
                .collect();

            let bitmart_map: HashMap<String, FoundFundingBitMart> = bitmart_data.into_iter()
                .map(|item| (item.symbol.clone(), item))
                .collect();

            let binance_map: HashMap<String, FoundFundingBinance> = binance_data.into_iter() 
                .map(|item| (item.symbol.clone(), item))
                .collect();

            let coinex_map: HashMap<String, FoundFundingCoinex> = coinex_data.into_iter()
                .map(|item| (item.symbol.clone(), item))
                .collect();

            let kucoin_map: HashMap<String, FoundFundingKuCoin> = kucoin_data.into_iter()
                .map(|item| (item.symbol.clone(), item))
                .collect();

            let okx_map: HashMap<String, FoundFundingOkx> = okx_data.into_iter()
                .map(|item| (item.symbol.clone(), item))
                .collect();

            let htx_map: HashMap<String, FoundFundingHtx> = htx_data.into_iter()
                .map(|item| (item.symbol.clone(), item))
                .collect();
            
            let coinw_map: HashMap<String, FoundFundingCoinW> = coinw_data.into_iter()
                .map(|item| (item.symbol.clone(), item))
                .collect();

            let bitunix_map: HashMap<String, FoundFundingBitunix> = bitunix_data.into_iter()
                .map(|item| (item.symbol.clone(), item))
                .collect();

            let maps: Vec<MapType<'_>> = vec![
                MapType::BinX(&binx_map),
                MapType::Bitget(&bitget_map),
                MapType::Bybit(&bybit_map),
                MapType::Gate(&gate_map),
                MapType::Mexc(&mexc_map),
                MapType::WhiteBit(&whitebit_map),
                MapType::BitMart(&bitmart_map),
                MapType::Binance(&binance_map),
                MapType::Coinex(&coinex_map),
                MapType::KuCoin(&kucoin_map),
                MapType::Okx(&okx_map),
                MapType::Htx(&htx_map),
                MapType::CoinW(&coinw_map),
                MapType::Bitunix(&bitunix_map)
            ];

            collect_all_maps(maps).await;

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}

async fn collect_all_maps(maps: Vec<MapType<'_>>) { 
    let mut all: HashMap<String, Vec<Box<dyn FundingParameter>>> = HashMap::new();

    for map in maps {
        match map {
            MapType::BinX(m) => {
                for (symbol, entry) in m {
                    all.entry(symbol.clone())
                        .or_default()
                        .push(Box::new(entry.clone()));
                }
            },
            MapType::Bitget(m) => {
                for (symbol, entry) in m {
                    all.entry(symbol.clone())
                        .or_default()
                        .push(Box::new(entry.clone()));
                }
            },
            MapType::Bybit(m) => {
                for (symbol, entry) in m {
                    all.entry(symbol.clone())
                        .or_default()
                        .push(Box::new(entry.clone()));
                }
            },
            MapType::Gate(m) => {
                for (symbol, entry) in m {
                    all.entry(symbol.clone())
                        .or_default()
                        .push(Box::new(entry.clone()));
                }
            },
            MapType::Mexc(m) => {
                for (symbol, entry) in m {
                    all.entry(symbol.clone())
                        .or_default()
                        .push(Box::new(entry.clone()));
                }
            },
            MapType::WhiteBit(m) => {
                for (symbol, entry) in m {
                    all.entry(symbol.clone())
                        .or_default()
                        .push(Box::new(entry.clone()));
                }
            },
            MapType::BitMart(m) => {
                for (symbol, entry) in m {
                    all.entry(symbol.clone())
                        .or_default()
                        .push(Box::new(entry.clone()));
                }
            },
            MapType::Binance(m) => {
                for (symbol, entry) in m {
                    all.entry(symbol.clone())
                        .or_default()
                        .push(Box::new(entry.clone()));
                }
            },
            MapType::Coinex(m) => {
                for (symbol, entry) in m {
                    all.entry(symbol.clone())
                        .or_default()
                        .push(Box::new(entry.clone()));
                }
            },
            MapType::KuCoin(m) => {
                for (symbol, entry) in m {
                    all.entry(symbol.clone())
                        .or_default()
                        .push(Box::new(entry.clone()));
                }
            },
            MapType::Okx(m) => {
                for (symbol, entry) in m {
                    all.entry(symbol.clone())
                        .or_default()
                        .push(Box::new(entry.clone()));
                }
            },
            MapType::Htx(m) => {
                for (symbol, entry) in m {
                    all.entry(symbol.clone())
                        .or_default()
                        .push(Box::new(entry.clone()));
                }
            },
            MapType::CoinW(m) => {
                for (symbol, entry) in m {
                    all.entry(symbol.clone())
                        .or_default()
                        .push(Box::new(entry.clone()));
                }
            },
            MapType::Bitunix(m) => {
                for (symbol, entry) in m {
                    all.entry(symbol.clone())
                        .or_default()
                        .push(Box::new(entry.clone()));
                }
            }
        }
    }

    let mut min_and_max_funding_by_symbol: HashMap<String, (
        String, f64, String, f64, f64, f64, String, String, i64, i64
    )> = HashMap::new();

    for (symbol, entries) in &all {
        if entries.len() > 1 {
            println!("🔁 Дубликат: {}", symbol);
            for e in entries {
                let funding_rate = e.last_funding_rate().parse::<f64>().unwrap_or(0.0);
                let mark_price = e.mark_price().parse::<f64>().unwrap_or(0.0);
                let time = e.next_funding_time();
                let unix_time = e.unix_time();

                println!(
                    "  - Биржа: {}, Funding Rate: {}",
                    e.exchange_name(),
                    e.last_funding_rate(),
                );

                min_and_max_funding_by_symbol
                    .entry(e.symbol().clone())
                    .and_modify(|(
                        min_exchange_name, 
                        min_rate, 
                        max_exchange_name, 
                        max_rate, 
                        min_price, 
                        max_price,
                        min_time,
                        max_time,
                        min_unix_time,
                        max_unix_time
                    )| {
                        if funding_rate <= *min_rate {
                            *min_rate = funding_rate;
                            *min_exchange_name = e.exchange_name().to_string();
                            *min_price = mark_price;
                            *min_time = time.to_string();
                            *min_unix_time = *unix_time;
                        }
                        if funding_rate > *max_rate {
                            *max_rate = funding_rate;
                            *max_exchange_name = e.exchange_name().to_string();
                            *max_price = mark_price;
                            *max_time = time.to_string();
                            *max_unix_time = *unix_time;
                        }
                    })
                    .or_insert((
                        e.exchange_name().to_string(), // exchange_min
                        funding_rate,                 // min
                        e.exchange_name().to_string(), // exchange_max
                        funding_rate,                  // max,
                        mark_price,
                        mark_price,
                        time.to_string(), 
                        time.to_string(),
                        *unix_time,
                        *unix_time
                    ));
            }
        }
    }

    for (symbol, (
        min_exchange_name, 
        min_rate, 
        max_exchange_name, 
        max_rate, 
        min_price, 
        max_price,
        min_time,
        max_time,
        min_unix_time,
        max_unix_time
    )) in &min_and_max_funding_by_symbol {
        let rate_spread = (max_rate - min_rate).abs();
        let price_spread = (max_price - min_price) / max_price * 100.00;

        let current_time = Utc::now().timestamp();
        let min_timeout = normalize_timestamp_to_second(*min_unix_time) - current_time;
        let max_timeout = normalize_timestamp_to_second(*max_unix_time) - current_time;
        let min_second_to_format_datetime = convert_to_minutes(min_timeout);
        let max_second_to_format_datetime = convert_to_minutes(max_timeout);

        if (min_timeout <= 600) && (min_timeout > 30) || (max_timeout <= 600) && (max_timeout > 30) {
            let msg = format!(
                "\n${}\n\n🏦 Биржы: {} / {}\n📈 Курсовой спред: {:.4}% ({}$ | {}$)\n⌛️ Спред фандинга: {:+.4}%, ({}% | {}%) \n🕗 Обновление фандинга: {} / {} \n⏰️ Время до перерасчета: {} / {}", 
                symbol, 
                min_exchange_name, max_exchange_name,
                price_spread, min_price, max_price,
                rate_spread, min_rate, max_rate,
                min_time, max_time,
                min_second_to_format_datetime, max_second_to_format_datetime
            );

            if let Ok(false) = search_symbol_ten_minute(symbol.clone()).await {
                add_symbol(symbol.clone()).await.unwrap();

                // Отправляем сообщение в группу
                match send_message_to_group(msg).await {
                    Ok(_) => println!("Сообщение отправлено!"),
                    Err(e) => println!("Ошибка при отправке: {}", e)
                };
            }
        } else {
            delete_symbol(symbol.to_string()).await.unwrap();
        }

        let min_second_to_format_datetime = convert_to_normal_time(min_timeout);
        let max_second_to_format_datetime = convert_to_normal_time(max_timeout);

        println!("\n${}\n", symbol);
        println!("🏦 Биржы: {} / {}", min_exchange_name, max_exchange_name);
        println!("📈 Курсовой спред: {:.4}% ({}$ | {}$)", price_spread, min_price, max_price);
        println!("⌛️ Спред фандинга: {:.4}% ({}% | {}%)", rate_spread, min_rate, max_rate);
        println!("🕗 Обновление фандинга: {}/{}", min_time, max_time);
        println!("⏰️ Время до перерасчета: {} / {}", min_second_to_format_datetime, max_second_to_format_datetime);
    }
}
