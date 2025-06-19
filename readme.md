# 📊 Funding Rate Bot

A Rust-based script that collects real-time data from multiple cryptocurrency exchanges.  
It retrieves important metrics such as:

- **Funding rate**
- **Mark price**
- **Next funding time**

It also includes a **Telegram bot** that notifies users about the most profitable funding opportunities across different platforms.

---

## 🌐 Supported Exchanges

This bot currently integrates with the following exchanges:

1. Binance  
2. BinX  
3. Bitget  
4. BitMart  
5. Bybit  
6. CoinEx  
7. Gate  
8. KuCoin  
9. MEXC  
10. OKX  
11. WhiteBit  
12. HTX
13. CoinW
14. Bitunix

---

## 🧠 Features

- ✅ Asynchronous data fetching using `reqwest` and `tokio`
- ✅ Automatic filtering by funding rate threshold
- ✅ Smart deduplication of symbols
- ✅ Telegram integration for notifications
- ✅ Easily extensible for adding more exchanges

---

## 🧑‍💻 Author
Made with ❤️ by [Rodion Kuznietsov]
Telegram: [@rodionkuznietsov]