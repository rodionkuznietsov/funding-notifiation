use chrono::Utc;
use sqlx::{sqlite::SqliteRow, Pool, Row, Sqlite, SqlitePool};

#[derive(Debug)]
pub struct SymbolData {
    symbol: String,
    timestamp: i64
}

async fn _db_connect() -> Result<Pool<Sqlite>, sqlx::Error>{
    let pool = SqlitePool::connect("sqlite://config/symbols.db").await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS symbols (
            symbol TEXT PRIMARY KEY,
            timestamp INTENGER
        )"
    ).execute(&pool).await?;
    
    Ok(pool)
}

pub async fn search_symbol_ten_minute(symbol: String) -> Result<bool, sqlx::Error> {
    let symbol = symbol.to_lowercase();

    let pool = _db_connect().await.expect("Не удалось подключиться к дб");

    let symbol_data = get_symbol(symbol.clone()).await;

    if let Some(data) = symbol_data {
        let timestamp = data.timestamp;
        let now = Utc::now().timestamp();
        let ten_minutes_ago = now - 570;
        //              1:40
        // 1:38   >=    1:30
        if timestamp < ten_minutes_ago {
            delete_symbol(symbol).await?; // Удаляем данные из временной базы данных
            return Ok(false);
        } else {
           return Ok(true);
        }
    }

    Ok(false)
}

pub async fn delete_symbol(symbol: String) -> Result<(), sqlx::Error> {
    let pool = _db_connect().await.expect("Не удалось подключится к бд");

    sqlx::query("DELETE FROM symbols WHERE symbol = ?")
        .bind(symbol)
        .execute(&pool)
        .await?;

    Ok(())
}

pub async fn add_symbol(symbol: String) -> Result<(), sqlx::Error> {
    let pool = _db_connect().await.expect("Не удалось подключиться к дб");

    let now = Utc::now().timestamp();
    sqlx::query("INSERT OR REPLACE INTO symbols (symbol, timestamp) VALUES (?, ?)")
        .bind(symbol.to_lowercase())
        .bind(now)
        .execute(&pool)
        .await.expect("Не удалось добавить токен");

    println!("Токен успешно добавлен");

    Ok(())
}

pub async fn get_symbol(symbol: String) -> Option<SymbolData> {
    let pool = _db_connect().await.expect("Не удалось подключиться к дб");

    let symbol_data: Vec<(String, i64)> = sqlx::query("SELECT * FROM symbols WHERE symbol = ?")
        .bind(symbol)
        .map(|row: SqliteRow| (row.get("symbol"), row.get("timestamp")))
        .fetch_all(&pool)
        .await.expect("Не удалось найти данных");

    for (symbol, timestamp) in symbol_data {
        let symbol_data = SymbolData {
            symbol: symbol.clone(),
            timestamp: timestamp.clone()
        };

        return Some(symbol_data)
    }

    None
}