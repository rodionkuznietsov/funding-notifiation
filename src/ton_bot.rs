use reqwest::Error;
use serde::Deserialize;

const BOT_API: &str = "YOUR_API_KEY";
const CHAT_ID: &str = "@your_group_for_funding_rate";

#[derive(Debug, Deserialize)]
struct ApiResponse {
    result: ResultResponse
}

#[derive(Debug, Deserialize, Default)]
struct ResultResponse {
    message_id: i64,
    sender_chat: Chat,
    chat: Chat,
    date: i64,
    text: String
}

#[derive(Debug, Deserialize, Default)]
struct Chat {
    id: i64,
    title: String,
    username: String,
    #[serde(rename = "type")]
    _type: String
}


#[derive(Debug, Default, Deserialize)]
struct ChannelPost {
    #[serde(default)]
    date: i64,
    #[serde(default)]
    text: String
}

pub async fn send_message_to_group(msg: String) -> Result<(), Error> {
    let url = format!("https://api.telegram.org/bot{}/sendMessage", BOT_API);

    let client = reqwest::Client::new();
    let response = client.get(url)
        .query(&[("chat_id", CHAT_ID), ("text", &msg)])
        .send()
        .await?;

    let response_json: ApiResponse = response.json().await?;

    // println!("Response JSon: {:?}", response_json.result);

    Ok(())
}
