pub enum ApiLimits {
    BinX,
    Bitget,
    Bybit,
    Gate,
    Mexc,
    WhiteBit,
    BitMart,
    Binance,
    Coinex,
    Kucoin,
    Okx,
    Htx,
    CoinW,
    Bitunix
}

impl ApiLimits {
    pub fn limit(&self) -> usize {
        match self {
            ApiLimits::BinX => 200,
            ApiLimits::Bitget => 1000,
            ApiLimits::Bybit => 1000,
            ApiLimits::Gate => 1000,
            ApiLimits::Mexc => 120,
            ApiLimits::WhiteBit => 1000,
            ApiLimits::BitMart => 120,
            ApiLimits::Binance => 200,
            ApiLimits::Coinex => 100,
            ApiLimits::Kucoin => 120,
            ApiLimits::Okx => 120,
            ApiLimits::Htx => 120,
            ApiLimits::CoinW => 120,
            ApiLimits::Bitunix => 10000,
        }
    }
}