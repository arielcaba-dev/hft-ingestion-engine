use crate::model::NormalizedMarketData;
use crate::config::SymbolConfig;
use std::collections::HashMap;

pub trait Normalizer {
    fn normalize(&self, raw_msg: &str) -> Option<NormalizedMarketData>;
}

pub struct StandardNormalizer {
    symbol_map: HashMap<String, String>,
}

impl StandardNormalizer {
    pub fn new(symbols: Vec<SymbolConfig>) -> Self {
        let mut map = HashMap::new();
        for s in symbols {
            for (exchange, remote_id) in s.exchange_mappings {
                // simple mapping key: "exchange:remote_id" -> "internal_id"
                map.insert(format!("{}:{}", exchange, remote_id), s.internal_id.clone());
            }
        }
        Self { symbol_map: map }
    }
}
