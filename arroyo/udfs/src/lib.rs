use serde::Deserialize;

// use arroyo_udf::arroyo_udf; // Uncomment for deployment

#[derive(Deserialize)]
struct MarketDataTags {
    tags: Vec<String>,
}

// #[arroyo_udf] // Uncomment for deployment
pub fn is_wash_trade(json_data: String) -> bool {
    let parsed: Result<MarketDataTags, _> = serde_json::from_str(&json_data);
    match parsed {
        Ok(data) => {
            // Heuristic: If tags contain "wash", "self_match", or "mirror", strictly filter it out.
            data.tags.iter().any(|tag| {
                let t = tag.to_lowercase();
                t.contains("wash") || t.contains("self_match") || t.contains("mirror")
            })
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wash_trade_detection() {
        let clean_trade = r#"{"tags": ["valid", "market_maker"]}"#.to_string();
        assert!(!is_wash_trade(clean_trade));

        let wash_trade = r#"{"tags": ["wash", "arbitrage"]}"#.to_string();
        assert!(is_wash_trade(wash_trade));

        let mixed_case = r#"{"tags": ["SELF_MATCH"]}"#.to_string();
        assert!(is_wash_trade(mixed_case));

        // Test with missing tags field - should return false (safe default)
        let no_tags = r#"{"price": 100}"#.to_string();
        assert!(!is_wash_trade(no_tags));
    }
}
