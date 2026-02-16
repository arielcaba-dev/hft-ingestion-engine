/*
[dependencies]
statrs = "0.16"
*/

pub fn calculate_rsi(prices: Vec<f64>) -> Option<f64> {
    if prices.len() < 15 {
        return None;
    }

    let mut gains = 0.0;
    let mut losses = 0.0;

    for i in 1..15 {
        let change = prices[i] - prices[i - 1];
        if change > 0.0 {
            gains += change;
        } else {
            losses -= change;
        }
    }

    let mut avg_gain = gains / 14.0;
    let mut avg_loss = losses / 14.0;

    for i in 15..prices.len() {
        let change = prices[i] - prices[i - 1];
        if change > 0.0 {
            avg_gain = (avg_gain * 13.0 + change) / 14.0;
            avg_loss = (avg_loss * 13.0) / 14.0;
        } else {
            avg_gain = (avg_gain * 13.0) / 14.0;
            avg_loss = (avg_loss * 13.0 - change) / 14.0;
        }
    }

    if avg_loss == 0.0 {
        return Some(100.0);
    }

    let rs = avg_gain / avg_loss;
    Some(100.0 - (100.0 / (1.0 + rs)))
}

pub fn calculate_realized_volatility(prices: Vec<f64>) -> Option<f64> {
    if prices.len() < 2 {
        return None;
    }

    let mut log_returns = Vec::with_capacity(prices.len() - 1);
    for i in 1..prices.len() {
        log_returns.push((prices[i] / prices[i - 1]).ln());
    }

    if log_returns.is_empty() {
        return None;
    }

    let mean = log_returns.iter().sum::<f64>() / log_returns.len() as f64;
    let variance = log_returns.iter().map(|&x| (x - mean).powi(2)).sum::<f64>()
        / (log_returns.len() - 1) as f64;

    // Annualize (assuming 5-min windows, scaled to yearly)
    // For crypto 24/7: sqrt(variance * 288 * 365) ?
    // Standard realization is often just the std dev of the window.
    // Let's return simple standard deviation of the returns for now.
    Some(variance.sqrt())
}

pub fn calculate_liquidity_score(volumes: Vec<f64>, prices: Vec<f64>) -> Option<f64> {
    if volumes.len() != prices.len() || prices.len() < 2 {
        return None;
    }

    let total_volume: f64 = volumes.iter().sum();

    // Calculate price variance as proxy for volatility
    let mean_price = prices.iter().sum::<f64>() / prices.len() as f64;
    let price_variance = prices
        .iter()
        .map(|&x| (x - mean_price).powi(2))
        .sum::<f64>()
        / (prices.len() - 1) as f64;

    if price_variance == 0.0 {
        return Some(total_volume * 100.0); // High liquidity if no price movement
    }

    // Hui-Heubel Liquidity Ratio inverted?
    // Simple Score: Volume / Volatility
    Some(total_volume / price_variance.sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx_eq(a: f64, b: f64, epsilon: f64) -> bool {
        (a - b).abs() < epsilon
    }

    // ========== RSI Tests ==========

    #[test]
    fn test_rsi_insufficient_data() {
        assert_eq!(calculate_rsi(vec![100.0, 101.0, 102.0]), None);
    }

    #[test]
    fn test_rsi_bounds() {
        let prices = vec![
            44.0, 44.5, 45.0, 45.5, 46.0, 46.5, 47.0, 47.5, 48.0, 48.5, 49.0, 49.5, 50.0, 50.5,
            51.0,
        ];
        let rsi = calculate_rsi(prices).unwrap();
        assert!(rsi >= 0.0 && rsi <= 100.0);
    }

    #[test]
    fn test_rsi_all_gains() {
        let mut prices = vec![100.0];
        for i in 1..=20 {
            prices.push(100.0 + i as f64);
        }
        let rsi = calculate_rsi(prices).unwrap();
        assert!(approx_eq(rsi, 100.0, 0.01));
    }

    #[test]
    fn test_rsi_all_losses() {
        let mut prices = vec![100.0];
        for i in 1..=20 {
            prices.push(100.0 - i as f64);
        }
        let rsi = calculate_rsi(prices).unwrap();
        assert!(approx_eq(rsi, 0.0, 0.01));
    }

    // ========== Volatility Tests ==========

    #[test]
    fn test_volatility_insufficient_data() {
        assert_eq!(calculate_realized_volatility(vec![100.0]), None);
    }

    #[test]
    fn test_volatility_zero_variance() {
        let vol = calculate_realized_volatility(vec![100.0; 20]).unwrap();
        assert!(approx_eq(vol, 0.0, 1e-10));
    }

    #[test]
    fn test_volatility_positive() {
        let prices = vec![100.0, 101.0, 99.0, 102.0, 98.0, 103.0];
        assert!(calculate_realized_volatility(prices).unwrap() > 0.0);
    }

    #[test]
    fn test_volatility_comparison() {
        let high_var = vec![100.0, 110.0, 90.0, 120.0, 80.0];
        let low_var = vec![100.0, 100.5, 99.5, 100.2, 99.8];
        assert!(
            calculate_realized_volatility(high_var).unwrap()
                > calculate_realized_volatility(low_var).unwrap()
        );
    }

    // ========== Liquidity Tests ==========

    #[test]
    fn test_liquidity_insufficient_data() {
        assert_eq!(calculate_liquidity_score(vec![100.0], vec![50.0]), None);
    }

    #[test]
    fn test_liquidity_mismatched_lengths() {
        assert_eq!(
            calculate_liquidity_score(vec![100.0, 200.0], vec![50.0]),
            None
        );
    }

    #[test]
    fn test_liquidity_zero_variance() {
        let liq =
            calculate_liquidity_score(vec![100.0, 200.0, 300.0], vec![50.0, 50.0, 50.0]).unwrap();
        assert!(liq > 1000.0);
    }

    #[test]
    fn test_liquidity_comparison() {
        let high_liq =
            calculate_liquidity_score(vec![1000.0, 1000.0, 1000.0], vec![100.0, 100.1, 99.9])
                .unwrap();
        let low_liq =
            calculate_liquidity_score(vec![10.0, 10.0, 10.0], vec![100.0, 110.0, 95.0]).unwrap();
        assert!(high_liq > low_liq);
    }
}
