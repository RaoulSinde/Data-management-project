import yfinance as yf
import pandas as pd
import numpy as np

start_date_project = '2022-01-01'
end_date_project = '2024-12-31'

dict_products = {
    'GOLD.PA': 'ETF Amundi Physical Gold', 'CW8.PA': 'ETF Amundi MSCI World', 'PRAC.DE': 'ETF Amundi Corporate Bonds',
    'IFRB.L': 'ETF iShares French Gov bonds', 'SDEU.L': 'ETF iSHares Germany Gov', '82829.HK': 'ETF iShares Chinese Gov',
    'GOVT': 'ETF iShares US Gov Bonds', '500.PA': 'ETF Amundi S&P 500', 'PRAC.DE': 'ETF Amundi Corporate Bonds',
    'IPRV.AS': 'ETF iShares Private Equity', 'DPYA.L': 'ETF iShares Dev Markets Property Yield',
    'HLTW.PA': 'ETF MSCI World Healthcare', 'AAPL': 'Apple', 'AMZN': 'Amazon', 'MSFT': 'Microsoft Corporation',
    'GOOGL': 'Google', 'TSLA': 'Tesla', 'META': 'Meta', 'NVDA': 'NVDIA', 'HO.PA': 'Thales',
    'AIR.PA': 'Airbus', 'TTE.PA': 'Total Energies', 'MC.PA': 'LVMH', 'RMS.PA': 'Hermes', 'NVO': 'Novo Nordisk'
}

dict_risk_type = {
    'GOLD.PA': 'low_risk', 'CW8.PA': 'low_risk', 'PRAC.DE': 'low_risk',
    'IFRB.L': 'low_risk', 'SDEU.L': 'low_risk', '82829.HK': 'low_risk', 'GOVT': 'low_risk',
    '500.PA': 'low_turnover', 'PRAC.DE': 'low_turnover', 'IPRV.AS': 'low_turnover',
    'DPYA.L': 'low_turnover', 'HLTW.PA': 'low_turnover', 'AAPL': 'high_yield_equity_only',
    'AMZN': 'high_yield_equity_only', 'MSFT': 'high_yield_equity_only', 'GOOGL': 'high_yield_equity_only',
    'TSLA': 'high_yield_equity_only', 'META': 'high_yield_equity_only', 'NVDA': 'high_yield_equity_only',
    'HO.PA': 'high_yield_equity_only', 'AIR.PA': 'high_yield_equity_only', 'TTE.PA': 'high_yield_equity_only',
    'MC.PA': 'high_yield_equity_only', 'RMS.PA': 'high_yield_equity_only', 'NVO': 'high_yield_equity_only'
}

def main(dict_1=dict_products, start_date=start_date_project, end_date=end_date_project, extreme_threshold=0.50):
    tickers = list(dict_1.keys())
    returns_data = (
        yf.download(tickers, start=start_date, end=end_date)['Close']
        .ffill()
        .pct_change()
        .replace([np.inf, -np.inf], 0)
        .rename(columns=dict_1)
    )

    # Detect and handle extreme values
    extreme_values = (returns_data.abs() > extreme_threshold)

    # Replace extreme values with the previous day's return
    for column in returns_data.columns:
        returns_data[column] = returns_data[column].mask(extreme_values[column]).ffill()

    returns_data_filtered = returns_data.iloc[1:]
    final_returns = returns_data_filtered.dropna()
    return final_returns

# Example usage
final_returns = main()
