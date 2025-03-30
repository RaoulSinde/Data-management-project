import pandas as pd
import numpy as np
import sqlite3
import json
from datetime import datetime, timedelta

def fetch_returns_from_db(database="project_database.db"):
    try:
        conn = sqlite3.connect(database)
        query = "SELECT date, ticker, return_value FROM Returns"
        returns_data = pd.read_sql_query(query, conn, parse_dates=['date'])
        conn.close()
        returns_data_deduped = returns_data.drop_duplicates(subset=['date', 'ticker'], keep='first')
        returns_data_pivot = returns_data_deduped.pivot(index='date', columns='ticker', values='return_value')
        return returns_data_pivot
    except sqlite3.Error as e:
        print(f"Erreur SQLite : {e}")
        return pd.DataFrame()

def low_risk_strategy(returns_data, volatility_target=0.10, volatility_window=30, momentum_window=30):
    rolling_volatility = returns_data.rolling(window=volatility_window).std() * np.sqrt(252)
    momentum = returns_data.pct_change(periods=momentum_window).iloc[-1]
    if rolling_volatility.empty or momentum.empty:
        return {}
    latest_volatility = rolling_volatility.iloc[-1]
    decisions = {}
    for product in returns_data.columns:
        if pd.notna(momentum[product]) and np.isfinite(momentum[product]):
            if latest_volatility[product] <= volatility_target:
                decisions[product] = int(momentum[product] * 10)  # Ensure qty is an integer
            else:
                decisions[product] = int(-momentum[product] * 10)
    return decisions

def low_turnover_strategy(returns_data, momentum_window=30, max_deals_per_month=2):
    momentum = returns_data.pct_change(periods=momentum_window).iloc[-1]
    decisions = {product: int(momentum[product] * 10)
                 for product in returns_data.columns if pd.notna(momentum[product]) and np.isfinite(momentum[product])}

    # Sort decisions by absolute value to prioritize larger moves
    sorted_decisions = sorted(decisions.items(), key=lambda x: abs(x[1]), reverse=True)

    # Limit the number of decisions to max_deals_per_month
    limited_decisions = dict(sorted_decisions[:max_deals_per_month])

    return limited_decisions

def high_yield_equity_strategy(returns_data, equity_tickers, momentum_window=10):
    equity_tickers = [ticker for ticker in equity_tickers if ticker in returns_data.columns]
    if not equity_tickers:
        return {}
    filtered_data = returns_data[equity_tickers]
    momentum = filtered_data.pct_change(periods=momentum_window).iloc[-1]
    decisions = {product: int(momentum[product] * 5)
                 for product in filtered_data.columns if pd.notna(momentum[product]) and np.isfinite(momentum[product])}
    return decisions

def record_deals(decisions, date, wallet_id, database="project_database.db", apply_deal_limit=False):
    try:
        conn = sqlite3.connect(database)
        cursor = conn.cursor()
        cursor.execute("SELECT manager_id FROM Managers WHERE wallets_managed_id = ?", (wallet_id,))
        manager_result = cursor.fetchone()
        if not manager_result:
            print(f"Aucun manager trouvé pour le portefeuille {wallet_id}")
            return
        manager_id = manager_result[0]
        cursor.execute("SELECT product_id, name FROM Products")
        product_id_map = {name: product_id for product_id, name in cursor.fetchall()}
        cursor.execute(""" 
            SELECT p.name, SUM(d.qty) as total_qty 
            FROM Deals d JOIN Products p ON d.product_id = p.product_id 
            WHERE d.wallet_id = ? GROUP BY p.name
        """, (wallet_id,))
        current_positions = {row[0]: row[1] for row in cursor.fetchall()}

        # Check the number of deals made in the current month if the limit should apply
        current_month_start = date[:7] + '-01'
        cursor.execute("""
            SELECT COUNT(*)
            FROM Deals
            WHERE wallet_id = ? AND date >= ?
        """, (wallet_id, current_month_start))
        current_month_deals_count = cursor.fetchone()[0]

        # Limit the number of transactions to 2 if the flag is set
        if apply_deal_limit and current_month_deals_count >= 2:
            print(f"Limite de transactions atteinte pour le portefeuille {wallet_id} en {date[:7]}")
            return

        insert_query = "INSERT INTO Deals (date, wallet_id, manager_id, product_id, qty) VALUES (?, ?, ?, ?, ?);"
        for product, qty in decisions.items():
            # Convert qty to an integer or float, ensuring it's a valid number
            try:
                qty = float(qty)  # Convert to float if possible
            except ValueError:
                continue  # If qty cannot be converted, skip this entry

            qty = min(abs(qty), 100) * np.sign(qty)  # Ensure we do not exceed 100 in absolute value

            # Only proceed if the limit is not reached
            if apply_deal_limit and current_month_deals_count >= 2:
                print(f"Limite de transactions atteinte pour le portefeuille {wallet_id} en {date[:7]}")
                break  # Stop processing any further transactions

            product_id = product_id_map.get(product)
            if product_id:
                current_position = current_positions.get(product, 0)
                if qty > 0:
                    cursor.execute(insert_query, (date, wallet_id, manager_id, product_id, qty))
                    print(f"Achat de {qty} unités de {product} dans le portefeuille {wallet_id} le {date}")
                    current_month_deals_count += 1
                elif qty < 0:
                    qty_to_sell = min(-qty, current_position)
                    if qty_to_sell > 0:
                        cursor.execute(insert_query, (date, wallet_id, manager_id, product_id, -qty_to_sell))
                        print(f"Vente de {qty_to_sell} unités de {product} dans le portefeuille {wallet_id} le {date}")
                        current_month_deals_count += 1
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erreur SQLite : {e}")
    finally:
        if conn:
            conn.close()

def update_portfolios(date, database="project_database.db"):
    try:
        current_date_dt = datetime.strptime(date, '%Y-%m-%d')
        conn = sqlite3.connect(database)
        cursor = conn.cursor()
        cursor.execute("SELECT wallet_id, risk_profile, products FROM Portfolios")
        portfolios = cursor.fetchall()
        full_returns_data = fetch_returns_from_db(database)
        returns_data = full_returns_data[full_returns_data.index <= current_date_dt]

        cursor.execute("SELECT ticker, name FROM Products")
        product_name_map = dict(cursor.fetchall())
        for wallet_id, risk_profile, products in portfolios:
            authorized_products_ids = json.loads(products)
            cursor.execute("SELECT ticker FROM Products WHERE product_id IN ({})".format(
                ','.join(map(str, authorized_products_ids))
            ))
            authorized_tickers = [row[0] for row in cursor.fetchall()]
            available_tickers = [ticker for ticker in authorized_tickers if ticker in returns_data.columns]
            if not available_tickers:
                continue
            filtered_returns = returns_data[available_tickers]
            if risk_profile == "low_risk":
                decisions = low_risk_strategy(filtered_returns)
                apply_deal_limit = False  # No limit for low_risk
            elif risk_profile == "low_turnover":
                decisions = low_turnover_strategy(filtered_returns)
                apply_deal_limit = True  # Apply limit for low_turnover
            elif risk_profile == "high_yield_equity_only":
                decisions = high_yield_equity_strategy(filtered_returns, available_tickers)
                apply_deal_limit = False  # No limit for high_yield_equity_only
            else:
                continue
            named_decisions = {product_name_map.get(ticker, ticker): qty for ticker, qty in decisions.items()}
            record_deals(named_decisions, date, wallet_id, database, apply_deal_limit)
    except sqlite3.Error as e:
        print(f"Erreur SQLite : {e}")
    finally:
        if conn:
            conn.close()

def run_weekly_updates(database="project_database.db"):
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() == 0:
            update_portfolios(current_date.strftime('%Y-%m-%d'), database)
        current_date += timedelta(days=1)

run_weekly_updates()
