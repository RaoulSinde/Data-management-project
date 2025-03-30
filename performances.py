import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import yfinance as yf
import ast

# Paramètres
DB_PATH = "project_database.db"  
RISK_FREE_RATE_ANNUAL = 0.02 # Je prend un taux sans risque de 2%
TRADING_DAYS = 252                 
START_DATE = "2023-01-01"
END_DATE = "2024-12-31"

def connect_db(db_path):
    """Se connecter à la base de données SQLite."""
    try:
        conn = sqlite3.connect(db_path)
        print("Connexion à la base de données réussie.")
        return conn
    except Exception as e:
        print(f"Erreur de connexion à la base : {e}")
        raise

def get_products_for_wallet(conn, wallet_id):
    """
    Récupérer la liste des product_id associés à un portefeuille.
    
    Le champ 'products' dans la table Portfolios est supposé contenir une chaîne de caractères.
    """
    query = "SELECT products FROM Portfolios WHERE wallet_id = ?"
    df = pd.read_sql_query(query, conn, params=(wallet_id,))
    if df.empty:
        return []
    products_str = df.iloc[0]['products']
    try:
        products_list = ast.literal_eval(products_str)
        return products_list
    except Exception as e:
        print(f"Erreur lors de la conversion des produits pour le wallet {wallet_id}: {e}")
        return []

def get_portfolio_returns(conn, wallet_id):
    """
    Récupérer les retours journaliers agrégés pour un portefeuille donné sur la période [START_DATE, END_DATE].

    Pour le portefeuille identifié par wallet_id, on récupère la liste des produits associés 
    puis on interroge la table Returns pour obtenir, pour chaque date, la moyenne des return_value.
    Le résultat est une DataFrame avec :
      - date
      - return (moyenne des return_value pour les produits du portefeuille)
    """
    products = get_products_for_wallet(conn, wallet_id)
    if not products:
        print(f"Aucun produit associé au portefeuille {wallet_id}.")
        return pd.DataFrame()
    
    # Préparation des placeholders pour la requête SQL
    placeholders = ",".join("?" for _ in products)
    query = f"""
    SELECT date, AVG(return_value) as return_value
    FROM Returns
    WHERE product_id IN ({placeholders})
      AND date BETWEEN ? AND ?
    GROUP BY date
    ORDER BY date;
    """
    # On ajoute START_DATE et END_DATE à la liste des paramètres
    df = pd.read_sql_query(query, conn, params=tuple(products) + (START_DATE, END_DATE))
    if not df.empty:
        df['date'] = pd.to_datetime(df['date'])
        df.sort_values(by='date', inplace=True)
        df.rename(columns={'return_value': 'return'}, inplace=True)
    return df

def get_sp500_returns():
    """
    Récupérer les retours journaliers du SP500 via Yahoo Finance pour la période d'évaluation.
    
    On utilise le ticker '^GSPC' et on calcule les retours à partir de la colonne de prix ajusté.
    Si les colonnes sont en multi-index, on les aplatit pour faciliter l'accès.
    """
    sp500 = yf.download("^GSPC", start=START_DATE, end=END_DATE, auto_adjust=True)
    if sp500.empty:
        print("Erreur lors du téléchargement des données du SP500.")
        return pd.DataFrame()
    
    if isinstance(sp500.columns, pd.MultiIndex):
        sp500.columns = sp500.columns.get_level_values(0)
    
    sp500.reset_index(inplace=True)
    
    if 'Close' in sp500.columns:
        price_col = 'Close'
    elif 'Adj Close' in sp500.columns:
        price_col = 'Adj Close'
    else:
        print("Aucune colonne de prix trouvée dans les données du SP500.")
        return pd.DataFrame()
    
    sp500['date'] = pd.to_datetime(sp500['Date'])
    sp500.sort_values(by='date', inplace=True)
    sp500['return'] = sp500[price_col].pct_change()
    sp500.dropna(subset=['return'], inplace=True)
    return sp500[['date', 'return']]

def get_portfolio_ids(conn):
    """
    Récupérer la correspondance entre wallet_name et wallet_id pour tous les portefeuilles.
    Retourne un dictionnaire : {wallet_name: wallet_id, ...}
    """
    query = "SELECT wallet_id, wallet_name FROM Portfolios"
    df = pd.read_sql_query(query, conn)
    return dict(zip(df['wallet_name'], df['wallet_id']))

def get_portfolio_manager_mapping(conn):
    """
    Récupérer la correspondance entre portefeuilles et managers en joignant les tables Portfolios et Managers.
    
    La jointure se fait sur : Portfolios.wallet_id = Managers.wallets_managed_id.
    La fonction retourne une DataFrame avec les colonnes :
      - wallet_name
      - manager_name
    """
    query = """
    SELECT p.wallet_name, m.manager_name 
    FROM Portfolios p
    JOIN Managers m ON p.wallet_id = m.wallets_managed_id;
    """
    mapping_df = pd.read_sql_query(query, conn)
    return mapping_df

def compute_sharpe_ratio(returns_series):
    """
    Calcul du ratio de Sharpe annualisé
    """
    if returns_series.empty:
        return np.nan
    daily_rf = RISK_FREE_RATE_ANNUAL / TRADING_DAYS
    excess_returns = returns_series - daily_rf
    mean_excess = excess_returns.mean()
    std_dev = excess_returns.std()
    if std_dev == 0:
        return np.nan
    sharpe = (mean_excess / std_dev) * np.sqrt(TRADING_DAYS)
    return sharpe

def compute_cumulative_returns(df):
    """
    Calcul des retours cumulatifs
    """
    if df.empty:
        return df
    df = df.copy()
    df['cum_return'] = (1 + df['return']).cumprod() - 1
    return df

def compute_beta(portfolio_df, benchmark_df):
    """
    Calcul du bêta du portefeuille par rapport au SP500.
    """
    if portfolio_df.empty or benchmark_df.empty:
        return np.nan
    merged = pd.merge(portfolio_df[['date', 'return']], benchmark_df[['date', 'return']], on='date', suffixes=('_port', '_bench'))
    if merged.empty:
        return np.nan
    cov = np.cov(merged['return_port'], merged['return_bench'])[0, 1]
    var = np.var(merged['return_bench'])
    beta = cov / var if var != 0 else np.nan
    return beta

def compute_volatility(returns_series):
    """
    Calcul de la volatilité annualisée du portefeuille.
    """
    if returns_series.empty:
        return np.nan
    daily_vol = returns_series.std()
    annual_vol = daily_vol * np.sqrt(TRADING_DAYS)
    return annual_vol

def compute_max_drawdown(df):
    """
    Calcul du max drawdown du portefeuille.
    """
    if df.empty:
        return np.nan
    portfolio_values = 1 + df['cum_return']
    running_max = portfolio_values.cummax()
    drawdown = (running_max - portfolio_values) / running_max
    max_drawdown = drawdown.max()
    return max_drawdown

def display_portfolio_content(conn, wallet_name):
    """
    Affiche le contenu du portefeuille spécifié par wallet_name.
    La colonne 'products' de la table Portfolios contient une chaîne représentant une liste d'IDs produits.
    La fonction convertit ces IDs en noms en utilisant la table Products.
    
    Args:
        conn (sqlite3.Connection): La connexion à la base de données.
        wallet_name (str): Le nom du portefeuille à afficher.
    """
    # Récupérer la ligne correspondant au portefeuille
    query = "SELECT products FROM Portfolios WHERE wallet_name = ?"
    df = pd.read_sql_query(query, conn, params=(wallet_name,))
    
    if df.empty:
        print(f"Aucun portefeuille trouvé pour {wallet_name}.")
        return

    products_str = df.iloc[0]['products']
    try:
        # Conversion de la chaîne en liste d'IDs
        product_ids = ast.literal_eval(products_str)
    except Exception as e:
        print(f"Erreur de conversion pour {wallet_name}: {e}")
        product_ids = []

    # Récupérer la correspondance entre product_id et nom depuis la table Products
    prod_query = "SELECT product_id, name FROM Products"
    prod_df = pd.read_sql_query(prod_query, conn)
    product_dict = dict(zip(prod_df['product_id'], prod_df['name']))
    
    # Remplacer chaque product_id par le nom correspondant (ou afficher l'ID si le nom est introuvable)
    product_names = [product_dict.get(pid, f"ID {pid}") for pid in product_ids]
    print(f"Portefeuille {wallet_name} contient : {product_names}")
        
def get_recent_deals(conn, wallet_id, limit=50):
    """
    Retourne les derniers deals pour le portefeuille spécifié (wallet_id).

    Pour chaque deal, on récupère :
      - La date de l'opération
      - Le nom du produit (via une jointure avec la table Products sur la colonne "name")
      - La quantité (qty) en valeur absolue
      - Une colonne 'operation' indiquant "Achat" pour une quantité positive et "Vente" pour une quantité négative

    Args:
        conn (sqlite3.Connection): La connexion à la base de données.
        wallet_id (int): L'identifiant du portefeuille.
        limit (int): Le nombre maximum de deals à retourner.

    Returns:
        DataFrame: Un DataFrame contenant les deals triés par date décroissante.
    """
    query = """
    SELECT d.date, pr.name AS product_name, d.qty
    FROM Deals d
    JOIN Products pr ON d.product_id = pr.product_id
    WHERE d.wallet_id = ?
    ORDER BY d.date DESC
    LIMIT ?
    """
    df_deals = pd.read_sql_query(query, conn, params=(wallet_id, limit))
    if not df_deals.empty:
        # On calcule d'abord l'opération à partir du signe original de qty
        df_deals['operation'] = df_deals['qty'].apply(lambda x: "Achat" if x > 0 else ("Vente" if x < 0 else "Inconnu"))
        # Puis on transforme qty en valeur absolue
        df_deals['qty'] = df_deals['qty'].abs()
    return df_deals
    
def main():
    # Connexion à la base de données
    conn = connect_db(DB_PATH)
    
    # Afficher le contenu des portefeuilles
    portfolios = get_portfolio_ids(conn)
    if not portfolios:
        print("Aucun portefeuille trouvé dans la base de données.")
    else:
        print("\n=== Contenu de tous les portefeuilles ===")
        for wallet_name in portfolios.keys():
            display_portfolio_content(conn, wallet_name)
    
    # Affichage des derniers deals pour chaque portefeuille
    print("\n=== Derniers deals par portefeuille ===")
    for wallet_name, wallet_id in portfolios.items():
        print(f"\nPortefeuille {wallet_name} :")
        df_deals = get_recent_deals(conn, wallet_id, limit=10)
        if df_deals.empty:
            print("  Aucun deal trouvé.")
        else:
            print(df_deals.to_string(index=False))
    
    # Récupération des données du SP500 pour le calcul du bêta
    sp500_df = get_sp500_returns()
    if sp500_df.empty:
        print("Impossible de récupérer les données du SP500 pour le calcul du bêta.")
    
    # Dictionnaires pour stocker les indicateurs
    sharpe_ratios   = {}
    betas           = {}
    final_cum_returns = {}
    cumulative_data = {} 
    volatilities    = {}
    max_drawdowns   = {}
    
    # Calcul des métriques pour chaque portefeuille
    for wallet_name, wallet_id in portfolios.items():
        df = get_portfolio_returns(conn, wallet_id)
        if df.empty:
            print(f"Aucune donnée de retour pour le portefeuille {wallet_name}.")
            continue
        
        # Calcul du ratio de Sharpe
        sharpe = compute_sharpe_ratio(df['return'])
        sharpe_ratios[wallet_name] = sharpe
        
        # Calcul de la volatilité annualisée
        vol = compute_volatility(df['return'])
        volatilities[wallet_name] = vol
        
        # Calcul du rendement cumulé
        df = compute_cumulative_returns(df)
        cumulative_data[wallet_name] = df
        final_cum_returns[wallet_name] = df['cum_return'].iloc[-1]
        
        # Calcul du max drawdown
        max_dd = compute_max_drawdown(df)
        max_drawdowns[wallet_name] = max_dd
        
        # Calcul du bêta avec le SP500
        if not sp500_df.empty:
            beta = compute_beta(df, sp500_df)
            betas[wallet_name] = beta

    # Affichage des résultats
    print("\n=== Ratio de Sharpe par portefeuille ===")
    for wallet_name, sharpe in sharpe_ratios.items():
        print(f"Portefeuille {wallet_name} : Sharpe Ratio = {sharpe:.3f}")
        
    if betas:
        print("\n=== Bêta par portefeuille (par rapport au SP500) ===")
        for wallet_name, beta in betas.items():
            print(f"Portefeuille {wallet_name} : Bêta = {beta:.3f}")
    
    print("\n=== Rendement cumulé final par portefeuille ===")
    for wallet_name, cum_return in final_cum_returns.items():
        print(f"Portefeuille {wallet_name} : Rendement cumulé = {cum_return*100:.2f}%")
    
    print("\n=== Volatilité annualisée par portefeuille ===")
    for wallet_name, vol in volatilities.items():
        print(f"Portefeuille {wallet_name} : Volatilité = {vol:.3f}")
    
    print("\n=== Max Drawdown par portefeuille ===")
    for wallet_name, max_dd in max_drawdowns.items():
        print(f"Portefeuille {wallet_name} : Max Drawdown = {max_dd*100:.2f}%")
    
    # Attribution des managers via la jointure avec la table Managers
    mapping_df = get_portfolio_manager_mapping(conn)
    if not mapping_df.empty:
        mapping_df['final_return'] = mapping_df['wallet_name'].map(final_cum_returns)
        # Calcul de la moyenne des rendements cumulatifs par manager
        manager_perf = mapping_df.groupby('manager_name')['final_return'].mean()
        best_manager = manager_perf.idxmax()
        best_return = manager_perf.max()
        print(f"\nLe manager le plus performant est {best_manager} avec un rendement cumulé moyen de {best_return*100:.2f}%.")
    else:
        print("Impossible de déterminer le manager le plus performant.")
    
    # Tracé du graphique des retours cumulatifs (sans marqueurs)
    plt.style.use('ggplot')
    fig, ax = plt.subplots(figsize=(12, 8))
    for wallet_name, df in cumulative_data.items():
        ax.plot(df['date'], df['cum_return'], linewidth=2, label=f"Portefeuille {wallet_name}")
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Retour cumulatif", fontsize=12)
    ax.set_title("Performance cumulée des portefeuilles", fontsize=14, fontweight='bold')
    ax.legend(title="Portefeuilles", fontsize=10)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    fig.autofmt_xdate() 
    plt.tight_layout()
    plt.show()
    
    # Fermeture de la connexion
    conn.close()

if __name__ == '__main__':
    main()