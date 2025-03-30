import subprocess
import sys

# Installer les dépendances à partir du fichier requirements.txt
subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])


import streamlit as st
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import ast
import performances 

### Avant de lancer l'app :
#- S'assurer que performances.py est dans le même répertoire que app.py
#- Pour lancer l'app : "streamlit run app.py" dans le terminal (dans le répertoire où se trouve app.py)


st.title("Dashboard de performance du fond")
st.write("Veuillez sélectionner un portefeuille dans le menu de gauche afin d’afficher les détails de sa performance.")

# Connexion à la base de données via performances.py
conn = performances.connect_db(performances.DB_PATH)
if conn is None:
    st.error("Erreur de connexion à la base de données.")
    st.stop()

# Récupération des portefeuilles : dictionnaire {wallet_name: wallet_id}
portfolio_dict = performances.get_portfolio_ids(conn)
if not portfolio_dict:
    st.error("Aucun portefeuille trouvé dans la base de données.")
    st.stop()

##################################################################################
# Sélection du portefeuille et affichage du meilleur manager via la barre latérale
##################################################################################
st.sidebar.header("Sélection du Portefeuille")
selected_portfolio = st.sidebar.selectbox("Sélectionnez votre portefeuille", list(portfolio_dict.keys()))
wallet_id = portfolio_dict[selected_portfolio]

# Calcul du meilleur manager et de son portefeuille
mapping_df = performances.get_portfolio_manager_mapping(conn)
if not mapping_df.empty:
    final_cum_returns_all = {}
    for wallet_name, port_id in portfolio_dict.items():  # Remplacer wallet_id par port_id ici
         df_ret = performances.get_portfolio_returns(conn, port_id)
         if not df_ret.empty:
             df_cum = performances.compute_cumulative_returns(df_ret)
             final_cum_returns_all[wallet_name] = df_cum['cum_return'].iloc[-1]
    mapping_df['final_return'] = mapping_df['wallet_name'].map(final_cum_returns_all)
    manager_perf = mapping_df.groupby('manager_name')['final_return'].mean()
    best_manager = manager_perf.idxmax()
    # Sélectionne le portefeuille géré par ce manager ayant le meilleur rendement
    best_portfolio_df = mapping_df[mapping_df['manager_name'] == best_manager]
    best_portfolio = best_portfolio_df.sort_values(by='final_return', ascending=False)['wallet_name'].iloc[0]
    st.sidebar.subheader("Meilleur Manager")
    st.sidebar.write(f"**{best_manager}** avec le portefeuille **{best_portfolio}**")


###############################################
# Détails du Portefeuille 
###############################################
st.header(f"Informations pour le portefeuille {selected_portfolio}")

# Récupération des retours journaliers du portefeuille sélectionné
df_returns = performances.get_portfolio_returns(conn, wallet_id)
if df_returns.empty:
    st.write("Aucune donnée de retour pour ce portefeuille.")
else:
    # Calcul des métriques de performance
    sharpe_ratio = performances.compute_sharpe_ratio(df_returns['return'])
    sp500_df = performances.get_sp500_returns()
    beta = performances.compute_beta(df_returns, sp500_df) if not sp500_df.empty else np.nan
    df_cum = performances.compute_cumulative_returns(df_returns)
    final_cum_return = df_cum['cum_return'].iloc[-1]
    
    # Calcul de la volatilité annualisée et du max drawdown
    volatility = performances.compute_volatility(df_returns['return'])
    max_drawdown = performances.compute_max_drawdown(df_cum)
    
    st.subheader("Métriques de performance du portefeuille")
    st.write("Les métriques de performance sont calculées sur la période du 01/01/2023 au 31/12/2024")
    st.write(f"**➡️ Ratio de Sharpe** : {sharpe_ratio:.3f}")
    st.write(f"**➡️ Bêta** : {beta:.3f}")
    st.write(f"**➡️ Rendement cumulé** : {final_cum_return*100:.2f}%")
    st.write(f"**➡️ Volatilité annualisée** : {volatility:.3f}")
    st.write(f"**➡️ Max Drawdown** : {max_drawdown*100:.2f}%")
    
    # Graphique de performance cumulée pour le portefeuille sélectionné avec comparaison SP500
    st.subheader("Graphique de la performance cumulée du portefeuille")
    plt.style.use('ggplot')
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.plot(df_cum['date'], df_cum['cum_return'], linewidth=2, label=selected_portfolio)
    
    # Calcul et tracé de la performance cumulée du SP500
    if not sp500_df.empty:
        sp500_cum = performances.compute_cumulative_returns(sp500_df)
        ax.plot(sp500_cum['date'], sp500_cum['cum_return'], linewidth=2, linestyle='--', label='SP500')
    
    ax.set_xlabel("Date", fontsize=12, fontweight='bold')
    ax.set_ylabel("Retour Cumulé", fontsize=12, fontweight='bold')
    ax.set_title("Performance Cumulée", fontsize=14, fontweight='bold')
    ax.legend(title="Portefeuille", fontsize=10)
    ax.grid(True, linestyle='--', alpha=0.6)
    fig.autofmt_xdate()
    plt.tight_layout()
    st.pyplot(fig)

# Affichage du contenu du portefeuille sous forme de tableau
st.subheader("Contenu du portefeuille")
query = "SELECT products FROM Portfolios WHERE wallet_name = ?"
df_portfolio = pd.read_sql_query(query, conn, params=(selected_portfolio,))
if df_portfolio.empty:
    st.write("Aucun portefeuille trouvé pour ce nom.")
else:
    products_str = df_portfolio.iloc[0]['products']
    try:
        product_ids = ast.literal_eval(products_str)
    except Exception as e:
        st.write(f"Erreur de conversion pour {selected_portfolio}: {e}")
        product_ids = []
    # Récupération de la correspondance entre product_id et nom depuis la table Products
    prod_query = "SELECT product_id, name FROM Products"
    prod_df = pd.read_sql_query(prod_query, conn)
    product_dict = dict(zip(prod_df['product_id'], prod_df['name']))
    product_names = [product_dict.get(pid, f"ID {pid}") for pid in product_ids]
    df_content = pd.DataFrame({'Actifs': product_names})
    st.dataframe(df_content)

# Affichage des 50 dernières transactions du portefeuille sélectionné
st.subheader("Les 50 dernières transactions du portefeuille")
df_deals = performances.get_recent_deals(conn, wallet_id, limit=50)
if df_deals.empty:
    st.write("Aucun deal trouvé pour ce portefeuille.")
else:
    st.dataframe(df_deals)

##################################################################################
# Graphique comparatif de tous les portefeuilles avec la performance du SP500
##################################################################################
st.header("Comparaison des performances de tous les portefeuilles")
cumulative_data_all = {}
for wallet_name, wallet_id in portfolio_dict.items():
    df_ret = performances.get_portfolio_returns(conn, wallet_id)
    if not df_ret.empty:
        df_cum_all = performances.compute_cumulative_returns(df_ret)
        cumulative_data_all[wallet_name] = df_cum_all

if cumulative_data_all:
    plt.style.use('ggplot')
    fig_all, ax_all = plt.subplots(figsize=(12, 8))
    for wallet_name, df in cumulative_data_all.items():
        ax_all.plot(df['date'], df['cum_return'], linewidth=2, label=wallet_name)
    # Ajout de la courbe du SP500
    if not sp500_df.empty:
        sp500_cum_all = performances.compute_cumulative_returns(sp500_df)
        ax_all.plot(sp500_cum_all['date'], sp500_cum_all['cum_return'], linewidth=2, linestyle='--', label='SP500')
    ax_all.set_xlabel("Date", fontsize=12, fontweight='bold')
    ax_all.set_ylabel("Retour Cumulé", fontsize=12, fontweight='bold')
    ax_all.set_title("Comparaison des performances cumulées", fontsize=14, fontweight='bold')
    ax_all.legend(title="Portefeuilles", fontsize=10)
    ax_all.grid(True, linestyle='--', alpha=0.6)
    fig_all.autofmt_xdate()
    plt.tight_layout()
    st.pyplot(fig_all)
else:
    st.write("Aucune donnée de performance disponible pour la comparaison.")

conn.close()
