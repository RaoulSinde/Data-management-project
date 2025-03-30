import pandas as pd
import sqlite3
from datetime import date
from faker import Faker
import json
import random
import data_collector as dc

# Initialiser Faker pour générer des données fictives
faker = Faker()

# Définir le nom du fichier de la base de données
project_database = "project_database.db"

# Importer les dictionnaires depuis le module data_collector
dict_products = dc.dict_products
dict_risk_type = dc.dict_risk_type

# Fonction pour créer une table dans la base de données SQLite
def create_table(create_table_query, table, database_name=project_database):
    try:
        # Connexion à la base de données SQLite
        conn = sqlite3.connect(database_name)
        cursor = conn.cursor()

        # Création de la table
        cursor.execute(create_table_query)
        print(f"Table {table} créée avec succès.")
        conn.commit()

    except sqlite3.Error as e:
        print(f"Erreur SQLite : {e}")

    finally:
        # Fermeture de la connexion
        if conn:
            conn.close()

# Classe Client pour gérer les clients
class Client:
    def __init__(self, name: str, first_name: str, birth_date: str, address: str, phone_number: str, email: str, entry_date: str, risk_profile: str):
        self.name = name
        self.first_name = first_name
        self.birth_date = birth_date
        self.address = address
        self.phone_number = phone_number
        self.email = email
        self.entry_date = entry_date
        self.risk_profile = risk_profile

    def clients_to_base(self, database=project_database):
        if self.risk_profile not in ["low_risk", "low_turnover", "high_yield_equity_only"]:
            raise ValueError(f"Profil de risque '{self.risk_profile}' non valide!")

        client_data = []
        try:
            # Connexion à la base de données SQLite
            conn = sqlite3.connect(database)
            cursor = conn.cursor()
            # Insertion des données dans la table Clients
            insert_query = """
            INSERT INTO Clients (name, first_name, birth_date, address, phone_number, email, entry_date, risk_profile)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """
            client_data.append((self.name, self.first_name, self.birth_date, self.address, self.phone_number, self.email, self.entry_date, self.risk_profile))
            cursor.executemany(insert_query, client_data)
            conn.commit()
            print(f"{self.name} clients insérés avec succès dans la table 'Clients'.")

        except sqlite3.Error as e:
            print(f"Erreur SQLite : {e}")

        finally:
            # Fermeture de la connexion
            if conn:
                conn.close()

# Fonction pour peupler la base de données avec des clients fictifs
def pop_clients_base(dict=dict_risk_type):
    for risk_profile in list(set(dict.values())):
        client_1 = Client(
            faker.last_name(),
            faker.first_name(),
            faker.date_of_birth(minimum_age=18, maximum_age=80).strftime('%Y-%m-%d'),  # Convertir en format chaîne
            faker.address().replace("\n", ", "),  # Remplacer les sauts de ligne pour la sécurité de la base de données
            faker.phone_number(),
            faker.email(),
            faker.date_between(start_date=date(2023, 1, 1), end_date=date(2024, 12, 31)).strftime('%Y-%m-%d'),  # Convertir en chaîne
            risk_profile
        )
        client_1.clients_to_base()

# Classe Products pour gérer les produits
class Products:
    def __init__(self, ticker: str, product_risk_profile: str, name: str):
        self.ticker = ticker
        self.product_risk_profile = product_risk_profile
        self.name = name

    def products_to_base(self, database=project_database):
        if self.product_risk_profile not in ["low_risk", "low_turnover", "high_yield_equity_only"]:
            raise ValueError(f"Profil de risque '{self.product_risk_profile}' non valide!")
        product_data = []
        try:
            conn = sqlite3.connect(database)
            cursor = conn.cursor()

            insert_query = """INSERT INTO Products (ticker, product_risk_profile, name) VALUES (?, ?, ?);"""
            product_data.append((self.ticker, self.product_risk_profile, self.name))
            cursor.executemany(insert_query, product_data)
            conn.commit()
            print(f'Produit {self.ticker} inséré en base')
        except sqlite3.Error as e:
            print(f"Erreur SQLite : {e}")

        finally:
            # Fermeture de la connexion
            if conn:
                conn.close()

# Fonction pour peupler la base de données avec des produits fictifs
def pop_products_base(dict_prod=dict_products, dict_risk_profile=dict_risk_type):
    for ticker in list(dict_prod.keys()):
        product = Products(ticker, dict_risk_profile[ticker], dict_prod[ticker])
        product.products_to_base()

# Classe Wallet pour gérer les portefeuilles
class Wallet:
    def __init__(self, wallet_name: str, risk_profile: str, products: list):
        self.wallet_name = wallet_name
        self.risk_profile = risk_profile
        self.products = products

    def wallet_to_base(self, database=project_database):
        # Sérialiser la liste des produits en une chaîne JSON
        products_json = json.dumps(self.products)  # Convertir la liste des produits au format JSON

        try:
            # Connexion à la base de données SQLite
            conn = sqlite3.connect(database)
            cursor = conn.cursor()

            # Insertion des données dans la table Portfolios
            insert_query = """
            INSERT INTO Portfolios (wallet_name, risk_profile, products)
            VALUES (?, ?, ?);
            """
            cursor.execute(insert_query, (self.wallet_name, self.risk_profile, products_json))
            conn.commit()
            print(f"Portfolio '{self.wallet_name}' ajouté avec succès dans la table 'Portfolios'.")

        except sqlite3.Error as e:
            print(f"Erreur SQLite : {e}")

        finally:
            # Fermeture de la connexion
            if conn:
                conn.close()

# Fonction pour obtenir les tickers par profil de risque
def get_tickers_by_risk_profile(database_name=project_database):
    try:
        # Connexion à la base de données SQLite
        conn = sqlite3.connect(database_name)
        cursor = conn.cursor()

        # Exécution de la requête pour obtenir uniquement les tickers groupés par profil de risque
        query = "SELECT product_risk_profile, product_id FROM Products"
        cursor.execute(query)

        # Récupérer tous les résultats
        products = cursor.fetchall()

        # Organiser les tickers par profil de risque
        tickers_by_risk_profile = {}
        for product in products:
            risk_profile, ticker = product
            if risk_profile not in tickers_by_risk_profile:
                tickers_by_risk_profile[risk_profile] = []
            tickers_by_risk_profile[risk_profile].append(ticker)

        return tickers_by_risk_profile

    except sqlite3.Error as e:
        print(f"Erreur SQLite : {e}")
        return {}

    finally:
        # Fermeture de la connexion
        if conn:
            conn.close()

# Fonction pour peupler les portefeuilles
def populate_wallets(dict):  # dict=get_tickers_by_risk_profile()
    tickers_by_risk_profile = dict  # Cette fonction récupère les tickers groupés par profil de risque

    # Parcourir chaque profil de risque et créer des portefeuilles
    for risk_profile, tickers in tickers_by_risk_profile.items():
        wallet_name = f"Portfolio_{risk_profile}"  # Exemple de nom de portefeuille
        wallet = Wallet(wallet_name=wallet_name, risk_profile=risk_profile, products=tickers)
        wallet.wallet_to_base()

# Classe Manager pour gérer les gestionnaires
class Manager:
    def __init__(self, manager_name: str, email: str, wallets_managed_id: int):
        self.manager_name = manager_name
        self.email = email
        self.wallets_managed = wallets_managed_id

    def manager_to_base(self, database=project_database):
        try:
            conn = sqlite3.connect(database)
            cursor = conn.cursor()

            # Insérer les données du gestionnaire dans la table Managers
            insert_query = """
            INSERT INTO Managers (manager_name, email, wallets_managed_id)
            VALUES (?, ?, ?);
            """
            cursor.execute(insert_query, (self.manager_name, self.email, self.wallets_managed))
            conn.commit()
            print(f"Manager '{self.manager_name}' ajouté à la table 'Managers'.")
        except sqlite3.Error as e:
            print(f"Erreur SQLite : {e}")
        finally:
            if conn:
                conn.close()

# Fonction pour obtenir les identifiants des portefeuilles
def get_wallet_id(database=project_database):
    try:
        conn = sqlite3.connect(database)
        cursor = conn.cursor()
        select_query = """SELECT wallet_id FROM Portfolios"""
        cursor.execute(select_query)

        # Récupérer tous les wallet_ids et les retourner sous forme de liste
        result = cursor.fetchall()

        # Extraire les wallet_ids des résultats et les retourner sous forme de liste
        wallet_ids = [wallet_id[0] for wallet_id in result]
        conn.close()

    except sqlite3.Error as e:
        raise RuntimeError(f"Erreur SQLite : {e}")

    return wallet_ids

# Fonction pour peupler la base de données avec des gestionnaires fictifs
def pop_manager_base(list_1, database=project_database):  # list_1=get_wallet_id()
    try:
        # Obtenir tous les identifiants des portefeuilles
        wallet_ids = list_1
        if not wallet_ids:
            raise ValueError("Aucun portefeuille trouvé dans la table 'Portfolios'!")

        # Créer un gestionnaire par identifiant de portefeuille (correspondance 1:1 entre gestionnaire et portefeuille)
        for wallet_id in wallet_ids:
            manager = Manager(faker.name(), faker.email(), wallet_id)  # Un portefeuille par gestionnaire
            manager.manager_to_base(database)

    except Exception as e:
        print(f"Erreur lors du peuplement de la base des gestionnaires : {e}")

# Classe Deal pour gérer les transactions
class Deal:
    def __init__(self, date: str, wallet_id: int, manager_id: int, product_id: int, qty: int):
        self.date = date
        self.wallet_id = wallet_id
        self.manager_id = manager_id
        self.product_id = product_id
        self.qty = qty

    def deal_to_base(self, database=project_database):
        try:
            conn = sqlite3.connect(database)
            cursor = conn.cursor()

            # Insérer les données de la transaction dans la table Deals
            insert_query = """
            INSERT INTO Deals (date, wallet_id, manager_id, product_id, qty)
            VALUES (?, ?, ?, ?, ?);
            """
            cursor.execute(insert_query, (self.date, self.wallet_id, self.manager_id, self.product_id, self.qty))
            conn.commit()
            print(f"Transaction du gestionnaire {self.manager_id} et sur son portefeuille {self.wallet_id} ajoutée à la table")
        except sqlite3.Error as e:
            print(f"Erreur SQLite : {e}")
        finally:
            if conn:
                conn.close()

# Fonction pour récupérer les identifiants des produits
def fetch_product_ids(database=project_database):
    """
    Récupérer les identifiants des produits pour chaque ticker depuis la table Products.
    Suppose que la table 'Products' a les colonnes 'product_id' et 'ticker'.
    """
    try:
        conn = sqlite3.connect(database)
        cursor = conn.cursor()

        cursor.execute("SELECT product_id, ticker FROM Products")
        product_dict = {ticker: product_id for product_id, ticker in cursor.fetchall()}

        conn.close()
        return product_dict

    except sqlite3.Error as e:
        print(f"Erreur SQLite lors de la récupération des identifiants des produits : {e}")
        return {}

# Fonction pour récupérer les noms des produits
def fetch_product_name(database=project_database):
    """
    Récupérer les noms des produits pour chaque ticker depuis la table Products.
    Suppose que la table 'Products' a les colonnes 'name' et 'ticker'.
    """
    try:
        conn = sqlite3.connect(database)
        cursor = conn.cursor()

        cursor.execute("SELECT name, ticker FROM Products")
        product_dict = {product_name: ticker for product_name, ticker in cursor.fetchall()}

        conn.close()
        return product_dict

    except sqlite3.Error as e:
        print(f"Erreur SQLite lors de la récupération des noms des produits : {e}")
        return {}

# Fonction pour peupler la table des rendements
def populate_returns_table(dict_product_id, dict_product_name, returns_df, database=project_database):
    """
    Peupler la table Returns en utilisant le DataFrame produit par main().
    """
    try:
        conn = sqlite3.connect(database)
        cursor = conn.cursor()
        insert_data = []
        for date, row in returns_df.iterrows():
            date = date.strftime('%Y-%m-%d')
            print(f"Traitement de la date : {date}")  # Débogage
            for product_name, return_value in row.items():
                print(product_name)
                ticker = dict_product_name.get(product_name)
                product_id = dict_product_id.get(ticker)  # Obtenir l'identifiant du produit pour le ticker
                if product_id is not None:
                    insert_data.append((product_id, ticker, date, return_value))

        if insert_data:
            insert_query = """
            INSERT INTO Returns (product_id, ticker, date, return_value)
            VALUES (?, ?, ?, ?)
            """
            cursor.executemany(insert_query, insert_data)
            conn.commit()
            print(f"{len(insert_data)} lignes insérées dans la table Returns.")
        else:
            print("Aucune donnée valide à insérer.")

    except sqlite3.Error as e:
        print(f"Erreur SQLite lors de l'insertion des données de rendements : {e}")

    finally:
        conn.close()

# Requêtes SQL pour créer les tables
create_clients_query = """
CREATE TABLE IF NOT EXISTS Clients (
    client_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    first_name TEXT,
    birth_date DATE,
    address TEXT,
    phone_number TEXT,
    email TEXT,
    entry_date DATE,
    risk_profile TEXT
);
"""
create_products_query = """
CREATE TABLE IF NOT EXISTS Products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT UNIQUE,
    product_risk_profile TEXT,
    name TEXT
);
"""
create_wallet_query = """
CREATE TABLE IF NOT EXISTS Portfolios (
    wallet_id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_name TEXT,
    risk_profile TEXT,
    products TEXT DEFAULT '[]'
);
"""
create_managers_query = """
CREATE TABLE IF NOT EXISTS Managers (
    manager_id INTEGER PRIMARY KEY AUTOINCREMENT,
    manager_name TEXT,
    email TEXT,
    wallets_managed_id INTEGER,
    FOREIGN KEY (wallets_managed_id) REFERENCES Portfolios(wallet_id)
);
"""

create_returns_query = """
CREATE TABLE Returns (
    id_return INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER,
    ticker TEXT,
    date DATE,
    return_value REAL,
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);
"""
create_deals_query = """
CREATE TABLE IF NOT EXISTS Deals (
    deal_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE,
    wallet_id INTEGER,
    manager_id INTEGER,
    product_id INTEGER,
    qty INTEGER,
    FOREIGN KEY (wallet_id) REFERENCES Portfolios(wallet_id),
    FOREIGN KEY (manager_id) REFERENCES Managers(manager_id)
);
"""

# Fonction principale pour exécuter le script
def main(database=project_database, clients_query=create_clients_query, products_query=create_products_query,
         wallets_query=create_wallet_query, managers_query=create_managers_query, deals_query=create_deals_query,
         returns_query=create_returns_query, dict_prod=dict_products, dict_risk_profile=dict_risk_type):
    returns_data = dc.main()

    # Création des tables
    create_table(clients_query, "clients", database)
    create_table(products_query, "produits", database)
    create_table(wallets_query, "portfolios", database)
    create_table(managers_query, "managers", database)
    create_table(deals_query, "deals", database)
    create_table(returns_query, "returns", database)
    # Peuplement des bases de données
    pop_clients_base(dict_risk_profile)
    pop_products_base(dict_prod, dict_risk_profile)
    populate_wallets(get_tickers_by_risk_profile())
    pop_manager_base(get_wallet_id())
    populate_returns_table(dict_product_id=fetch_product_ids(), dict_product_name=fetch_product_name(), returns_df=returns_data, database=database)

# Exécution de la fonction principale
if __name__ == "__main__":
    main()
