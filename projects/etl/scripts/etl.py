# projects/etl/scripts/etl.py
import os
import pandas as pd
import numpy as np
import logging
import sys
from datetime import datetime, timedelta

# Configurazione percorsi
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')

RAW_FILE = 'train_transaction.csv'
PROCESSED_FILE = 'powerbi_ready_data.csv'
LOG_FILE = 'fraud_etl.log'

RAW_DATA_PATH = os.path.join(RAW_DIR, RAW_FILE)
PROCESSED_DATA_PATH = os.path.join(PROCESSED_DIR, PROCESSED_FILE)
LOG_PATH = os.path.join(LOGS_DIR, LOG_FILE)

# Setup cartelle
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Soglie importi
MIN_AMOUNT = 120
MAX_AMOUNT = 100000

# Parametri principali da regolare:
BASE_FRAUD_PROB = 0.01  # Probabilità base di frode
NIGHT_MULTIPLIER = 9.0   # Moltiplicatore per ore notturne locali (22-6)
MORNING_MULTIPLIER = 0.3 # Moltiplicatore per mattina locale (7-12)
DAY_MULTIPLIER = 0.7     # Moltiplicatore per pomeriggio/sera locale (13-21)

# Definizione "notte" locale (ore in cui aumentare le frodi)
NIGHT_START = 22  # 22:00 locali
NIGHT_END = 6     # 06:00 locali

# Definizione delle zone geografiche
COUNTRY_ZONES = {
    'North America': ['United States', 'Canada'],
    'Europe West': ['Germany', 'United Kingdom', 'France', 'Italy', 'Netherlands', 'Switzerland'],
    'Asia Pacific': ['China', 'Japan', 'India', 'South Korea', 'Australia', 'Indonesia'],
    'Latin America': ['Brazil', 'Mexico'],
    'Middle East': ['Saudi Arabia', 'Turkey', 'Poland', 'Russia']
}

# Dettaglio paesi con distribuzioni
COUNTRIES_DATA = [
    {'country': 'United States', 'population': 331_900_000, 'gdp_rank': 1, 'W': 0.37, 'C': 0.11, 'R': 0.24, 'H': 0.13, 'S': 0.15, 'zone': 'North America', 'gmt_offset': -5.0},
    {'country': 'China', 'population': 1_412_600_000, 'gdp_rank': 2, 'W': 0.43, 'C': 0.09, 'R': 0.21, 'H': 0.11, 'S': 0.16, 'zone': 'Asia Pacific', 'gmt_offset': 8.0},
    {'country': 'Japan', 'population': 125_700_000, 'gdp_rank': 3, 'W': 0.19, 'C': 0.26, 'R': 0.34, 'H': 0.13, 'S': 0.08, 'zone': 'Asia Pacific', 'gmt_offset': 9.0},
    {'country': 'Germany', 'population': 83_200_000, 'gdp_rank': 4, 'W': 0.24, 'C': 0.16, 'R': 0.33, 'H': 0.14, 'S': 0.13, 'zone': 'Europe West', 'gmt_offset': 1.0},
    {'country': 'India', 'population': 1_408_900_000, 'gdp_rank': 5, 'W': 0.14, 'C': 0.41, 'R': 0.11, 'H': 0.19, 'S': 0.15, 'zone': 'Asia Pacific', 'gmt_offset': 5.5},
    {'country': 'United Kingdom', 'population': 67_300_000, 'gdp_rank': 6, 'W': 0.31, 'C': 0.13, 'R': 0.26, 'H': 0.14, 'S': 0.16, 'zone': 'Europe West', 'gmt_offset': 0.0},
    {'country': 'France', 'population': 67_800_000, 'gdp_rank': 7, 'W': 0.29, 'C': 0.16, 'R': 0.24, 'H': 0.16, 'S': 0.15, 'zone': 'Europe West', 'gmt_offset': 1.0},
    {'country': 'Brazil', 'population': 213_500_000, 'gdp_rank': 8, 'W': 0.23, 'C': 0.31, 'R': 0.16, 'H': 0.21, 'S': 0.09, 'zone': 'Latin America', 'gmt_offset': -3.0},
    {'country': 'Italy', 'population': 59_100_000, 'gdp_rank': 9, 'W': 0.26, 'C': 0.21, 'R': 0.29, 'H': 0.14, 'S': 0.10, 'zone': 'Europe West', 'gmt_offset': 1.0},
    {'country': 'Canada', 'population': 38_000_000, 'gdp_rank': 10, 'W': 0.39, 'C': 0.09, 'R': 0.31, 'H': 0.11, 'S': 0.10, 'zone': 'North America', 'gmt_offset': -5.0},
    {'country': 'Russia', 'population': 143_400_000, 'gdp_rank': 11, 'W': 0.21, 'C': 0.24, 'R': 0.19, 'H': 0.23, 'S': 0.13, 'zone': 'Middle East', 'gmt_offset': 3.0},
    {'country': 'South Korea', 'population': 51_700_000, 'gdp_rank': 12, 'W': 0.32, 'C': 0.11, 'R': 0.29, 'H': 0.14, 'S': 0.14, 'zone': 'Asia Pacific', 'gmt_offset': 9.0},
    {'country': 'Australia', 'population': 25_700_000, 'gdp_rank': 13, 'W': 0.37, 'C': 0.11, 'R': 0.29, 'H': 0.13, 'S': 0.10, 'zone': 'Asia Pacific', 'gmt_offset': 10.0},
    {'country': 'Mexico', 'population': 128_900_000, 'gdp_rank': 14, 'W': 0.19, 'C': 0.34, 'R': 0.16, 'H': 0.19, 'S': 0.12, 'zone': 'Latin America', 'gmt_offset': -6.0},
    {'country': 'Indonesia', 'population': 275_500_000, 'gdp_rank': 15, 'W': 0.11, 'C': 0.44, 'R': 0.12, 'H': 0.22, 'S': 0.11, 'zone': 'Asia Pacific', 'gmt_offset': 7.0},
    {'country': 'Netherlands', 'population': 17_500_000, 'gdp_rank': 16, 'W': 0.34, 'C': 0.13, 'R': 0.24, 'H': 0.16, 'S': 0.13, 'zone': 'Europe West', 'gmt_offset': 1.0},
    {'country': 'Saudi Arabia', 'population': 35_000_000, 'gdp_rank': 17, 'W': 0.26, 'C': 0.23, 'R': 0.19, 'H': 0.19, 'S': 0.13, 'zone': 'Middle East', 'gmt_offset': 3.0},
    {'country': 'Turkey', 'population': 84_800_000, 'gdp_rank': 18, 'W': 0.18, 'C': 0.36, 'R': 0.14, 'H': 0.19, 'S': 0.13, 'zone': 'Middle East', 'gmt_offset': 3.0},
    {'country': 'Switzerland', 'population': 8_700_000, 'gdp_rank': 19, 'W': 0.16, 'C': 0.19, 'R': 0.39, 'H': 0.11, 'S': 0.15, 'zone': 'Europe West', 'gmt_offset': 1.0},
    {'country': 'Poland', 'population': 37_800_000, 'gdp_rank': 20, 'W': 0.24, 'C': 0.26, 'R': 0.19, 'H': 0.19, 'S': 0.12, 'zone': 'Europe West', 'gmt_offset': 1.0}
]

# Provider email globali
EMAIL_PROVIDERS = {
    'gmail': 35.2,
    'icloud': 19.7,
    'outlook': 12.6,
    'yahoo': 8.4,
    'other': 24.1
}

# Distribuzione carte per zona
CARD_DISTRIBUTION = {
    'North America': {'Credit': 68, 'Debit': 28, 'Prepaid': 4},
    'Europe West': {'Credit': 55, 'Debit': 40, 'Prepaid': 5},
    'Asia Pacific': {'Credit': 30, 'Debit': 65, 'Prepaid': 5},
    'Latin America': {'Credit': 25, 'Debit': 70, 'Prepaid': 5},
    'Middle East': {'Credit': 35, 'Debit': 60, 'Prepaid': 5}
}

def generate_fraud_based_on_local_hour(transaction_time, gmt_offset):
    """Genera frodi con probabilità maggiore nelle ore notturne LOCALI"""
    local_hour = (transaction_time.hour + int(gmt_offset)) % 24
    
    # Aumentiamo significativamente il differenziale
    if 22 <= local_hour or local_hour <= 5:  # Notte fonda (22:00-05:59)
        fraud_prob = 0.12  # 12% di probabilità
    elif 6 <= local_hour <= 9:  # Mattina (06:00-09:59)
        fraud_prob = 0.04  # 4%
    elif 10 <= local_hour <= 17:  # Giorno (10:00-17:59)
        fraud_prob = 0.01  # 1%
    else:  # Sera (18:00-21:59)
        fraud_prob = 0.03  # 3%
    
    return np.random.choice([True, False], p=[fraud_prob, 1-fraud_prob])

def verify_file_structure():
    """Verifica la struttura delle cartelle"""
    if not os.path.exists(RAW_DATA_PATH):
        logger.error(f"File non trovato: {RAW_DATA_PATH}")
        logger.info("""
        Soluzioni:
        1. Posiziona il file 'train_transaction.csv' in: data/raw/
        2. Controlla il nome del file
        """)
        return False
    return True

def extract_data():
    """Estrae i campi necessari"""
    try:
        logger.info("Caricamento dati in corso...")
        cols_to_load = [
            'isFraud', 'TransactionAmt', 'TransactionDT',
            'card2', 'card4', 'addr1', 'addr2', 'P_emaildomain'
        ]
        
        df = pd.read_csv(RAW_DATA_PATH, usecols=cols_to_load)
        
        # Verifica colonne obbligatorie
        required_cols = {'isFraud', 'TransactionAmt'}
        if not required_cols.issubset(df.columns):
            missing = required_cols - set(df.columns)
            raise ValueError(f"Colonne mancanti: {missing}")
            
        return df
    
    except Exception as e:
        logger.error(f"Errore durante il caricamento: {str(e)}")
        raise

def generate_country_weights():
    """Calcola i pesi per la distribuzione delle transazioni per paese"""
    total_pop = sum(c['population'] for c in COUNTRIES_DATA)
    country_weights = []
    
    for country in COUNTRIES_DATA:
        # Peso basato su popolazione e inverso del rank PIL (più alto il PIL, più peso)
        weight = (country['population'] / total_pop) * (1 / np.log(country['gdp_rank'] + 1))
        country_weights.append({
            'country': country['country'],
            'weight': weight,
            'zone': country['zone'],
            'products': {
                'W': country['W'],
                'C': country['C'],
                'R': country['R'],
                'H': country['H'],
                'S': country['S']
            }
        })
    
    # Normalizza i pesi
    total_weight = sum(c['weight'] for c in country_weights)
    for country in country_weights:
        country['prob'] = country['weight'] / total_weight
    
    return country_weights

def get_email_provider(country_name):
    """Assegna provider email in modo realistico"""
    # Trova la zona del paese
    zone = next((c['zone'] for c in COUNTRIES_DATA if c['country'] == country_name), 'other')
    
    # Personalizzazione per zona
    if zone == 'North America':
        providers = {'gmail': 40, 'icloud': 30, 'outlook': 15, 'yahoo': 10, 'other': 5}
    elif zone == 'Europe West':
        providers = {'gmail': 35, 'icloud': 15, 'outlook': 25, 'yahoo': 5, 'other': 20}
    else:
        providers = EMAIL_PROVIDERS
    
    # Converti in probabilità
    total = sum(providers.values())
    probs = {k: v/total for k, v in providers.items()}
    
    return np.random.choice(list(probs.keys()), p=list(probs.values()))

def get_card_type(zone):
    """Assegna tipo carta in base alla zona"""
    card_probs = CARD_DISTRIBUTION.get(zone, {'Credit': 40, 'Debit': 55, 'Prepaid': 5})
    total = sum(card_probs.values())
    probs = {k: v/total for k, v in card_probs.items()}
    return np.random.choice(list(probs.keys()), p=list(probs.values()))

def transform_data(df):
    """Applica tutte le trasformazioni"""
    try:
        # Filtro per range di importo
        df = df[(df['TransactionAmt'] >= MIN_AMOUNT) & 
                (df['TransactionAmt'] <= MAX_AMOUNT)].copy()
        
        # 1. Genera pesi paese (deve venire prima per avere i GMT offset)
        country_weights = generate_country_weights()
        countries = [c['country'] for c in country_weights]
        country_probs = [c['prob'] for c in country_weights]
        
        # 2. Assegna provenienza in base a pesi
        df['provenienza'] = np.random.choice(
            countries, 
            size=len(df),
            p=country_probs
        )
        
        # 3. Normalizza date
        df['TransactionDT'] = pd.to_datetime(df['TransactionDT'], unit='s')
        latest_date = df['TransactionDT'].max()
        target_date = pd.to_datetime('2024-12-31')
        delta = target_date - latest_date
        df['TransactionDate'] = df['TransactionDT'] + delta
        
        # 4. Genera frodi in base all'ora LOCALE
        # Creiamo un dizionario paese -> offset
        country_to_offset = {c['country']: c['gmt_offset'] for c in COUNTRIES_DATA}
        
        # Applica la funzione considerando l'offset
        df['isFraud'] = df.apply(
            lambda row: generate_fraud_based_on_local_hour(
                row['TransactionDate'],
                country_to_offset[row['provenienza']]
            ),
            axis=1
        )
        
        # 5. Assegna ProductCD in base alla distribuzione paese
        def assign_product(row):
            country = next((c for c in country_weights if c['country'] == row['provenienza']), None)
            if country:
                products = ['W', 'C', 'R', 'H', 'S']
                probs = [country['products'][p] for p in products]
                return np.random.choice(products, p=probs)
            return np.random.choice(['W', 'C', 'R', 'H', 'S'], p=[0.25, 0.25, 0.2, 0.2, 0.1])
        
        df['ProductCD'] = df.apply(assign_product, axis=1)
        
        # 6. Assegna provider email
        df['email_provider'] = df['provenienza'].apply(get_email_provider)
        
        # 7. Assegna tipo carta
        def get_zone(country):
            return next((c['zone'] for c in country_weights if c['country'] == country), 'other')
        
        df['tipo_carta'] = df['provenienza'].apply(get_zone).apply(get_card_type)
        
        # 8. Brand carta
        df['brand_carta'] = df['card4'].str.title().fillna('Unknown')
        
        # Distribuzione realistica per i valori Unknown
        def assign_unknown_brand(row):
            if row['brand_carta'] == 'Unknown':
                if row['tipo_carta'] == 'Credit':
                    return np.random.choice(['Visa', 'Mastercard', 'Amex'], p=[0.6, 0.35, 0.05])
                else:
                    return np.random.choice(['Visa', 'Mastercard'], p=[0.7, 0.3])
            return row['brand_carta']
        
        df['brand_carta'] = df.apply(assign_unknown_brand, axis=1)
        
        # 9. Normalizza date
        df['TransactionDT'] = pd.to_datetime(df['TransactionDT'], unit='s')
        latest_date = df['TransactionDT'].max()
        target_date = pd.to_datetime('2024-12-31')
        delta = target_date - latest_date
        df['TransactionDate'] = df['TransactionDT'] + delta
        
        return df
    
    except Exception as e:
        logger.error(f"Errore durante la trasformazione: {str(e)}")
        raise

def analyze_data(df):
    """Genera report statistici ampliato"""
    # Aggiungiamo l'ora locale all'analisi
    country_to_offset = {c['country']: c['gmt_offset'] for c in COUNTRIES_DATA}
    
    df['local_hour'] = df.apply(
        lambda row: (row['TransactionDate'].hour + int(country_to_offset[row['provenienza']])) % 24,
        axis=1
    )
    
    # Statistiche per fascia oraria locale
    hour_stats = df.groupby('local_hour').agg(
        Transazioni=('isFraud', 'count'),
        Frodi=('isFraud', 'sum'),
        Perc_Frodi=('isFraud', lambda x: round(x.mean() * 100, 2))
    ).sort_values('Perc_Frodi', ascending=False)
    
    logger.info("\nDistribuzione frodi per ora locale (considerando GMT offset):")
    logger.info("\n" + hour_stats.to_string())
    
    # Statistiche per provider email
    provider_stats = df.groupby('email_provider').agg(
        Transazioni=('isFraud', 'count'),
        Frodi=('isFraud', 'sum'),
        Perc_Frodi=('isFraud', lambda x: round(x.mean() * 100, 2))
    ).sort_values('Transazioni', ascending=False)
    
    logger.info("\nDistribuzione provider email:")
    logger.info("\n" + provider_stats.to_string())
    
    # Statistiche per tipo carta e brand
    card_stats = df.groupby(['tipo_carta', 'brand_carta']).agg(
        Transazioni=('isFraud', 'count'),
        Frodi=('isFraud', 'sum')
    ).sort_values('Transazioni', ascending=False)
    
    logger.info("\nDistribuzione carte di pagamento:")
    logger.info("\n" + card_stats.to_string())
    
    return df

def save_data(df):
    """Salva i dati processati in UTF-8"""
    try:
        output_cols = [
            'isFraud', 'TransactionAmt', 'ProductCD',
            'provenienza', 'email_provider',
            'TransactionDate', 'tipo_carta', 'brand_carta'
        ]
        
        # Salva in UTF-8 con BOM (compatibile con Excel e PostgreSQL)
        df[output_cols].to_csv(PROCESSED_DATA_PATH, index=False, encoding='utf-8-sig')
        
        logger.info(f"File CSV salvato in UTF-8: {PROCESSED_DATA_PATH}")
    except Exception as e:
        logger.error(f"Errore durante il salvataggio: {str(e)}")
        raise

def main():
    try:
        logger.info("=== INIZIO ELABORAZIONE ===")
        
        if not verify_file_structure():
            sys.exit(1)
            
        raw_data = extract_data()
        transformed_data = transform_data(raw_data)
        analyzed_data = analyze_data(transformed_data)
        save_data(analyzed_data)
        
        logger.info("=== ELABORAZIONE COMPLETATA CON SUCCESSO ===")
    
    except Exception as e:
        logger.error(f"ERRORE CRITICO: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()