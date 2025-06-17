# projects/etl/scripts/etl_final.py
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

# Soglie importi
MIN_AMOUNT = 120
MAX_AMOUNT = 100000

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
    {'country': 'United States', 'population': 331_900_000, 'gdp_rank': 1, 'W': 0.42, 'C': 0.12, 'R': 0.28, 'H': 0.14, 'S': 0.04, 'zone': 'North America'},
    {'country': 'China', 'population': 1_412_600_000, 'gdp_rank': 2, 'W': 0.38, 'C': 0.15, 'R': 0.30, 'H': 0.12, 'S': 0.05, 'zone': 'Asia Pacific'},
    {'country': 'Japan', 'population': 125_700_000, 'gdp_rank': 3, 'W': 0.21, 'C': 0.19, 'R': 0.41, 'H': 0.11, 'S': 0.08, 'zone': 'Asia Pacific'},
    {'country': 'Germany', 'population': 83_200_000, 'gdp_rank': 4, 'W': 0.23, 'C': 0.18, 'R': 0.40, 'H': 0.12, 'S': 0.07, 'zone': 'Europe West'},
    {'country': 'India', 'population': 1_408_900_000, 'gdp_rank': 5, 'W': 0.11, 'C': 0.48, 'R': 0.12, 'H': 0.24, 'S': 0.05, 'zone': 'Asia Pacific'},
    {'country': 'United Kingdom', 'population': 67_300_000, 'gdp_rank': 6, 'W': 0.35, 'C': 0.15, 'R': 0.25, 'H': 0.18, 'S': 0.07, 'zone': 'Europe West'},
    {'country': 'France', 'population': 67_800_000, 'gdp_rank': 7, 'W': 0.32, 'C': 0.17, 'R': 0.28, 'H': 0.16, 'S': 0.07, 'zone': 'Europe West'},
    {'country': 'Brazil', 'population': 213_500_000, 'gdp_rank': 8, 'W': 0.28, 'C': 0.25, 'R': 0.20, 'H': 0.20, 'S': 0.07, 'zone': 'Latin America'},
    {'country': 'Italy', 'population': 59_100_000, 'gdp_rank': 9, 'W': 0.30, 'C': 0.20, 'R': 0.25, 'H': 0.17, 'S': 0.08, 'zone': 'Europe West'},
    {'country': 'Canada', 'population': 38_000_000, 'gdp_rank': 10, 'W': 0.41, 'C': 0.11, 'R': 0.29, 'H': 0.14, 'S': 0.05, 'zone': 'North America'},
    {'country': 'Russia', 'population': 143_400_000, 'gdp_rank': 11, 'W': 0.25, 'C': 0.30, 'R': 0.15, 'H': 0.22, 'S': 0.08, 'zone': 'Middle East'},
    {'country': 'South Korea', 'population': 51_700_000, 'gdp_rank': 12, 'W': 0.33, 'C': 0.12, 'R': 0.35, 'H': 0.13, 'S': 0.07, 'zone': 'Asia Pacific'},
    {'country': 'Australia', 'population': 25_700_000, 'gdp_rank': 13, 'W': 0.40, 'C': 0.10, 'R': 0.30, 'H': 0.15, 'S': 0.05, 'zone': 'Asia Pacific'},
    {'country': 'Mexico', 'population': 128_900_000, 'gdp_rank': 14, 'W': 0.22, 'C': 0.35, 'R': 0.15, 'H': 0.20, 'S': 0.08, 'zone': 'Latin America'},
    {'country': 'Indonesia', 'population': 275_500_000, 'gdp_rank': 15, 'W': 0.09, 'C': 0.52, 'R': 0.10, 'H': 0.23, 'S': 0.06, 'zone': 'Asia Pacific'},
    {'country': 'Netherlands', 'population': 17_500_000, 'gdp_rank': 16, 'W': 0.36, 'C': 0.14, 'R': 0.26, 'H': 0.17, 'S': 0.07, 'zone': 'Europe West'},
    {'country': 'Saudi Arabia', 'population': 35_000_000, 'gdp_rank': 17, 'W': 0.27, 'C': 0.28, 'R': 0.18, 'H': 0.19, 'S': 0.08, 'zone': 'Middle East'},
    {'country': 'Turkey', 'population': 84_800_000, 'gdp_rank': 18, 'W': 0.20, 'C': 0.40, 'R': 0.15, 'H': 0.18, 'S': 0.07, 'zone': 'Middle East'},
    {'country': 'Switzerland', 'population': 8_700_000, 'gdp_rank': 19, 'W': 0.19, 'C': 0.21, 'R': 0.39, 'H': 0.12, 'S': 0.09, 'zone': 'Europe West'},
    {'country': 'Poland', 'population': 37_800_000, 'gdp_rank': 20, 'W': 0.26, 'C': 0.25, 'R': 0.20, 'H': 0.21, 'S': 0.08, 'zone': 'Middle East'}
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
        
        # 1. Genera pesi paese
        country_weights = generate_country_weights()
        countries = [c['country'] for c in country_weights]
        country_probs = [c['prob'] for c in country_weights]
        
        # 2. Assegna provenienza in base a pesi
        df['provenienza'] = np.random.choice(
            countries, 
            size=len(df),
            p=country_probs
        )
        
        # 3. Assegna ProductCD in base alla distribuzione paese
        def assign_product(row):
            country = next((c for c in country_weights if c['country'] == row['provenienza']), None)
            if country:
                products = ['W', 'C', 'R', 'H', 'S']
                probs = [country['products'][p] for p in products]
                return np.random.choice(products, p=probs)
            return np.random.choice(['W', 'C', 'R', 'H', 'S'], p=[0.25, 0.25, 0.2, 0.2, 0.1])
        
        df['ProductCD'] = df.apply(assign_product, axis=1)
        
        # 4. Assegna provider email
        df['email_provider'] = df['provenienza'].apply(get_email_provider)
        
        # 5. Assegna tipo carta
        def get_zone(country):
            return next((c['zone'] for c in country_weights if c['country'] == country), 'other')
        
        df['tipo_carta'] = df['provenienza'].apply(get_zone).apply(get_card_type)
        
        # 6. Brand carta (LOGICA ORIGINALE MIGLIORATA)
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
        
        # 7. Normalizza date
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
    stats = {
        'Transazioni totali': len(df),
        'Frodi totali': df['isFraud'].sum(),
        '% Frodi': round(df['isFraud'].mean() * 100, 2),
        'Min importo': df['TransactionAmt'].min(),
        'Max importo': df['TransactionAmt'].max(),
        'Prima data': df['TransactionDate'].min(),
        'Ultima data': df['TransactionDate'].max()
    }
    
    # Statistiche per provenienza
    country_stats = df.groupby('provenienza').agg(
        Transazioni=('isFraud', 'count'),
        Frodi=('isFraud', 'sum'),
        Perc_Frodi=('isFraud', lambda x: round(x.mean() * 100, 2))
    ).sort_values('Transazioni', ascending=False)
    
    # Statistiche per provider email
    provider_stats = df.groupby('email_provider').agg(
        Transazioni=('isFraud', 'count'),
        Frodi=('isFraud', 'sum'),
        Perc_Frodi=('isFraud', lambda x: round(x.mean() * 100, 2))
    ).sort_values('Transazioni', ascending=False)
    
    # Statistiche per tipo carta e brand
    card_stats = df.groupby(['tipo_carta', 'brand_carta']).agg(
        Transazioni=('isFraud', 'count'),
        Frodi=('isFraud', 'sum')
    ).sort_values('Transazioni', ascending=False)
    
    logger.info("\n=== STATISTICHE ===")
    for k, v in stats.items():
        logger.info(f"{k}: {v}")
    
    logger.info("\nTop 10 paesi per transazioni:")
    logger.info(country_stats.head(10).to_string())
    
    logger.info("\nDistribuzione provider email:")
    logger.info(provider_stats.to_string())
    
    logger.info("\nDistribuzione carte di pagamento:")
    logger.info(card_stats.to_string())
    
    return df

def save_data(df):
    """Salva i dati processati"""
    try:
        output_cols = [
            'isFraud', 'TransactionAmt', 'ProductCD',
            'provenienza', 'email_provider',
            'TransactionDate', 'tipo_carta', 'brand_carta'
        ]
        
        df[output_cols].to_csv(PROCESSED_DATA_PATH, index=False)
        logger.info(f"Dati salvati in {PROCESSED_DATA_PATH}")
        
        logger.info("\nAnteprima dati salvati:")
        logger.info(df[output_cols].head().to_string())
    
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