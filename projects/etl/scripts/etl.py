# projects/etl/scripts/etl_final.py
import os
import pandas as pd
import logging
import sys

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
MIN_AMOUNT = 100
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
    """Estrae i 3 campi obbligatori"""
    try:
        logger.info("Caricamento dati in corso...")
        df = pd.read_csv(RAW_DATA_PATH, usecols=['isFraud', 'ProductCD', 'TransactionAmt'])
        
        # Verifica colonne obbligatorie
        required_cols = {'isFraud', 'ProductCD', 'TransactionAmt'}
        if not required_cols.issubset(df.columns):
            missing = required_cols - set(df.columns)
            raise ValueError(f"Colonne mancanti: {missing}")
            
        return df
    
    except Exception as e:
        logger.error(f"Errore durante il caricamento: {str(e)}")
        raise

def filter_data(df):
    """Applica i filtri essenziali"""
    try:
        # Filtro per range di importo
        df = df[(df['TransactionAmt'] >= MIN_AMOUNT) & 
                (df['TransactionAmt'] <= MAX_AMOUNT)].copy()
        
        # Verifica tutte e 5 le categorie
        valid_products = {'W', 'H', 'C', 'R', 'S'}
        missing_products = valid_products - set(df['ProductCD'].unique())
        
        if missing_products:
            logger.warning(f"Categorie prodotto mancanti: {missing_products}")
        
        return df
    
    except Exception as e:
        logger.error(f"Errore durante il filtraggio: {str(e)}")
        raise

def analyze_data(df):
    """Genera report statistici"""
    stats = {
        'Transazioni totali': len(df),
        'Frodi totali': df['isFraud'].sum(),
        '% Frodi': round(df['isFraud'].mean() * 100, 2),
        'Min importo': df['TransactionAmt'].min(),
        'Max importo': df['TransactionAmt'].max()
    }
    
    product_stats = df.groupby('ProductCD').agg(
        Transazioni=('isFraud', 'count'),
        Frodi=('isFraud', 'sum'),
        Perc_Frodi=('isFraud', lambda x: round(x.mean() * 100, 2))
    )
    
    logger.info("\n=== STATISTICHE ===")
    for k, v in stats.items():
        logger.info(f"{k}: {v}")
    
    logger.info("\nDistribuzione per categoria:")
    logger.info(product_stats.to_string())
    
    return df

def save_data(df):
    """Salva i dati processati"""
    try:
        df.to_csv(PROCESSED_DATA_PATH, index=False)
        logger.info(f"Dati salvati in {PROCESSED_DATA_PATH}")
    except Exception as e:
        logger.error(f"Errore durante il salvataggio: {str(e)}")
        raise

def main():
    try:
        logger.info("=== INIZIO ELABORAZIONE ===")
        
        if not verify_file_structure():
            sys.exit(1)
            
        # Pipeline ETL
        raw_data = extract_data()
        filtered_data = filter_data(raw_data)
        analyzed_data = analyze_data(filtered_data)
        save_data(analyzed_data)
        
        logger.info("=== ELABORAZIONE COMPLETATA CON SUCCESSO ===")
    
    except Exception as e:
        logger.error(f"ERRORE CRITICO: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()