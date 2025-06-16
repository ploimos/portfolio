import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
import shutil

# 1. Configurazione percorsi ASSOLUTI
BASE_DIR = Path(__file__).parent.parent  # Cartella principale del progetto (etl/)
DATA_DIR = BASE_DIR / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'
LOGS_DIR = BASE_DIR / 'logs'

# Crea tutte le cartelle necessarie
for folder in [RAW_DATA_DIR, PROCESSED_DATA_DIR, LOGS_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

# 2. Configurazione logging avanzata
log_file = LOGS_DIR / 'etl.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# 3. Funzioni di supporto con gestione errori migliorata
def filter_data(df):
    """Filtra i dati mantenendo tutte le frodi e un campione delle normali"""
    try:
        fraud = df[df['isFraud'] == True]
        normal = df[df['isFraud'] == False].sample(frac=0.05, random_state=42)
        logger.info(f"Filtro applicato: {len(fraud)} frodi + {len(normal)} normali")
        return pd.concat([fraud, normal])
    except Exception as e:
        logger.error(f"Errore nel filtro dati: {str(e)}", exc_info=True)
        raise

def optimize_dtypes(df):
    """Ottimizza i tipi di dati per ridurre la memoria"""
    try:
        df['TransactionAmt'] = pd.to_numeric(df['TransactionAmt'], downcast='float')
        df['ProductCD'] = df['ProductCD'].astype('category')
        df['isFraud'] = df['isFraud'].astype('bool')
        logger.info("Tipi di dati ottimizzati")
        return df
    except Exception as e:
        logger.error(f"Errore nell'ottimizzazione: {str(e)}", exc_info=True)
        raise

# 4. Funzioni principali ETL
def load_data():
    """Carica i dati dal file CSV con gestione errori rinforzata"""
    try:
        input_file = RAW_DATA_DIR / 'train_transaction.csv'
        
        if not input_file.exists():
            raise FileNotFoundError(f"File non trovato: {input_file}")
        
        logger.info(f"Caricamento dati da: {input_file}")
        
        # Caricamento con gestione warning migliorata
        with pd.option_context('mode.chained_assignment', None):
            df = pd.read_csv(
                input_file,
                usecols=['TransactionID', 'TransactionDT', 'TransactionAmt', 'ProductCD', 'isFraud'],
                dtype={'isFraud': 'int8'}
            )
            
            # Conversione data esplicita
            df['TransactionDT'] = pd.to_numeric(df['TransactionDT'], errors='coerce')
            df['TransactionDate'] = pd.to_datetime(df['TransactionDT'], unit='s', errors='coerce')
            df = df.drop(columns=['TransactionDT'])
        
        logger.info(f"Dati caricati con successo. Righe: {len(df)}")
        return df
        
    except Exception as e:
        logger.error(f"ERRORE CRITICO nel caricamento: {str(e)}", exc_info=True)
        logger.error(f"Percorso cercato: {input_file.absolute()}")
        logger.error("Verifica che:")
        logger.error("1. Il file esista nella cartella raw/")
        logger.error("2. Il nome del file sia esatto (case-sensitive)")
        logger.error("3. Il file non sia aperto in altri programmi")
        raise

def save_to_csv(df):
    """Salva i dati in formato CSV con validazione per Windows"""
    try:
        output_file = PROCESSED_DATA_DIR / 'powerbi_ready_data.csv'
        
        # Verifica spazio disco (versione Windows)
        total, used, free = shutil.disk_usage(output_file.parent)
        if df.memory_usage().sum() > free:
            raise IOError(f"Spazio disco insufficiente. Necessari: {df.memory_usage().sum()/1e6:.2f}MB, Disponibili: {free/1e6:.2f}MB")
        
        # Salvataggio con controllo errori
        temp_file = output_file.with_suffix('.tmp')
        df.to_csv(
            temp_file,
            index=False,
            encoding='utf-8',
            date_format='%Y-%m-%d %H:%M:%S'
        )
        
        # Sostituzione atomica del file
        if output_file.exists():
            output_file.unlink()
        temp_file.rename(output_file)
        
        logger.info(f"Dati salvati con successo in: {output_file}")
        print(f"\n✅ File generato con successo:\n{output_file.absolute()}\n")
        return output_file
    except Exception as e:
        if 'temp_file' in locals() and temp_file.exists():
            temp_file.unlink()
        logger.error(f"Errore nel salvataggio: {str(e)}", exc_info=True)
        raise

# 5. Esecuzione principale con gestione errori completa
if __name__ == "__main__":
    try:
        logger.info("=== INIZIO ETL ===")
        
        # Caricamento
        logger.info("Fase 1/4 - Caricamento dati")
        df_raw = load_data()
        
        # Filtro
        logger.info("Fase 2/4 - Filtro dati")
        df_filtered = filter_data(df_raw)
        
        # Ottimizzazione
        logger.info("Fase 3/4 - Ottimizzazione")
        df_optimized = optimize_dtypes(df_filtered)
        
        # Salvataggio
        logger.info("Fase 4/4 - Salvataggio")
        saved_path = save_to_csv(df_optimized)
        
        logger.info(f"=== ETL COMPLETATO ===")
        logger.info(f"Righe finali: {len(df_optimized)}")
        logger.info(f"Dimensioni file: {saved_path.stat().st_size / (1024*1024):.2f} MB")
        
    except Exception as e:
        logger.critical("ETL FALLITO!", exc_info=True)
        print(f"\n❌ Errore durante l'ETL. Controlla il log: {log_file.absolute()}\n")
        raise