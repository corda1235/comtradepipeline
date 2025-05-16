# File: src/database/init_db.py
# Script Python per inizializzare il database PostgreSQL per Comtrade Data Pipeline

import os
import sys
import argparse
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

# Aggiungi la directory principale al path per importare i moduli del progetto
sys.path.append(str(Path(__file__).parent.parent))

# Importa la funzione di caricamento della configurazione dal progetto principale
try:
    from src.utils.config_loader import load_config
except ImportError:
    print("ERRORE: Impossibile importare i moduli dal progetto principale")
    sys.exit(1)


def parse_args():
    """Analizza gli argomenti da riga di comando."""
    parser = argparse.ArgumentParser(description='Inizializza il database PostgreSQL per Comtrade Data Pipeline')
    
    parser.add_argument(
        '--host', 
        help='Host del database'
    )
    
    parser.add_argument(
        '--port', 
        type=int,
        help='Porta del database'
    )
    
    parser.add_argument(
        '--dbname', 
        help='Nome del database'
    )
    
    parser.add_argument(
        '--user', 
        help='Utente del database'
    )
    
    parser.add_argument(
        '--password', 
        help='Password del database'
    )
    
    parser.add_argument(
        '--sql-file',
        default='init_schema.sql',
        help='File SQL di inizializzazione (default: init_schema.sql)'
    )
    
    return parser.parse_args()


def init_database(db_config, sql_file):
    """Inizializza il database usando lo script SQL specificato."""
    print(f"Inizializzazione database {db_config['dbname']} su {db_config['host']}:{db_config['port']}")
    
    # Connessione per verificare se il database esiste
    try:
        # Connessione al server PostgreSQL
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            dbname='postgres'  # Connessione al database di default
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Verifica se il database esiste
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_config['dbname']}'")
        exists = cursor.fetchone()
        
        if not exists:
            print(f"Il database {db_config['dbname']} non esiste. Creazione...")
            # Crea il database
            cursor.execute(f"CREATE DATABASE {db_config['dbname']}")
            print("Database creato con successo.")
        else:
            print(f"Il database {db_config['dbname']} esiste già.")
        
        # Chiudi la connessione
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"ERRORE durante la verifica/creazione del database: {e}")
        return False
    
    # Connessione al database specifico e inizializzazione schema
    try:
        # Leggi il file SQL
        sql_path = Path(__file__).parent / sql_file
        if not sql_path.exists():
            print(f"ERRORE: File SQL {sql_file} non trovato")
            return False
        
        with open(sql_path, 'r') as f:
            sql_script = f.read()
        
        # Connessione al database
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            dbname=db_config['dbname']
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Esegui lo script SQL
        print("Esecuzione dello script di inizializzazione...")
        cursor.execute(sql_script)
        
        # Chiudi la connessione
        cursor.close()
        conn.close()
        
        print("Inizializzazione del database completata con successo.")
        return True
        
    except psycopg2.Error as e:
        print(f"ERRORE durante l'inizializzazione del database: {e}")
        return False
    except Exception as e:
        print(f"ERRORE imprevisto: {e}")
        return False


def main():
    """Funzione principale."""
    args = parse_args()
    
    # Carica configurazione dal file .env
    env_path = Path(__file__).parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
    
    # Carica configurazione dal progetto
    config = load_config()
    db_config = config['db']
    
    # Sovrascrivi le configurazioni con i parametri da riga di comando se presenti
    if args.host:
        db_config['host'] = args.host
    if args.port:
        db_config['port'] = args.port
    if args.dbname:
        db_config['dbname'] = args.dbname
    if args.user:
        db_config['user'] = args.user
    if args.password:
        db_config['password'] = args.password
    
    # Inizializza il database
    success = init_database(db_config, args.sql_file)
    
    if success:
        print("Schema del database configurato e pronto all'uso.")
        sys.exit(0)
    else:
        print("ERRORE: Inizializzazione del database fallita.")
        sys.exit(1)


if __name__ == "__main__":
    main()
