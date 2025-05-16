# File: setup_dev.sh

#!/bin/bash

# Script di configurazione dell'ambiente di sviluppo per il Comtrade Data Pipeline

echo "Configurazione dell'ambiente di sviluppo per Comtrade Data Pipeline"

# Controlla se virtualenv è installato
if ! command -v virtualenv &> /dev/null; then
    echo "Installazione di virtualenv..."
    pip install virtualenv
fi

# Crea l'ambiente virtuale se non esiste
if [ ! -d "venv" ]; then
    echo "Creazione dell'ambiente virtuale..."
    python -m venv venv
fi

# Attiva l'ambiente virtuale
echo "Attivazione dell'ambiente virtuale..."
source venv/bin/activate

# Installa le dipendenze
echo "Installazione dipendenze..."
pip install -r requirements.txt

# Crea directory necessarie
echo "Creazione directory di progetto..."
mkdir -p logs
mkdir -p cache

# Configura .env se non esiste
if [ ! -f ".env" ]; then
    echo "Creazione file .env di esempio..."
    cp .env.example .env
    echo "IMPORTANTE: Modifica il file .env con le tue chiavi API e credenziali database"
fi

# Crea database PostgreSQL (solo se psql è disponibile)
if command -v psql &> /dev/null; then
    echo "Controllo database PostgreSQL..."
    
    # Leggi le configurazioni dal file .env
    if [ -f ".env" ]; then
        source <(grep -v '^#' .env | sed -E 's/(.*)=(.*)$/export \1="\2"/g')
    fi
    
    # Usa i valori di default se non definiti
    DB_NAME=${DB_NAME:-comtrade_data}
    DB_USER=${DB_USER:-postgres}
    
    # Controlla se il database esiste già
    if psql -lqt | cut -d \| -f 1 | grep -qw $DB_NAME; then
        echo "Database $DB_NAME già esistente"
    else
        echo "Creazione database $DB_NAME..."
        createdb $DB_NAME -U $DB_USER
        
        if [ $? -eq 0 ]; then
            echo "Database creato con successo"
        else
            echo "ATTENZIONE: Impossibile creare il database. Crealo manualmente."
        fi
    fi
else
    echo "ATTENZIONE: psql non trovato. Crea il database PostgreSQL manualmente."
fi

echo ""
echo "Setup completato!"
echo "Per utilizzare lo script:"
echo "1. Attiva l'ambiente virtuale: source venv/bin/activate"
echo "2. Configura il file .env con le tue credenziali"
echo "3. Inizializza il database: python main.py --db-init-only"
echo "4. Esegui la pipeline: python main.py --countries=all --start-date=2022-01 --end-date=2022-12"
echo ""