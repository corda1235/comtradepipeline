# File: start.sh

#!/bin/bash

# Script di avvio per il Comtrade Data Pipeline

# Abilita il fallback quando un comando fallisce
set -e

# Carica le variabili d'ambiente
if [ -f ".env" ]; then
    source <(grep -v '^#' .env | sed -E 's/(.*)=(.*)$/export \1="\2"/g')
fi

# Attiva l'ambiente virtuale
if [ -d "venv" ]; then
    source venv/bin/activate
else
    echo "ERRORE: Ambiente virtuale non trovato. Eseguire prima setup_dev.sh."
    exit 1
fi

# Gestione argomenti
COUNTRIES="all"
START_DATE=""
END_DATE=""
LOG_LEVEL="INFO"
CLEAR_CACHE=false
DB_INIT_ONLY=false

print_usage() {
    echo "Utilizzo: $0 [opzioni]"
    echo "Opzioni:"
    echo "  -c, --countries     Codici paese separati da virgola o 'all' (default: all)"
    echo "  -s, --start-date    Data di inizio in formato YYYY-MM (richiesta)"
    echo "  -e, --end-date      Data di fine in formato YYYY-MM (richiesta)"
    echo "  -l, --log-level     Livello di logging (default: INFO)"
    echo "  --clear-cache       Cancella la cache prima dell'esecuzione"
    echo "  --db-init-only      Inizializza solo il database senza eseguire la pipeline"
    echo "  -h, --help          Mostra questo messaggio di aiuto"
    exit 1
}

# Parsing degli argomenti
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--countries)
            COUNTRIES="$2"
            shift 2
            ;;
        -s|--start-date)
            START_DATE="$2"
            shift 2
            ;;
        -e|--end-date)
            END_DATE="$2"
            shift 2
            ;;
        -l|--log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        --clear-cache)
            CLEAR_CACHE=true
            shift
            ;;
        --db-init-only)
            DB_INIT_ONLY=true
            shift
            ;;
        -h|--help)
            print_usage
            ;;
        *)
            echo "Opzione non riconosciuta: $1"
            print_usage
            ;;
    esac
done

# Verifica argomenti obbligatori
if [ "$DB_INIT_ONLY" = false ] && ([ -z "$START_DATE" ] || [ -z "$END_DATE" ]); then
    echo "ERRORE: start-date e end-date sono richiesti."
    print_usage
fi

# Costruisci il comando
CMD="python main.py"

if [ "$DB_INIT_ONLY" = true ]; then
    CMD="$CMD --db-init-only"
else
    CMD="$CMD --countries=$COUNTRIES --start-date=$START_DATE --end-date=$END_DATE"
fi

CMD="$CMD --log-level=$LOG_LEVEL"

if [ "$CLEAR_CACHE" = true ]; then
    CMD="$CMD --clear-cache"
fi

# Esegui il comando
echo "Esecuzione: $CMD"
eval $CMD

exit_code=$?
if [ $exit_code -eq 0 ]; then
    echo "Esecuzione completata con successo."
else
    echo "Esecuzione fallita con codice di uscita $exit_code."
fi

exit $exit_code