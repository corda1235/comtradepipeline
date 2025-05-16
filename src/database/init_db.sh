# File: db/init_db.sh
#!/bin/bash

# Script per inizializzare il database PostgreSQL per Comtrade Data Pipeline

# Carica variabili d'ambiente
if [ -f "../.env" ]; then
    source <(grep -v '^#' ../.env | sed -E 's/(.*)=(.*)$/export \1="\2"/g')
fi

# Imposta valori predefiniti se non presenti nell'ambiente
DB_HOST=${DB_HOST:-localhost}
DB_PORT=${DB_PORT:-5432}
DB_NAME=${DB_NAME:-comtrade_data}
DB_USER=${DB_USER:-postgres}
DB_PASSWORD=${DB_PASSWORD:-""}

# Funzione per stampare l'aiuto
print_help() {
    echo "Utilizzo: $0 [opzioni]"
    echo "Opzioni:"
    echo "  -h, --help       Mostra questo messaggio di aiuto"
    echo "  --host=HOST      Host del database (default: $DB_HOST)"
    echo "  --port=PORT      Porta del database (default: $DB_PORT)"
    echo "  --dbname=NAME    Nome del database (default: $DB_NAME)"
    echo "  --user=USER      Utente del database (default: $DB_USER)"
    echo "  --password=PWD   Password del database"
    echo ""
}

# Parsing degli argomenti
for i in "$@"; do
    case $i in
        -h|--help)
            print_help
            exit 0
            ;;
        --host=*)
            DB_HOST="${i#*=}"
            shift
            ;;
        --port=*)
            DB_PORT="${i#*=}"
            shift
            ;;
        --dbname=*)
            DB_NAME="${i#*=}"
            shift
            ;;
        --user=*)
            DB_USER="${i#*=}"
            shift
            ;;
        --password=*)
            DB_PASSWORD="${i#*=}"
            shift
            ;;
        *)
            echo "Opzione non riconosciuta: $i"
            print_help
            exit 1
            ;;
    esac
done

echo "Inizializzazione database $DB_NAME su $DB_HOST:$DB_PORT"

# Costruisci la stringa di connessione
CONN_STRING="postgresql://$DB_USER"
if [ -n "$DB_PASSWORD" ]; then
    CONN_STRING="$CONN_STRING:$DB_PASSWORD"
fi
CONN_STRING="$CONN_STRING@$DB_HOST:$DB_PORT/$DB_NAME"

# Verifica se il database esiste
echo "Verifico se il database $DB_NAME esiste..."
if ! psql -h $DB_HOST -p $DB_PORT -U $DB_USER -lqt | cut -d \| -f 1 | grep -qw $DB_NAME; then
    echo "Il database $DB_NAME non esiste. Creazione..."
    createdb -h $DB_HOST -p $DB_PORT -U $DB_USER $DB_NAME
    if [ $? -ne 0 ]; then
        echo "ERRORE: Impossibile creare il database."
        exit 1
    fi
    echo "Database creato con successo."
else
    echo "Il database $DB_NAME esiste già."
fi

# Esegui lo script SQL
echo "Esecuzione dello script di inizializzazione..."
psql $CONN_STRING -f init_schema.sql

if [ $? -eq 0 ]; then
    echo "Inizializzazione del database completata con successo."
else
    echo "ERRORE: Inizializzazione del database fallita."
    exit 1
fi

echo "Schema del database configurato e pronto all'uso."
exit 0
