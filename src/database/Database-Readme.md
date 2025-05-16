# Gestione del Database per Comtrade Data Pipeline

Questa directory contiene gli script e le istruzioni per la gestione del database PostgreSQL utilizzato dal Comtrade Data Pipeline.

## Schema del Database

Lo schema del database è organizzato nel seguente modo:

### Tabelle di Riferimento

- **reporters**: Paesi dichiaranti (27 paesi dell'UE)
- **partners**: Paesi partner commerciali
- **commodities**: Codici delle merci (HS a 6 cifre)
- **flows**: Tipi di flusso commerciale (import/export)
- **measurement_units**: Unità di misura

### Tabella Principale

- **tariffline_data**: Contiene tutti i dati commerciali dettagliati con riferimenti alle tabelle di lookup

### Tabella di Log

- **import_logs**: Tiene traccia delle operazioni di importazione

## Inizializzazione del Database

Esistono due modi per inizializzare il database:

### 1. Usando lo script Shell

```bash
./init_db.sh [opzioni]
```

Opzioni disponibili:
- `--host=HOST`: Host del database (default dal file .env)
- `--port=PORT`: Porta del database (default dal file .env)
- `--dbname=NAME`: Nome del database (default dal file .env)
- `--user=USER`: Utente del database (default dal file .env)
- `--password=PWD`: Password del database (default dal file .env)

### 2. Usando lo script Python

```bash
python init_db.py [opzioni]
```

Opzioni disponibili:
- `--host HOST`: Host del database
- `--port PORT`: Porta del database
- `--dbname DBNAME`: Nome del database
- `--user USER`: Utente del database
- `--password PASSWORD`: Password del database
- `--sql-file FILE`: File SQL di inizializzazione (default: init_schema.sql)

## Struttura delle Tabelle

### reporters

Contiene i 27 paesi dell'Unione Europea che sono dichiaranti nel dataset.

Colonne:
- `id`: Chiave primaria
- `reporter_code`: Codice del paese (es. 'DE', 'FR')
- `reporter_name`: Nome del paese (es. 'Germany', 'France')
- `created_at`: Data di creazione del record
- `updated_at`: Data dell'ultimo aggiornamento

### partners

Contiene tutti i paesi partner commerciali.

Colonne:
- `id`: Chiave primaria
- `partner_code`: Codice del paese partner
- `partner_name`: Nome del paese partner
- `created_at`: Data di creazione del record
- `updated_at`: Data dell'ultimo aggiornamento

### commodities

Contiene i codici delle merci secondo il sistema armonizzato (HS).

Colonne:
- `id`: Chiave primaria
- `commodity_code`: Codice HS della merce (a 6 cifre)
- `commodity_description`: Descrizione della merce
- `hs_level`: Livello di dettaglio HS (per questo progetto sempre 6)
- `created_at`: Data di creazione del record
- `updated_at`: Data dell'ultimo aggiornamento

### flows

Contiene i tipi di flusso commerciale (per questo progetto solo "Import").

Colonne:
- `id`: Chiave primaria
- `flow_code`: Codice del flusso ('M' per import)
- `flow_desc`: Descrizione del flusso
- `created_at`: Data di creazione del record
- `updated_at`: Data dell'ultimo aggiornamento

### measurement_units

Contiene le unità di misura utilizzate nei dati commerciali.

Colonne:
- `id`: Chiave primaria
- `unit_code`: Codice dell'unità di misura
- `unit_description`: Descrizione dell'unità di misura
- `created_at`: Data di creazione del record
- `updated_at`: Data dell'ultimo aggiornamento

### tariffline_data

Contiene i dati dettagliati del commercio internazionale.

Colonne:
- `id`: Chiave primaria
- `reporter_id`: ID del paese dichiarante (riferimento a `reporters.id`)
- `partner_id`: ID del paese partner (riferimento a `partners.id`)
- `commodity_id`: ID della merce (riferimento a `commodities.id`)
- `flow_id`: ID del flusso (riferimento a `flows.id`)
- `period`: Periodo in formato 'YYYYMM'
- `year`: Anno
- `month`: Mese
- `net_weight`: Peso netto
- `quantity`: Quantità
- `quantity_unit`: Unità di misura della quantità
- `trade_value`: Valore commerciale
- `flag`: Flag di qualità dei dati
- `is_reporter_estimate`: Indica se il valore è una stima del dichiarante
- `customs`: Valore doganale
- `qty_unit_code`: Codice dell'unità di misura della quantità
- `qty_unit`: Nome dell'unità di misura della quantità
- `alt_qty`: Quantità alternativa
- `alt_qty_unit_code`: Codice dell'unità di misura alternativa
- `gross_weight`: Peso lordo
- `cif_value`: Valore CIF (Cost, Insurance, Freight)
- `fob_value`: Valore FOB (Free On Board)
- `created_at`: Data di creazione del record
- `updated_at`: Data dell'ultimo aggiornamento
- `source_file`: Identificatore della sorgente dati

### import_logs

Tiene traccia delle operazioni di importazione.

Colonne:
- `id`: Chiave primaria
- `reporter_code`: Codice del paese dichiarante
- `start_period`: Periodo di inizio in formato 'YYYYMM'
- `end_period`: Periodo di fine in formato 'YYYYMM'
- `records_processed`: Numero di record processati
- `records_inserted`: Numero di record inseriti
- `records_skipped`: Numero di record saltati (duplicati)
- `duration_seconds`: Durata dell'operazione in secondi
- `status`: Stato dell'operazione ('SUCCESS', 'FAILED', 'PARTIAL')
- `error_message`: Messaggio di errore (se presente)
- `started_at`: Data e ora di inizio
- `completed_at`: Data e ora di completamento
- `api_calls`: Numero di chiamate API effettuate
- `cache_hits`: Numero di hit dalla cache

## Query di Esempio

### Ottenere le importazioni totali per paese e anno

```sql
SELECT 
    r.reporter_name,
    t.year,
    SUM(t.trade_value) as total_import_value
FROM 
    comtrade.tariffline_data t
JOIN 
    comtrade.reporters r ON t.reporter_id = r.id
GROUP BY 
    r.reporter_name, t.year
ORDER BY 
    r.reporter_name, t.year;
```

### Ottenere le importazioni mensili per un paese specifico

```sql
SELECT 
    t.year,
    t.month,
    SUM(t.trade_value) as monthly_import_value
FROM 
    comtrade.tariffline_data t
JOIN 
    comtrade.reporters r ON t.reporter_id = r.id
WHERE 
    r.reporter_code = 'DE'
GROUP BY 
    t.year, t.month
ORDER BY 
    t.year, t.month;
```
```

## Conclusione

Ho creato una guida dettagliata per la definizione dello schema del database PostgreSQL per il Comtrade Data Pipeline. La configurazione include:

1. **Script SQL (init_schema.sql)** per creare lo schema del database con tutte le tabelle e indici necessari
2. **Script Bash (init_db.sh)** per inizializzare facilmente il database da riga di comando
3. **Script Python (init_db.py)** come alternativa più robusta per l'inizializzazione del database
4. **Documentazione (README.md)** con dettagli sulla struttura del database e query di esempio

Lo schema è progettato per:
- Supportare efficacemente i dati Tariffline con granularità HS a 6 cifre
- Normalizzare i dati per evitare ridondanze
- Includere indici appropriati per query performanti
- Tracciare le operazioni di importazione con una tabella di log
- Mantenere timestamps di creazione/aggiornamento per tutti i record
