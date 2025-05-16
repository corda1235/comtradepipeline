-- File: db/init_schema.sql
-- Script di inizializzazione dello schema del database PostgreSQL per Comtrade Data Pipeline

-- Crea schema comtrade se non esiste
CREATE SCHEMA IF NOT EXISTS comtrade;

-- Imposta lo schema per le istruzioni successive
SET search_path TO comtrade;

-- Tabella per i paesi dichiaranti (reporters)
CREATE TABLE IF NOT EXISTS reporters (
    id SERIAL PRIMARY KEY,
    reporter_code VARCHAR(5) UNIQUE NOT NULL,
    reporter_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabella per i paesi partner (partners)
CREATE TABLE IF NOT EXISTS partners (
    id SERIAL PRIMARY KEY,
    partner_code VARCHAR(5) UNIQUE NOT NULL,
    partner_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabella per i codici delle merci (commodities)
CREATE TABLE IF NOT EXISTS commodities (
    id SERIAL PRIMARY KEY,
    commodity_code VARCHAR(10) UNIQUE NOT NULL,
    commodity_description TEXT NOT NULL,
    hs_level SMALLINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabella per i tipi di flusso commerciale (flows)
CREATE TABLE IF NOT EXISTS flows (
    id SERIAL PRIMARY KEY,
    flow_code VARCHAR(5) UNIQUE NOT NULL,
    flow_desc VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabella per le unità di misura (measurement_units)
CREATE TABLE IF NOT EXISTS measurement_units (
    id SERIAL PRIMARY KEY,
    unit_code VARCHAR(10) UNIQUE NOT NULL,
    unit_description VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabella principale per i dati tariffari (tariffline_data)
CREATE TABLE IF NOT EXISTS tariffline_data (
    id SERIAL PRIMARY KEY,
    
    -- Dimensional fields (chiavi esterne)
    reporter_id INTEGER NOT NULL REFERENCES reporters(id),
    partner_id INTEGER NOT NULL REFERENCES partners(id),
    commodity_id INTEGER NOT NULL REFERENCES commodities(id),
    flow_id INTEGER NOT NULL REFERENCES flows(id),
    
    -- Time fields
    period VARCHAR(10) NOT NULL,
    year SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    
    -- Measurement fields
    net_weight DECIMAL(18, 2),
    quantity DECIMAL(18, 2),
    quantity_unit VARCHAR(50),
    trade_value DECIMAL(18, 2),
    flag INTEGER,
    
    -- Additional fields
    is_reporter_estimate BOOLEAN,
    customs DECIMAL(18, 2),
    qty_unit_code VARCHAR(10),
    qty_unit VARCHAR(50),
    alt_qty DECIMAL(18, 2),
    alt_qty_unit_code VARCHAR(10),
    gross_weight DECIMAL(18, 2),
    cif_value DECIMAL(18, 2),
    fob_value DECIMAL(18, 2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255),
    
    -- Composite unique key to prevent duplicates
    UNIQUE(reporter_id, partner_id, commodity_id, flow_id, period)
);

-- Tabella per memorizzare i log dell'importazione (import_logs)
CREATE TABLE IF NOT EXISTS import_logs (
    id SERIAL PRIMARY KEY,
    reporter_code VARCHAR(5) NOT NULL,
    start_period VARCHAR(10) NOT NULL,
    end_period VARCHAR(10) NOT NULL,
    records_processed INTEGER NOT NULL DEFAULT 0,
    records_inserted INTEGER NOT NULL DEFAULT 0,
    records_skipped INTEGER NOT NULL DEFAULT 0,
    duration_seconds DECIMAL(10, 2),
    status VARCHAR(20) NOT NULL,
    error_message TEXT,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    api_calls INTEGER NOT NULL DEFAULT 0,
    cache_hits INTEGER NOT NULL DEFAULT 0
);

-- Indici per migliorare le performance delle query

-- Indici sulla tabella tariffline_data
CREATE INDEX IF NOT EXISTS idx_tariffline_reporter ON tariffline_data(reporter_id);
CREATE INDEX IF NOT EXISTS idx_tariffline_partner ON tariffline_data(partner_id);
CREATE INDEX IF NOT EXISTS idx_tariffline_commodity ON tariffline_data(commodity_id);
CREATE INDEX IF NOT EXISTS idx_tariffline_flow ON tariffline_data(flow_id);
CREATE INDEX IF NOT EXISTS idx_tariffline_period ON tariffline_data(period);
CREATE INDEX IF NOT EXISTS idx_tariffline_year_month ON tariffline_data(year, month);
CREATE INDEX IF NOT EXISTS idx_tariffline_value ON tariffline_data(trade_value);

-- Indici sulle tabelle di riferimento
CREATE INDEX IF NOT EXISTS idx_reporters_code ON reporters(reporter_code);
CREATE INDEX IF NOT EXISTS idx_partners_code ON partners(partner_code);
CREATE INDEX IF NOT EXISTS idx_commodities_code ON commodities(commodity_code);
CREATE INDEX IF NOT EXISTS idx_flows_code ON flows(flow_code);

-- Funzione per aggiornare automaticamente il timestamp updated_at
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger per aggiornare automaticamente il timestamp updated_at nelle tabelle

-- Trigger per reporters
DROP TRIGGER IF EXISTS update_reporters_timestamp ON reporters;
CREATE TRIGGER update_reporters_timestamp
BEFORE UPDATE ON reporters
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

-- Trigger per partners
DROP TRIGGER IF EXISTS update_partners_timestamp ON partners;
CREATE TRIGGER update_partners_timestamp
BEFORE UPDATE ON partners
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

-- Trigger per commodities
DROP TRIGGER IF EXISTS update_commodities_timestamp ON commodities;
CREATE TRIGGER update_commodities_timestamp
BEFORE UPDATE ON commodities
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

-- Trigger per flows
DROP TRIGGER IF EXISTS update_flows_timestamp ON flows;
CREATE TRIGGER update_flows_timestamp
BEFORE UPDATE ON flows
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

-- Trigger per measurement_units
DROP TRIGGER IF EXISTS update_measurement_units_timestamp ON measurement_units;
CREATE TRIGGER update_measurement_units_timestamp
BEFORE UPDATE ON measurement_units
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

-- Trigger per tariffline_data
DROP TRIGGER IF EXISTS update_tariffline_data_timestamp ON tariffline_data;
CREATE TRIGGER update_tariffline_data_timestamp
BEFORE UPDATE ON tariffline_data
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

-- Trigger per import_logs
DROP TRIGGER IF EXISTS update_import_logs_timestamp ON import_logs;
CREATE TRIGGER update_import_logs_timestamp
BEFORE UPDATE ON import_logs
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

-- Inserimento dei dati iniziali

-- Inserimento dei flussi commerciali di base
INSERT INTO flows (flow_code, flow_desc) VALUES
('M', 'Import')
ON CONFLICT (flow_code) DO NOTHING;

-- Commento: Aggiungere qui altri dati di base se necessario
