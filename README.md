# ComTrade data download Pipeline Script

Questo progetto è relativo allo sviluppo di uno script parametrico che consente il download e l'archiviazione dei dati del commercio internazionale relativi alle importazioni dei 27 paesi dell'Unione Europea.
Si basa sul pacchetto Python comtradeapicall, disponibile su PyPI. Questo pacchetto è progettato per semplificare l'interazione con le API di UN Comtrade, una piattaforma delle Nazioni Unite che fornisce dati dettagliati sul commercio internazionale. Attraverso comtradeapicall, gli utenti possono estrarre e scaricare dati commerciali in modo più agevole, utilizzando funzioni Python che gestiscono le complessità delle chiamate API sottostanti.

Ecco un riepilogo completo dei dettagli tecnici dello script:

## Dati e API
- **Tipo di dati**: Tariffline 
- **Granularità HS**: 6 cifre (sottointestazioni, standard internazionale)
- **Frequenza**: Dati mensili
- **Selezione dati**: Importazioni (flowCode = M) di merci (typeCode = C) dei 27 paesi UE
- **Partner2Code**: Non richiesto
- **API Keys**: Due chiavi disponibili (primary e secondary)
- **Limiti API**: 500 chiamate al giorno, 100.000 record per chiamata
- **Asincronicità**: NON implementabile con l'API key disponibile

## Funzionalità Tecniche
- **Linguaggio**: Python
- **Parametrizzazione**: Per paese e intervallo temporale (da anno-mese a anno-mese)
- **Caching**: Per minimizzare le chiamate API
- **Logging**: Dettagliato per facilitare il debug
- **Gestione errori**: Meccanismo di retry per download falliti
- **Pipeline**: Processo completo di download e archiviazione

## Archiviazione
- **Database**: PostgreSQL
- **Elaborazione**: Conversione e normalizzazione dei dati prima dell'inserimento
- **Schema DB**: Creazione di uno schema appropriato

## Ambiente
- **Sistema operativo**: VPS Linux Ubuntu 22.04
- **Ambiente virtuale**: VENV
- **Versioning**: Repository GitHub (https://github.com/corda1235/comtradepipeline)
