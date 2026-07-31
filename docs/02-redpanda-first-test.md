# Primo test di Redpanda

## Obiettivo

In questo test verifico il funzionamento dell'infrastruttura Redpanda
prima di sviluppare i servizi Python.

Il test permette di separare eventuali problemi infrastrutturali da
eventuali problemi presenti nel codice applicativo.

## Configurazione

L'ambiente utilizza:

- un broker Redpanda;
- Redpanda Console;
- una rete Docker;
- un volume persistente;
- un topic con tre partizioni.

Il topic utilizzato è:

`factory.telemetry`

## Creazione del topic

Ho creato il topic tramite il comando:

```bash
docker exec redpanda rpk topic create factory.telemetry --partitions 3