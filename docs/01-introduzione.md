# Introduzione al progetto

## Contesto

I moderni sistemi industriali sono composti da sensori nei macchinari,
servizi edge e applicazioni distribuite che producono e consumano dati
in tempo reale.

In un'architettura tradizionale, i componenti possono comunicare tramite
richieste dirette. Questo approccio crea una dipendenza tra il produttore
del dato e il servizio che deve elaborarlo.

Il progetto utilizza invece un'architettura event-driven nella quale
i componenti comunicano pubblicando e consumando eventi attraverso
Redpanda.

## Scenario applicativo

Lo scenario simulato è una Smart Factory composta da più macchinari.
Ogni macchina produce dati telemetrici relativi al proprio funzionamento.

I dati includono:

- temperatura;
- vibrazione;
- velocità;
- consumo energetico;
- stato operativo.

Un Maintenance Agent analizza gli eventi, mantiene un contesto per ogni
macchina e sceglie autonomamente l'azione più appropriata rispetto
all'obiettivo di ridurre il rischio di guasto.


## Scopo didattico

Il sistema non rappresenta un impianto industriale pronto per la produzione. È un prototipo didattico che permette di osservare concretamente concetti relativi a:

- simulazione controllata della telemetria di un macchinario;
- organizzazione degli eventi in topic distinti;
- calcolo del rischio a partire da temperatura, vibrazione e andamento delle misurazioni;
- utilizzo della memoria recente per valutare lo stato della macchina;
- selezione automatica di azioni come `NO_ACTION`, `MONITOR`, `REDUCE_SPEED`, `REQUEST_INSPECTION` ed `EMERGENCY_STOP`;
- separazione tra la decisione del Maintenance Agent e l'esecuzione del Machine Controller;
- gestione di risultati operativi positivi e negativi, rappresentati da `SUCCESS` e `FAILED`;
- acquisizione del risultato da parte dell'agente attraverso il ciclo di feedback;
- tracciabilità end-to-end degli eventi tramite `correlation_id`;
- utilizzo di topic, partizioni, offset e consumer group in Redpanda;
- osservazione del flusso mediante Redpanda Console;
- orchestrazione dei componenti tramite Docker Compose.

## Configurazione

L'ambiente utilizza:

- un broker Redpanda;
- Redpanda Console;
- una rete Docker;
- un volume persistente;
- un topic con tre partizioni.