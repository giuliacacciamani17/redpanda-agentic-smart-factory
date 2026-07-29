# Introduzione al progetto

## Contesto

I moderni sistemi industriali sono composti da sensori nei macchinari,
servizi edge e applicazioni distribuite che producono e consumano dati
in tempo reale.

In un'architettura tradizionale, i com*onenti possono comunicare tramite
richieste dirette. Questo approccio crea una dipendenza tra il produttore
del dato e il servizio che deve elaborarlo.

Il progetto utilizza invece un'architettura event-driven nella quale
i componenti comunicato pubblicando e consumando eventi attraverso
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

Il sistema non rappresenta un impianto industriale pronto per la
produzione. È un prototipo didattico che permette di osservare
concretamente concetti relativi a:

- sistemi distribuiti;
- edge computing;
- event streaming;
- comunicazione asincrona;
- agenti software;
- persistenza degli eventi;
- resilienza;
- containerizzazione.