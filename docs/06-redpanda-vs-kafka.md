# Redpanda e Apache Kafka a confronto

## 1. Introduzione

Redpanda e Apache Kafka sono piattaforme di event streaming distribuito basate su producer, consumer, broker, topic, partizioni, chiavi, offset e consumer group.

Nel progetto **Redpanda Agentic Smart Factory**, Redpanda costituisce il data plane che trasporta:

```text
factory.telemetry
→ factory.agent-decisions
→ factory.commands
→ factory.command-results
→ factory.agent-feedback
```

Redpanda è compatibile con molte API e molti client Kafka, ma non è una distribuzione di Apache Kafka. È un prodotto distinto, con implementazione, strumenti operativi e architettura propri.

---

## 2. Apache Kafka

Apache Kafka è una piattaforma open source distribuita per lo streaming di eventi. I producer pubblicano record nei topic, i topic sono divisi in partizioni e i consumer elaborano i record individualmente o all'interno di consumer group.

Kafka è usato per pipeline in tempo reale, integrazione tra applicazioni, raccolta di eventi, stream processing e sistemi event-driven.

```text
Producer → Kafka broker e topic → Consumer
```

---

## 3. Redpanda

Redpanda è una piattaforma di event streaming compatibile con il protocollo Kafka. Questa compatibilità permette di utilizzare molti client Kafka senza cambiare il modello applicativo.

Nel progetto viene usato il client Python:

```python
from confluent_kafka import Consumer, Producer
```

La configurazione mantiene proprietà Kafka standard:

```python
configuration = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
```

La compatibilità riguarda protocollo e API supportate, non l'identità completa tra i prodotti.

---

## 4. Concetti condivisi

Redpanda e Kafka condividono i concetti usati dalla Smart Factory:

- producer;
- consumer;
- broker;
- topic;
- partizioni;
- chiavi;
- offset;
- consumer group;
- retention;
- replica.

Il codice del progetto pubblica record con:

```python
producer.produce(
    topic=topic,
    key=machine_id.encode("utf-8"),
    value=json.dumps(event).encode("utf-8"),
    callback=delivery_report,
)
```

Il modello applicativo sarebbe quindi in gran parte riutilizzabile con Kafka.

---

## 5. Compatibilità e identità

Dire che Redpanda è Kafka API-compatible significa che supporta molte richieste e molti client dell'ecosistema Kafka.

Non significa che:

- il broker sia implementato nello stesso modo;
- ogni funzione sia identica;
- gli strumenti amministrativi siano gli stessi;
- tutte le estensioni siano compatibili;
- il comportamento operativo sia uguale in ogni dettaglio.

Prima di una migrazione è necessario verificare versioni, API, sicurezza, quote, transazioni e funzionalità specifiche.

---

## 6. Implementazione del broker

Apache Kafka viene eseguito sulla JVM. Redpanda usa un motore nativo differente e non richiede la JVM per il broker.

Questa differenza non cambia i concetti visibili al codice Python, ma incide su:

- runtime;
- gestione delle risorse;
- distribuzione del software;
- configurazione;
- strumenti operativi;
- profilo prestazionale.

Nel progetto Redpanda viene avviato con:

```yaml
image: redpandadata/redpanda:v26.2.1
```

Usando Kafka cambierebbero immagine, configurazione del broker e comandi amministrativi.

---

## 7. Metadati e consenso

Kafka moderno utilizza KRaft per la gestione distribuita dei metadati. I server possono avere ruolo di broker, controller oppure entrambi.

Redpanda utilizza una propria architettura basata su Raft.

È quindi scorretto affermare che Kafka moderno richieda sempre ZooKeeper.

La formulazione corretta è:

```text
Kafka moderno usa KRaft.
Redpanda usa una propria architettura basata su Raft.
```

---

## 8. Strumenti operativi

Redpanda fornisce `rpk`, usato nel progetto per:

```bash
rpk topic list
rpk topic create factory.telemetry --partitions 3
rpk group describe maintenance-agent-group
```

Kafka dispone degli strumenti della propria distribuzione per topic, consumer group, configurazione, produzione e consumo da console.

I concetti amministrati sono simili, ma esperienza operativa e sintassi cambiano.

---

## 9. Interfaccia grafica

Il progetto usa Redpanda Console:

```yaml
redpanda-console:
  image: redpandadata/console:v3.9.0
```

La Console mostra topic, JSON, chiavi, partizioni, offset, consumer group e lag.

Redpanda Console non fa parte del progetto Apache Kafka, anche se può essere utilizzata in ambienti Kafka-compatible opportunamente configurati.

---

## 10. Client Python e portabilità

Il progetto usa `confluent-kafka`, non una libreria proprietaria del broker Redpanda.

Questo favorisce la portabilità. In un passaggio a Kafka potrebbe essere sufficiente, per la parte minima, modificare il broker:

```yaml
KAFKA_BROKER: kafka:9092
```

In un ambiente reale sarebbe comunque necessario verificare sicurezza, TLS, autenticazione, acknowledgment, idempotenza, transazioni, quote e compatibilità delle versioni.

---

## 11. Topic del progetto

La stessa struttura logica può essere realizzata sia con Redpanda sia con Kafka:

```text
factory.telemetry
factory.agent-decisions
factory.commands
factory.command-results
factory.agent-feedback
```

Ogni topic ha tre partizioni nel progetto e usa `machine_id` come chiave.

```python
key=event["machine_id"].encode("utf-8")
```

Gli eventi della stessa macchina vengono instradati coerentemente, preservando l'ordine nella relativa partizione.

---

## 12. Consumer group e lag

Nel progetto sono presenti:

```text
maintenance-agent-group
machine-controller-group
```

Entrambi hanno mostrato:

```text
STATE = Stable
TOTAL-LAG = 0
```

Il Maintenance Agent aveva elaborato cinque telemetrie, mentre il Machine Controller aveva elaborato tre comandi.

Consumer group, offset e lag sono concetti condivisi tra Redpanda e Kafka.

---

## 13. Commit degli offset

Il progetto disabilita il commit automatico:

```python
"enable.auto.commit": False
```

Dopo l'elaborazione esegue:

```python
consumer.commit(
    message=message,
    asynchronous=False,
)
```

Questa logica utilizza il modello Kafka dei consumer group e può essere usata con Redpanda grazie alla compatibilità API.

---

## 14. Persistenza e retention

Entrambe le piattaforme conservano eventi in log partizionati e applicano politiche di retention.

Nel progetto Redpanda usa un volume Docker:

```yaml
volumes:
  - redpanda-data:/var/lib/redpanda/data
```

Il comando seguente rimuove anche il volume e azzera i dati locali:

```bash
docker compose down -v
```

Con Kafka sarebbe necessario configurare analogamente uno storage persistente.

---

## 15. Replica e tolleranza ai guasti

Redpanda e Kafka possono replicare le partizioni tra più nodi.

Il progetto locale utilizza:

```text
1 broker
1 replica
```

Questa configurazione non dimostra la tolleranza al guasto di un cluster. Per un confronto completo servirebbero cluster multi-nodo equivalenti.

---

## 16. Sicurezza

Entrambe le piattaforme offrono funzioni di sicurezza, ma configurazioni e dettagli possono differire:

- TLS;
- SASL;
- ACL;
- quote;
- audit;
- gestione delle identità.

Il progetto locale non abilita queste funzioni. Una migrazione reale deve verificare anche le eccezioni di compatibilità documentate da Redpanda.

---

## 17. Schema Registry

Gli eventi della Smart Factory sono JSON e vengono validati nel codice:

```python
required_fields = {
    "event_id",
    "correlation_id",
    "machine_id",
    "timestamp",
    "temperature",
    "vibration",
}
```

Redpanda offre funzionalità di Schema Registry nella propria piattaforma. Nell'ecosistema Kafka è possibile aggiungere una soluzione di Schema Registry separata.

Il prototipo non ne ha ancora bisogno, ma sarebbe utile per versionamento e compatibilità degli eventi.

---

## 18. Stream processing ed ecosistema

Kafka dispone di Kafka Streams, una libreria Java per stream processing.

Il progetto non usa Kafka Streams. La logica è implementata direttamente nei servizi Python:

```text
Maintenance Agent
Machine Controller
```

Redpanda supporta applicazioni Kafka-compatible e offre strumenti propri nel proprio ecosistema.

Kafka possiede un ecosistema ampio e maturo. Redpanda punta alla compatibilità con molti client e strumenti esistenti, ma la compatibilità deve essere verificata per il caso concreto.

---

## 19. Prestazioni

Il progetto non è un benchmark tra Redpanda e Kafka.

Un confronto corretto richiederebbe:

- stesso hardware;
- stesso numero di nodi;
- stessa replica;
- stesse partizioni;
- stessi messaggi;
- stessi acknowledgment;
- stessa compressione;
- stesso client;
- misure di throughput, latenza, CPU e memoria.

Dal prototipo non è corretto concludere che una piattaforma sia sempre più veloce dell'altra.

---

## 20. Complessità operativa

Redpanda ha permesso di creare una demo locale compatta con:

```bash
docker compose up -d
```

Kafka moderno può essere eseguito in KRaft e può usare nodi con ruoli combinati negli ambienti piccoli.

La complessità effettiva dipende da cluster, disponibilità, sicurezza, monitoraggio, upgrade e competenze del team. Non è corretto descrivere ogni ambiente Redpanda come semplice e ogni ambiente Kafka come complesso.

---

## 21. Che cosa cambierebbe usando Kafka

Le principali modifiche riguarderebbero:

1. servizio broker in `compose.yaml`;
2. immagine Docker;
3. configurazione KRaft;
4. listener e advertised listener;
5. inizializzazione dello storage;
6. health check;
7. strumenti amministrativi;
8. soluzione grafica;
9. procedure operative.

Gran parte del codice Python potrebbe restare invariata.

---

## 22. Che cosa non cambierebbe

Potrebbero restare invariati:

- cinque topic applicativi;
- payload JSON;
- `machine_id` come chiave;
- `correlation_id`;
- Risk Engine;
- policy decisionale;
- logica del Machine Controller;
- risultati `SUCCESS` e `FAILED`;
- feedback dell'agente;
- consumer group;
- commit manuale degli offset.

Questo dimostra la separazione tra logica di dominio e infrastruttura di streaming.

---

## 23. Confronto sintetico

| Aspetto | Redpanda | Apache Kafka |
|---|---|---|
| Tipo | Event streaming distribuito | Event streaming distribuito |
| API Kafka | Compatibile con molte API | Implementazione di riferimento |
| Runtime broker | Nativo, senza JVM | JVM |
| Metadati | Architettura Redpanda basata su Raft | KRaft nelle versioni moderne |
| CLI principale | `rpk` | Strumenti della distribuzione Kafka |
| UI nel progetto | Redpanda Console | Soluzione scelta separatamente |
| Topic e partizioni | Sì | Sì |
| Consumer group e offset | Sì | Sì |
| `confluent-kafka` | Utilizzabile | Utilizzabile |
| Compatibilità totale | No, esistono eccezioni | Non applicabile rispetto a se stesso |

---

## 24. Perché Redpanda è stato scelto

Nel progetto accademico Redpanda ha offerto:

- avvio locale con Docker;
- compatibilità con `confluent-kafka-python`;
- `rpk` per amministrazione;
- Redpanda Console per osservabilità;
- topic partizionati;
- consumer group;
- offset e lag;
- persistenza;
- tracciabilità tramite `correlation_id`;
- supporto al ciclo telemetria, decisione, comando, risultato e feedback.

La scelta è coerente con l'obiettivo di studiare un data plane per agenti.

---

## 25. Quando Kafka potrebbe essere preferibile

Kafka può essere preferibile quando:

- l'organizzazione usa già Kafka;
- esistono procedure e competenze consolidate;
- sono richieste integrazioni validate specificamente su Kafka;
- Kafka Streams è centrale;
- si usa un servizio Kafka gestito già adottato;
- una funzione necessaria rientra nelle eccezioni di compatibilità Redpanda;
- è richiesta esplicitamente l'implementazione Apache.

La scelta dipende dal contesto, non da una superiorità universale.

---

## 26. Errori concettuali da evitare

### "Redpanda è Kafka"

Correzione: Redpanda è un prodotto distinto compatibile con molte API Kafka.

### "Kafka richiede sempre ZooKeeper"

Correzione: Kafka moderno usa KRaft.

### "Compatibilità significa identità completa"

Correzione: funzionalità e API richieste devono essere verificate.

### "Il progetto dimostra che Redpanda è più veloce"

Correzione: il progetto dimostra un'architettura funzionante, non un benchmark.

### "Redpanda prende le decisioni"

Correzione: Redpanda trasporta eventi; il Maintenance Agent prende decisioni.

---

## 27. Relazione con l'agentic data plane

Sia Redpanda sia Kafka possono sostenere un data plane per agenti.

Il carattere agentico non deriva automaticamente dal broker. Deriva dall'insieme di:

- telemetrie;
- memoria;
- valutazione del rischio;
- decisioni;
- comandi;
- risultati;
- feedback;
- correlazione;
- audit.

Nel progetto Redpanda è la tecnologia scelta per implementare il livello di trasporto e persistenza.

---

## 28. Futura modalità multi-macchina

Una versione futura potrebbe usare:

```text
machine-01
machine-02
machine-03
```

Sia Redpanda sia Kafka possono partizionare gli eventi per `machine_id`.

Il Maintenance Agent manterrebbe uno stato separato per ogni macchina, mentre più istanze dello stesso consumer group potrebbero dividersi le partizioni.

---

## 29. Limiti del confronto

Il progetto usa un solo broker, replica singola, pochi eventi, payload JSON piccoli e nessun TLS.

Non contiene un cluster Kafka equivalente.

Il confronto riguarda quindi:

- architettura;
- compatibilità;
- strumenti;
- portabilità del codice;
- ruolo nel progetto.

Non misura prestazioni o resilienza multi-nodo.

---

## 30. Possibile esperimento comparativo futuro

Un confronto sperimentale potrebbe creare due ambienti:

```text
Ambiente A: Redpanda
Ambiente B: Kafka in KRaft
```

Dovrebbero usare stessi topic, partizioni, replica, payload, client, hardware e configurazione di acknowledgment.

Le misure potrebbero includere throughput, latenza p95 e p99, CPU, memoria, tempo di avvio e recovery.

---

## 31. Conclusione

Redpanda e Apache Kafka condividono il modello fondamentale dell'event streaming distribuito.

Redpanda permette al progetto di usare client, topic, partizioni, chiavi, offset e consumer group secondo il modello Kafka-compatible.

Le differenze principali riguardano implementazione, runtime, gestione dei metadati, strumenti operativi, componenti e compatibilità funzionale.

Kafka moderno usa KRaft e non deve essere descritto come obbligatoriamente dipendente da ZooKeeper.

Nel progetto Smart Factory, la logica Python sarebbe in gran parte portabile, mentre infrastruttura Docker e operazioni amministrative dovrebbero essere adattate.

Redpanda è stato scelto per fornire un data plane osservabile e gestibile per il ciclo:

```text
telemetria
→ decisione
→ comando
→ risultato
→ feedback
```

---

## Riferimenti

- Redpanda Documentation, How Redpanda Works: <https://docs.redpanda.com/streaming/current/get-started/architecture/>
- Redpanda Documentation, Kafka Compatibility: <https://docs.redpanda.com/streaming/current/develop/kafka-clients/>
- Redpanda Documentation, rpk: <https://docs.redpanda.com/streaming/current/reference/rpk/>
- Apache Kafka official website: <https://kafka.apache.org/>
- Apache Kafka Documentation, KRaft: <https://kafka.apache.org/40/operations/kraft/>
- Apache Kafka Documentation, Distribution and offset tracking: <https://kafka.apache.org/43/implementation/distribution/>
