# Redpanda nella Smart Factory agentica

## 1. Introduzione

Redpanda è la piattaforma di event streaming utilizzata nel progetto **Redpanda Agentic Smart Factory** per trasportare e conservare gli eventi scambiati tra i componenti.

Nel progetto, Redpanda non calcola il rischio e non decide quali azioni eseguire. Redpanda fornisce invece il livello infrastrutturale attraverso cui circolano:

- telemetrie della macchina;
- decisioni del Maintenance Agent;
- comandi operativi;
- risultati del Machine Controller;
- feedback finali acquisiti dall'agente.

La posizione di Redpanda nell'architettura è:

```text
Machine Simulator
        |
        v
     Redpanda
        |
        v
Maintenance Agent
        |
        v
     Redpanda
        |
        v
Machine Controller
        |
        v
     Redpanda
        |
        v
Maintenance Agent
```

Redpanda costituisce quindi il data plane condiviso del progetto.

---

## 2. Che cos'è Redpanda

Redpanda è una piattaforma distribuita di streaming di eventi compatibile con il protocollo Kafka.

La compatibilità permette alle applicazioni di utilizzare molti client progettati per Kafka senza cambiare il modello di programmazione basato su:

- producer;
- consumer;
- broker;
- topic;
- partizioni;
- chiavi;
- offset;
- consumer group.

Nel progetto Python viene usato il client:

```text
confluent-kafka
```

Lo stesso client viene utilizzato da:

- Machine Simulator;
- Maintenance Agent;
- Machine Controller.

La compatibilità con il protocollo Kafka non significa che Redpanda e Apache Kafka siano lo stesso prodotto. Redpanda implementa le API necessarie alla comunicazione, ma possiede una propria architettura e propri strumenti operativi.

---

## 3. Il modello producer, broker e consumer

Il modello fondamentale è composto da tre ruoli.

### Producer

Un producer crea e pubblica eventi.

Nel progetto sono producer:

```text
Machine Simulator
Maintenance Agent
Machine Controller
```

### Broker

Il broker riceve, memorizza e rende disponibili gli eventi.

Nel progetto il broker è:

```text
Redpanda
```

### Consumer

Un consumer legge ed elabora gli eventi.

Nel progetto sono consumer:

```text
Maintenance Agent
Machine Controller
```

Uno stesso servizio può essere contemporaneamente producer e consumer. Il Maintenance Agent, per esempio, consuma telemetrie e risultati, ma produce decisioni, comandi e feedback.

---

## 4. Ruolo di Redpanda nel progetto

Redpanda collega componenti indipendenti.

Il Machine Simulator non conosce il codice del Maintenance Agent. Il Machine Simulator conosce soltanto:

```text
broker = redpanda:9092
topic = factory.telemetry
```

Il Maintenance Agent non chiama direttamente il Machine Controller. L'agente pubblica un comando su:

```text
factory.commands
```

Il Machine Controller riceve il comando consumando lo stesso topic.

Questo modello elimina la necessità di connessioni dirette tra i componenti.

```text
Comunicazione diretta
Simulator -> Agent -> Controller

Comunicazione event-driven
Simulator -> Redpanda -> Agent -> Redpanda -> Controller
```

---

## 5. Architettura Redpanda in Docker Compose

Nel progetto Redpanda viene eseguito tramite Docker Compose:

```yaml
redpanda:
  image: redpandadata/redpanda:v26.2.1
  container_name: redpanda
```

L'immagine specifica la versione utilizzata dal progetto:

```text
v26.2.1
```

L'uso di una versione esplicita rende l'ambiente più riproducibile rispetto all'uso di un tag generico come `latest`.

Il progetto usa un singolo broker locale, sufficiente per la dimostrazione accademica del flusso degli eventi.

---

## 6. Modalità di sviluppo locale

La configurazione avvia Redpanda in modalità adatta a un ambiente di sviluppo:

```yaml
command:
  - redpanda
  - start
  - --mode
  - dev-container
```

La modalità locale semplifica l'avvio del broker in un container.

Il progetto configura inoltre:

```yaml
- --smp
- "1"
- --memory
- 1G
- --reserve-memory
- 0M
```

Questi parametri limitano le risorse utilizzate dal broker in ambiente didattico:

- una unità di elaborazione;
- un gigabyte di memoria;
- nessuna memoria riservata aggiuntiva.

Questa configurazione non rappresenta un dimensionamento di produzione. È una configurazione controllata per eseguire il progetto su un computer personale.

---

## 7. Listener interno ed esterno

Redpanda espone due indirizzi Kafka:

```yaml
- --kafka-addr
- internal://0.0.0.0:9092,external://0.0.0.0:19092
```

Gli indirizzi pubblicizzati sono:

```yaml
- --advertise-kafka-addr
- internal://redpanda:9092,external://localhost:19092
```

La distinzione è importante.

### Comunicazione tra container

I servizi Docker utilizzano:

```text
redpanda:9092
```

Esempio:

```yaml
environment:
  KAFKA_BROKER: redpanda:9092
```

### Comunicazione dall'host

I programmi avviati direttamente dal computer utilizzano:

```text
localhost:19092
```

Esempio della configurazione Python predefinita:

```python
KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "localhost:19092",
)
```

Questa doppia configurazione permette di usare lo stesso progetto sia dentro Docker sia durante test locali.

---

## 8. Porta amministrativa

Il progetto espone anche:

```yaml
ports:
  - "9644:9644"
```

La porta `9644` è utilizzata dall'API amministrativa di Redpanda.

La presenza di una porta amministrativa separata dalla porta Kafka distingue le operazioni di gestione dalle operazioni di produzione e consumo degli eventi.

---

## 9. Persistenza tramite volume

Redpanda salva i dati nel volume:

```yaml
volumes:
  - redpanda-data:/var/lib/redpanda/data
```

Il volume è dichiarato nella parte finale del file:

```yaml
volumes:
  redpanda-data:
```

La persistenza implica che un semplice arresto dei container non elimina automaticamente:

- topic;
- messaggi;
- offset;
- metadati del broker.

Il comando:

```bash
docker compose down
```

arresta e rimuove i container, ma mantiene il volume.

Il comando:

```bash
docker compose down -v
```

rimuove anche il volume e azzera i dati locali.

Nel progetto questa distinzione è stata usata per ripetere dimostrazioni pulite con topic vuoti.

---

## 10. Health check

Il servizio Redpanda include:

```yaml
healthcheck:
  test:
    - CMD-SHELL
    - rpk cluster health | grep -q 'Healthy:.*true'
  interval: 10s
  timeout: 5s
  retries: 10
```

Il health check esegue periodicamente:

```bash
rpk cluster health
```

Il container viene considerato pronto quando l'output indica:

```text
Healthy: true
```

Gli altri servizi possono quindi dichiarare:

```yaml
depends_on:
  redpanda:
    condition: service_healthy
```

Questo evita che Maintenance Agent e Machine Controller tentino di collegarsi prima che il broker sia disponibile.

Il health check migliora il coordinamento dell'avvio, ma non sostituisce completamente retry e gestione delle connessioni nelle applicazioni.

---

## 11. La rete Docker

I servizi appartengono alla rete:

```yaml
networks:
  - factory-network
```

La rete è dichiarata come:

```yaml
networks:
  factory-network:
```

Docker fornisce la risoluzione dei nomi tra container. Per questo `redpanda` può essere usato come hostname:

```text
redpanda:9092
```

Non è necessario conoscere l'indirizzo IP dinamico del container.

---

## 12. Topic

Un topic è un flusso logico di eventi.

Nel progetto sono presenti cinque topic applicativi:

```text
factory.telemetry
factory.agent-decisions
factory.commands
factory.command-results
factory.agent-feedback
```

Ogni topic ha una responsabilità specifica.

```text
factory.telemetry
Misurazioni della macchina.

factory.agent-decisions
Decisioni del Maintenance Agent.

factory.commands
Azioni operative da eseguire.

factory.command-results
Risultati prodotti dal Machine Controller.

factory.agent-feedback
Feedback acquisiti e pubblicati dall'agente.
```

La separazione per responsabilità rende più semplice osservare, filtrare e mantenere il sistema.

---

## 13. Creazione dei topic con rpk

I topic sono stati creati con comandi come:

```bash
docker exec redpanda rpk topic create \
factory.telemetry --partitions 3
```

La stessa struttura è stata utilizzata per gli altri topic.

La verifica avviene con:

```bash
docker exec redpanda rpk topic list
```

Nel progetto l'output ha mostrato:

```text
NAME                     PARTITIONS  REPLICAS
factory.agent-decisions  3           1
factory.agent-feedback   3           1
factory.command-results  3           1
factory.commands         3           1
factory.telemetry        3           1
```

---

## 14. Partizioni

Ogni topic è suddiviso in tre partizioni.

Le partizioni permettono di:

- distribuire gli eventi;
- aumentare il parallelismo;
- assegnare porzioni del lavoro a consumer differenti;
- mantenere sequenze ordinate indipendenti.

L'ordinamento è garantito all'interno della singola partizione, non globalmente tra tutte le partizioni.

Nel progetto attuale viene usata principalmente una macchina:

```text
machine-01
```

Poiché gli eventi usano la stessa chiave, sono stati osservati principalmente nella partizione `1`.

In una futura versione multi-macchina, chiavi differenti potranno distribuire il carico tra più partizioni.

---

## 15. Replica

L'output dei topic mostra:

```text
REPLICAS = 1
```

Il valore è coerente con l'ambiente locale, che contiene un solo broker.

Una replica singola non offre tolleranza al guasto del broker. Se il broker locale non è disponibile, il data plane non può continuare a servire gli eventi.

Un ambiente distribuito reale utilizzerebbe più broker e un fattore di replica adeguato ai requisiti di disponibilità.

---

## 16. Chiave dei record

Il Machine Simulator pubblica usando:

```python
producer.produce(
    topic=TELEMETRY_TOPIC,
    key=event["machine_id"].encode("utf-8"),
    value=json.dumps(event).encode("utf-8"),
    callback=handle_delivery,
)
```

Il Maintenance Agent segue lo stesso principio:

```python
producer.produce(
    topic=topic,
    key=machine_id.encode("utf-8"),
    value=json.dumps(event).encode("utf-8"),
    callback=delivery_report,
)
```

Il Machine Controller pubblica i risultati con:

```python
key=command_result["machine_id"].encode("utf-8")
```

L'uso di `machine_id` come chiave permette di instradare coerentemente gli eventi della stessa macchina.

---

## 17. Valore dei record

Gli eventi vengono serializzati in JSON:

```python
value=json.dumps(event).encode("utf-8")
```

Il consumer esegue l'operazione inversa:

```python
event = json.loads(
    message_value.decode("utf-8")
)
```

Il formato JSON è stato scelto perché:

- è leggibile in Redpanda Console;
- è semplice da usare in Python;
- rende immediata la dimostrazione accademica;
- permette di aggiungere campi senza cambiare una struttura binaria.

Nel prototipo non è ancora presente uno Schema Registry. La validazione dei campi obbligatori viene eseguita nel codice applicativo.

---

## 18. Producer Python

Un producer viene creato indicando il broker e un identificativo client.

Esempio del Maintenance Agent:

```python
def create_producer() -> Producer:
    configuration = {
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": AGENT_ID,
    }

    return Producer(configuration)
```

Il producer non deve conoscere il consumer. Deve soltanto conoscere il topic sul quale pubblicare.

Questa caratteristica realizza il disaccoppiamento tra servizi.

---

## 19. Conferma di consegna

La pubblicazione usa una callback:

```python
callback=delivery_report
```

La callback controlla se la consegna al broker ha prodotto un errore:

```python
if error is not None:
    print(
        f"Errore di pubblicazione: {error}",
        flush=True,
    )
```

In caso di successo vengono mostrati:

```text
topic
partition
offset
```

Esempio:

```text
Evento pubblicato topic=factory.telemetry partition=1 offset=4
```

La conferma indica che il broker ha accettato il record. Non indica che tutti i consumer abbiano già completato l'elaborazione.

---

## 20. Poll e flush del producer

Dopo la produzione viene chiamato:

```python
producer.poll(0)
```

Il metodo permette al client di servire callback ed eventi interni senza bloccare l'esecuzione.

Nei punti in cui è necessario attendere la consegna dei messaggi in sospeso viene utilizzato:

```python
producer.flush(10)
```

Il timeout limita il tempo massimo di attesa.

Nel simulatore a esecuzione breve, il flush finale è importante perché il processo termina dopo aver pubblicato un numero limitato di eventi.

---

## 21. Consumer Python

Il Maintenance Agent crea un consumer con:

```python
configuration = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
```

Il Machine Controller utilizza una configurazione analoga, ma con:

```python
"auto.offset.reset": "latest"
```

Le proprietà principali sono:

```text
bootstrap.servers
Indirizzo del broker.

group.id
Identificativo del consumer group.

auto.offset.reset
Posizione iniziale se non esiste un offset valido.

enable.auto.commit
Controllo del commit automatico.
```

---

## 22. Sottoscrizione ai topic

Il Machine Controller ascolta:

```python
consumer.subscribe([COMMANDS_TOPIC])
```

Il Maintenance Agent ascolta due topic:

```python
consumer.subscribe(
    [
        TELEMETRY_TOPIC,
        COMMAND_RESULTS_TOPIC,
    ]
)
```

Questa doppia sottoscrizione permette all'agente di:

- percepire la macchina;
- osservare l'esito delle azioni richieste.

Il metodo `message.topic()` consente di distinguere il flusso ricevuto.

---

## 23. Consumer group

Un consumer group coordina una o più istanze della stessa applicazione.

Il progetto usa:

```text
maintenance-agent-group
machine-controller-group
```

La lista viene verificata con:

```bash
docker exec redpanda rpk group list
```

L'output osservato è stato:

```text
BROKER  GROUP                     STATE
0       machine-controller-group  Stable
0       maintenance-agent-group   Stable
```

Lo stato `Stable` indica che i membri sono registrati e l'assegnazione delle partizioni è stabile.

---

## 24. Offset

Un offset è la posizione di un record all'interno di una partizione.

Esempio:

```text
partition 1, offset 0
partition 1, offset 1
partition 1, offset 2
```

Gli offset permettono al consumer group di ricordare fino a quale record è arrivato.

Gli offset sono tecnici e locali alla partizione. Non sostituiscono gli identificativi applicativi:

```text
event_id
Identifica la telemetria.

correlation_id
Collega la catena end-to-end.

offset
Indica la posizione nel log.
```

---

## 25. Commit manuale degli offset

Il progetto disabilita il commit automatico:

```python
"enable.auto.commit": False
```

Dopo l'elaborazione viene eseguito:

```python
consumer.commit(
    message=message,
    asynchronous=False,
)
```

Il flusso è:

```text
lettura
→ validazione
→ elaborazione
→ pubblicazione degli eventi derivati
→ commit dell'offset
```

Il commit sincrono semplifica la verifica del prototipo e garantisce che il programma attenda la risposta relativa al commit.

---

## 26. Auto offset reset

`auto.offset.reset` non decide sempre da dove partire. Viene usato quando il consumer group non possiede un offset valido per la partizione.

Il Maintenance Agent usa:

```text
earliest
```

Un nuovo gruppo può quindi leggere dai record più vecchi ancora disponibili.

Il Machine Controller usa:

```text
latest
```

Un nuovo gruppo parte dalla posizione più recente e attende nuovi record.

Questa scelta è stata utile durante lo sviluppo per evitare l'elaborazione di migliaia di vecchi comandi di test.

---

## 27. Lag

Il lag rappresenta il numero di record che un consumer group deve ancora elaborare.

Il comando:

```bash
docker exec redpanda rpk group describe maintenance-agent-group
```

ha mostrato:

```text
TOTAL-LAG = 0
```

Per il topic di telemetria è stato osservato:

```text
CURRENT-OFFSET = 5
LOG-END-OFFSET = 5
LAG = 0
```

Il comando:

```bash
docker exec redpanda rpk group describe machine-controller-group
```

ha mostrato:

```text
CURRENT-OFFSET = 3
LOG-END-OFFSET = 3
LAG = 0
```

Questi dati significano che:

- il Maintenance Agent aveva elaborato tutte le cinque telemetrie;
- il Machine Controller aveva elaborato tutti i tre comandi;
- nessun messaggio applicativo era in attesa.

---

## 28. Topic interno `__consumer_offsets`

Redpanda usa un topic interno per conservare gli offset dei consumer group:

```text
__consumer_offsets
```

Questo topic non appartiene al dominio della Smart Factory e non deve essere modificato manualmente.

Nel progetto, la descrizione dei consumer group ha mostrato il coordinatore associato a una partizione di `__consumer_offsets`.

Il topic tecnico permette ai consumer group di riprendere dalle posizioni registrate dopo un riavvio.

---

## 29. rpk

`rpk` è lo strumento a riga di comando utilizzato per amministrare e osservare Redpanda.

Nel progetto è stato usato per:

- creare topic;
- eliminare topic;
- elencare topic;
- produrre record di test;
- consumare record;
- descrivere consumer group;
- controllare il lag;
- verificare la salute del cluster.

Esempi:

```bash
rpk topic list
```

```bash
rpk topic create factory.telemetry --partitions 3
```

```bash
rpk group describe maintenance-agent-group
```

Poiché `rpk` è disponibile nel container Redpanda, i comandi vengono eseguiti con:

```bash
docker exec redpanda rpk ...
```

---

## 30. Redpanda Console

Redpanda Console viene eseguita con:

```yaml
redpanda-console:
  image: redpandadata/console:v3.9.0
```

La Console si collega al broker tramite:

```yaml
environment:
  KAFKA_BROKERS: redpanda:9092
```

L'interfaccia è esposta su:

```text
http://localhost:8080
```

La Console permette di osservare il data plane senza scrivere codice aggiuntivo.

Nel progetto è stata usata per:

- verificare il numero di messaggi;
- aprire il JSON degli eventi;
- controllare `selected_action`;
- controllare `SUCCESS` e `FAILED`;
- seguire il `correlation_id`;
- osservare topic e partizioni;
- controllare i consumer group.

---

## 31. Correlation ID

Ogni evento di telemetria contiene un `correlation_id`.

Lo stesso valore viene propagato in:

```text
factory.telemetry
factory.agent-decisions
factory.commands
factory.command-results
factory.agent-feedback
```

Esempio:

```text
correlation_id = abc-125
```

La ricerca dello stesso valore nei topic permette di ricostruire:

```text
telemetria
→ decisione
→ comando
→ risultato
→ feedback
```

Redpanda conserva ciascun record nel topic appropriato. Il collegamento semantico tra i record è definito dal campo applicativo `correlation_id`.

---

## 32. Flusso di un evento normale

Una telemetria normale può produrre:

```text
factory.telemetry
        |
        v
factory.agent-decisions
selected_action = NO_ACTION
```

Il flusso termina senza comando.

Non vengono creati record in:

```text
factory.commands
factory.command-results
factory.agent-feedback
```

Questo comportamento dimostra che non tutti gli eventi devono attraversare tutti i topic.

---

## 33. Flusso di un evento operativo

Una telemetria rischiosa può produrre:

```text
factory.telemetry
        |
        v
factory.agent-decisions
selected_action = REDUCE_SPEED
        |
        v
factory.commands
        |
        v
factory.command-results
result = SUCCESS
        |
        v
factory.agent-feedback
feedback_status = PROCESSED
```

Ogni passaggio possiede un identificativo specifico e lo stesso `correlation_id`.

---

## 34. Risultato positivo

Un comando riuscito può generare:

```json
{
  "action": "REDUCE_SPEED",
  "result": "SUCCESS",
  "failure_reason": null,
  "machine_status": "REDUCED_SPEED",
  "previous_speed": 1400,
  "current_speed": 900
}
```

Il risultato indica che lo stato operativo è cambiato.

Il Maintenance Agent riceve il record e pubblica il feedback corrispondente.

---

## 35. Risultato negativo

Un comando fallito può generare:

```json
{
  "action": "EMERGENCY_STOP",
  "result": "FAILED",
  "failure_reason": "Simulated actuator communication failure",
  "machine_status": "RUNNING",
  "previous_speed": 1400,
  "current_speed": 1400
}
```

Il risultato negativo è importante perché separa:

```text
azione richiesta
```

da:

```text
azione realmente applicata
```

Il feedback dell'agente dimostra che il fallimento è stato acquisito e memorizzato.

---

## 36. Modalità del Machine Controller

Il progetto configura:

```yaml
CONTROLLER_MODE: MIXED
```

La modalità `MIXED` permette di simulare sia risultati positivi sia risultati negativi.

Questa modalità non appartiene a Redpanda. È una regola applicativa del Machine Controller.

Redpanda svolge però un ruolo essenziale perché conserva e distribuisce entrambi i tipi di risultato senza interpretarli.

```text
Redpanda
Trasporta SUCCESS e FAILED.

Machine Controller
Decide il risultato simulato.

Maintenance Agent
Interpreta il risultato come feedback.
```

---

## 37. Redpanda come data plane neutrale

Redpanda non applica la politica decisionale del progetto.

Redpanda non decide:

- se la temperatura è troppo alta;
- quale rischio assegnare;
- quale comando inviare;
- se un comando deve fallire;
- come reagire a un fallimento.

Redpanda garantisce invece il canale attraverso cui queste informazioni vengono scambiate e mantenute.

Questa neutralità è una proprietà importante del data plane: la logica di dominio rimane nei servizi applicativi.

---

## 38. Ripetizione controllata degli esperimenti

Il simulatore è configurato con:

```yaml
restart: "no"
```

Il processo genera un blocco limitato di eventi e termina.

Questa scelta evita un flusso infinito e rende più semplice:

- contare i record;
- seguire ogni `correlation_id`;
- confrontare topic differenti;
- ripetere una dimostrazione;
- osservare successi e fallimenti.

Redpanda mantiene i record anche dopo la conclusione del simulatore.

---

## 39. Pulizia dei topic

Durante lo sviluppo è stato necessario ripartire da topic vuoti.

I topic applicativi possono essere eliminati con:

```bash
docker exec redpanda rpk topic delete \
factory.telemetry \
factory.agent-decisions \
factory.commands \
factory.command-results \
factory.agent-feedback
```

Successivamente vengono ricreati.

Questa procedura è adatta alla demo locale, ma non rappresenta una normale operazione su un ambiente di produzione.

In produzione si configurerebbero retention, archiviazione, sicurezza e procedure controllate di gestione dei dati.

---

## 40. Retention

La retention stabilisce per quanto tempo o fino a quale dimensione i record vengono conservati.

Nel prototipo non è stata definita una retention specifica per i topic applicativi. Vengono quindi utilizzati i valori configurati nel broker.

Una futura configurazione potrebbe differenziare:

```text
factory.telemetry
Retention più breve per dati frequenti.

factory.agent-decisions
Retention più lunga per audit.

factory.commands
Retention coerente con le esigenze operative.

factory.command-results
Retention utile alla verifica delle esecuzioni.

factory.agent-feedback
Retention utile all'audit del ciclo agentico.
```

---

## 41. Scalabilità

La presenza di tre partizioni permette una futura scalabilità orizzontale.

Più istanze del Maintenance Agent con lo stesso `group.id` possono dividere le partizioni.

Il grado massimo di parallelismo utile per un singolo consumer group è legato al numero di partizioni disponibili.

Con tre partizioni:

```text
1 consumer
Può leggere tutte le partizioni.

2 consumer
Possono dividersi le partizioni.

3 consumer
Possono ricevere una partizione ciascuno.

Più di 3 consumer
Alcuni membri possono rimanere inattivi.
```

Nel progetto attuale è presente un solo membro per gruppo.

---

## 42. Estensione multi-macchina

Il progetto è predisposto per più macchine grazie alla chiave `machine_id`.

Una possibile estensione comprende:

```text
machine-01
machine-02
machine-03
```

Ogni macchina potrebbe produrre un profilo differente:

```text
machine-01 → stabile
machine-02 → degradante
machine-03 → critico
```

Gli stessi topic continuerebbero a essere utilizzati.

Redpanda distribuirebbe i record in base alle chiavi, mentre il Maintenance Agent manterrebbe uno stato separato per ogni macchina.

---

## 43. Compatibilità Kafka nel progetto

Il progetto usa costrutti standard del protocollo Kafka:

- bootstrap server;
- producer;
- consumer;
- topic;
- partizioni;
- chiavi;
- offset;
- consumer group;
- commit manuale.

La libreria Python:

```text
confluent-kafka
```

può comunicare con Redpanda attraverso le API compatibili.

Questo permette di usare strumenti e conoscenze dell'ecosistema Kafka mantenendo Redpanda come broker del progetto.

La compatibilità non deve essere interpretata come identità completa. Alcune funzionalità, strumenti operativi e dettagli architetturali differiscono.

---

## 44. Perché Redpanda è adatto al progetto

Redpanda soddisfa le esigenze principali del prototipo:

1. espone API compatibili con client Kafka;
2. supporta producer e consumer indipendenti;
3. conserva eventi in topic partizionati;
4. gestisce consumer group e offset;
5. offre `rpk` per amministrazione e verifica;
6. offre Redpanda Console per osservabilità;
7. può essere eseguito localmente con Docker;
8. permette di seguire il ciclo agentico completo;
9. prepara il progetto alla scalabilità multi-macchina;
10. separa la logica applicativa dall'infrastruttura dati.

---

## 45. Cosa Redpanda non fa nel progetto

È importante non attribuire a Redpanda responsabilità applicative.

Redpanda non:

- genera la telemetria;
- calcola medie;
- calcola trend;
- valuta il rischio;
- seleziona azioni;
- simula l'attuatore;
- decide successi o fallimenti;
- aggiorna lo stato dell'agente.

Queste responsabilità appartengono rispettivamente a:

```text
Machine Simulator
Maintenance Agent
Machine Controller
```

Redpanda rende possibile lo scambio affidabile e osservabile degli eventi prodotti da questi componenti.

---

## 46. Limiti della configurazione locale

### Broker singolo

Il progetto non dimostra replica tra più nodi o tolleranza al guasto del cluster.

### Replica singola

Ogni partizione possiede una sola copia.

### Protezione semplificata

Non sono configurati TLS, SASL o ACL.

### Nessuno Schema Registry usato dall'applicazione

I payload JSON sono validati nel codice.

### Retention non personalizzata

I topic utilizzano la configurazione predefinita.

### Stato applicativo non persistente

Maintenance Agent e Machine Controller conservano parte dello stato in memoria.

Questi limiti sono accettabili per un prototipo didattico, ma devono essere considerati in una progettazione di produzione.

---

## 47. Possibili sviluppi futuri

Il ruolo di Redpanda potrebbe essere esteso con:

- cluster multi-broker;
- fattore di replica maggiore di uno;
- configurazione esplicita della retention;
- Schema Registry;
- sicurezza TLS e SASL;
- autorizzazioni per topic;
- dead-letter topic;
- più istanze dei consumer;
- più macchine;
- metriche e dashboard;
- retry e reprocessing;
- archiviazione a lungo termine;
- trasformazioni streaming;
- test automatici di resilienza.

---

## 48. Evidenze sperimentali

Le verifiche eseguite nel progetto hanno mostrato:

```text
5 topic applicativi
3 partizioni per topic
1 replica per topic
2 consumer group
stato Stable
lag totale pari a 0
```

Il Maintenance Agent aveva:

```text
factory.telemetry
CURRENT-OFFSET = 5
LOG-END-OFFSET = 5
LAG = 0
```

Il Machine Controller aveva:

```text
factory.commands
CURRENT-OFFSET = 3
LOG-END-OFFSET = 3
LAG = 0
```

Queste evidenze mostrano che Redpanda ha:

- ricevuto gli eventi;
- conservato i record;
- assegnato partizioni e offset;
- coordinato i consumer group;
- registrato l'avanzamento;
- consentito ai consumer di completare il flusso.

---

## 49. Conclusione

Redpanda è il centro infrastrutturale della Smart Factory agentica.

Il progetto utilizza Redpanda per collegare:

```text
Machine Simulator
Maintenance Agent
Machine Controller
```

I cinque topic rappresentano le fasi del ciclo:

```text
factory.telemetry
Percezione.

factory.agent-decisions
Decisione.

factory.commands
Azione richiesta.

factory.command-results
Risultato dell'esecuzione.

factory.agent-feedback
Acquisizione del risultato.
```

Partizioni, chiavi, offset e consumer group permettono di mantenere ordine per macchina, registrare l'avanzamento e preparare il sistema alla scalabilità.

`rpk` e Redpanda Console rendono il flusso osservabile e verificabile.

Redpanda non sostituisce la logica dell'agente. Redpanda fornisce il data plane persistente e disaccoppiato che consente all'agente di percepire, decidere, agire e osservare il risultato.

---

## Riferimenti

- Redpanda Documentation, Kafka client compatibility: <https://docs.redpanda.com/streaming/current/develop/kafka-clients/>
- Redpanda Documentation, rpk commands: <https://docs.redpanda.com/streaming/current/reference/rpk/>
- Redpanda Documentation, rpk topic: <https://docs.redpanda.com/streaming/current/reference/rpk/rpk-topic/rpk-topic/>
- Redpanda Documentation, topic creation: <https://docs.redpanda.com/streaming/current/reference/rpk/rpk-topic/rpk-topic-create/>
- Redpanda Documentation, consumer groups: <https://docs.redpanda.com/streaming/current/reference/rpk/rpk-group/rpk-group/>
- Redpanda Documentation, consumer offsets: <https://docs.redpanda.com/streaming/current/develop/consume-data/consumer-offsets/>
- Redpanda Documentation, topic properties: <https://docs.redpanda.com/streaming/current/manage/cluster-maintenance/topic-property-configuration/>
