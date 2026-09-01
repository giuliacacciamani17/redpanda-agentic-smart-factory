# Agentic Data Plane nella Smart Factory

## 1. Introduzione

Un data plane è l'infrastruttura responsabile del trasporto effettivo dei dati tra i componenti di un sistema. In una Smart Factory, il data plane permette a sensori, simulatori, agenti software, controller e strumenti di osservabilità di scambiarsi eventi senza dipendere da collegamenti diretti tra ogni coppia di servizi.

Nel progetto **Redpanda Agentic Smart Factory**, Redpanda svolge il ruolo di data plane condiviso. I componenti applicativi non si chiamano direttamente tra loro. Ogni componente pubblica o consuma eventi attraverso topic dedicati.

Il flusso completo è:

```text
Machine Simulator
        |
        v
factory.telemetry
        |
        v
Maintenance Agent
        |
        +--> factory.agent-decisions
        |
        +--> factory.commands
                 |
                 v
        Machine Controller
                 |
                 v
      factory.command-results
                 |
                 v
        Maintenance Agent
                 |
                 v
       factory.agent-feedback
```

Questa architettura consente di separare chiaramente cinque momenti:

1. percezione dell'ambiente;
2. valutazione e decisione;
3. richiesta di un'azione;
4. esecuzione dell'azione;
5. acquisizione del feedback.

---

## 2. Che cos'è un data plane

In un sistema distribuito è utile distinguere tra **control plane** e **data plane**.

Il control plane definisce configurazioni, regole e politiche. Per esempio, decide quali servizi devono esistere, quali topic devono essere disponibili e quali soglie definiscono un rischio critico.

Il data plane trasporta invece i dati operativi prodotti durante l'esecuzione. Nel progetto, questi dati comprendono:

- temperature e vibrazioni;
- decisioni del Maintenance Agent;
- comandi operativi;
- risultati prodotti dal Machine Controller;
- feedback finali acquisiti dall'agente.

In forma sintetica:

```text
Control plane
Definisce come il sistema deve funzionare.

Data plane
Trasporta ciò che accade durante il funzionamento.
```

Il file `compose.yaml` appartiene principalmente alla configurazione del sistema. Le comunicazioni che attraversano i topic Redpanda costituiscono invece il data plane operativo.

---

## 3. Che cos'è un agentic data plane

Un agentic data plane è un'infrastruttura dati progettata per supportare il ciclo operativo di uno o più agenti software.

Un agente necessita di dati per:

- percepire lo stato dell'ambiente;
- mantenere una memoria;
- prendere decisioni;
- richiedere azioni;
- osservare il risultato delle azioni;
- aggiornare il proprio stato interno.

Di conseguenza, il data plane non trasporta soltanto dati grezzi. Trasporta anche le conseguenze del ragionamento dell'agente.

Nel progetto sono presenti cinque categorie di eventi:

```text
Percezioni
Decisioni
Comandi
Risultati
Feedback
```

Questa struttura rende il data plane "agentic" perché sostiene l'intero ciclo percezione, decisione, azione e feedback.

---

## 4. Perché non utilizzare chiamate dirette

Un'alternativa sarebbe fare in modo che il Machine Simulator chiami direttamente il Maintenance Agent e che il Maintenance Agent chiami direttamente il Machine Controller.

Questa soluzione creerebbe dipendenze strette:

```text
Simulator dipende da Agent
Agent dipende da Controller
Controller deve essere disponibile immediatamente
```

Nel progetto, invece, i componenti comunicano attraverso Redpanda:

```text
Producer
  |
  v
Topic persistente
  |
  v
Consumer
```

Il producer deve conoscere soltanto:

- il broker;
- il nome del topic;
- la chiave;
- il valore dell'evento.

Il producer non deve conoscere l'indirizzo o l'implementazione del consumer.

Questo disaccoppiamento permette di modificare, riavviare o sostituire un componente senza riscrivere tutti gli altri.

---

## 5. I componenti del progetto

### 5.1 Machine Simulator

Il Machine Simulator rappresenta il macchinario o il dispositivo edge che produce telemetria.

Il simulatore pubblica su:

```text
factory.telemetry
```

Il codice usa il `machine_id` come chiave:

```python
producer.produce(
    topic=TELEMETRY_TOPIC,
    key=event["machine_id"].encode("utf-8"),
    value=json.dumps(event).encode("utf-8"),
    callback=handle_delivery,
)
```

Il simulatore non calcola il rischio e non seleziona azioni. Il suo compito è descrivere lo stato osservato della macchina.

### 5.2 Maintenance Agent

Il Maintenance Agent consuma telemetria e risultati dei comandi:

```python
consumer.subscribe(
    [
        TELEMETRY_TOPIC,
        COMMAND_RESULTS_TOPIC,
    ]
)
```

La doppia sottoscrizione rappresenta due fasi diverse:

```text
factory.telemetry
Percezione iniziale.

factory.command-results
Osservazione dell'effetto dell'azione.
```

L'agente pubblica su tre topic:

- `factory.agent-decisions`;
- `factory.commands`;
- `factory.agent-feedback`.

### 5.3 Machine Controller

Il Machine Controller consuma:

```text
factory.commands
```

Dopo aver simulato l'esecuzione, pubblica su:

```text
factory.command-results
```

Il Controller non decide quale azione sia appropriata. Esegue l'azione già selezionata dal Maintenance Agent.

### 5.4 Redpanda Console

Redpanda Console permette di osservare:

- topic;
- messaggi;
- chiavi;
- partizioni;
- offset;
- consumer group;
- lag.

La Console non fa parte della logica decisionale. È uno strumento di osservabilità del data plane.

---

## 6. I topic del data plane

### 6.1 `factory.telemetry`

Producer:

```text
Machine Simulator
```

Consumer:

```text
Maintenance Agent
```

Contenuto principale:

```json
{
  "event_id": "identificativo-evento",
  "correlation_id": "identificativo-correlazione",
  "machine_id": "machine-01",
  "timestamp": "data-e-ora",
  "temperature": 84.2,
  "vibration": 5.6,
  "speed": 1400,
  "energy_consumption": 122.5
}
```

Questo topic rappresenta ciò che l'agente percepisce.

### 6.2 `factory.agent-decisions`

Producer:

```text
Maintenance Agent
```

Contenuto principale:

```json
{
  "decision_id": "identificativo-decisione",
  "source_event_id": "identificativo-telemetria",
  "correlation_id": "identificativo-correlazione",
  "machine_id": "machine-01",
  "risk_score": 0.56,
  "previous_action": "MONITOR",
  "selected_action": "REDUCE_SPEED",
  "reason": "The risk level requires a speed reduction"
}
```

Il topic conserva tutte le decisioni, comprese `NO_ACTION` e `MONITOR`. Costituisce quindi il registro di audit del ragionamento dell'agente.

### 6.3 `factory.commands`

Producer:

```text
Maintenance Agent
```

Consumer:

```text
Machine Controller
```

Questo topic contiene soltanto azioni operative:

```text
REDUCE_SPEED
REQUEST_INSPECTION
EMERGENCY_STOP
```

`NO_ACTION` e `MONITOR` non generano un comando, perché non richiedono una modifica diretta del macchinario.

### 6.4 `factory.command-results`

Producer:

```text
Machine Controller
```

Consumer:

```text
Maintenance Agent
```

Il topic contiene l'esito tecnico dell'esecuzione:

```json
{
  "result_id": "identificativo-risultato",
  "command_id": "identificativo-comando",
  "correlation_id": "identificativo-correlazione",
  "machine_id": "machine-01",
  "action": "REDUCE_SPEED",
  "result": "SUCCESS",
  "machine_status": "REDUCED_SPEED",
  "previous_speed": 1400,
  "current_speed": 900
}
```

Il risultato può essere `SUCCESS` oppure `FAILED`.

### 6.5 `factory.agent-feedback`

Producer:

```text
Maintenance Agent
```

Il topic dimostra che l'agente ha ricevuto il risultato del Controller e ha aggiornato il proprio stato interno.

Esempio:

```json
{
  "feedback_id": "identificativo-feedback",
  "command_id": "identificativo-comando",
  "result_id": "identificativo-risultato",
  "correlation_id": "identificativo-correlazione",
  "machine_id": "machine-01",
  "action": "EMERGENCY_STOP",
  "command_result": "FAILED",
  "machine_status": "RUNNING",
  "feedback_status": "PROCESSED"
}
```

È importante distinguere:

```text
command_result = FAILED
Il Controller non è riuscito a eseguire il comando.

feedback_status = PROCESSED
L'agente ha ricevuto e interpretato correttamente il fallimento.
```

---

## 7. Producer e consumer

Un producer pubblica eventi. Un consumer legge ed elabora eventi.

Nel progetto, uno stesso servizio può svolgere entrambi i ruoli.

Il Maintenance Agent è:

- consumer di `factory.telemetry`;
- producer di `factory.agent-decisions`;
- producer di `factory.commands`;
- consumer di `factory.command-results`;
- producer di `factory.agent-feedback`.

Il Machine Controller è:

- consumer di `factory.commands`;
- producer di `factory.command-results`.

Questa combinazione crea una pipeline asincrona e bidirezionale a livello logico, pur mantenendo ogni collegamento fisico basato su topic indipendenti.

---

## 8. Comunicazione asincrona

La comunicazione è asincrona perché il producer non attende che il consumer completi l'intera elaborazione applicativa.

Il producer invia l'evento a Redpanda:

```python
producer.produce(
    topic=topic,
    key=machine_id.encode("utf-8"),
    value=json.dumps(event).encode("utf-8"),
    callback=delivery_report,
)
```

La callback conferma che Redpanda ha accettato il messaggio e comunica:

- topic;
- partizione;
- offset.

Il consumer recupera successivamente gli eventi disponibili.

Questo modello permette al producer di continuare a funzionare anche se il consumer è temporaneamente più lento.

---

## 9. Persistenza degli eventi

Redpanda conserva i record nei topic secondo la configurazione di retention.

La persistenza permette di:

- osservare eventi precedenti;
- riavviare un consumer;
- recuperare messaggi non ancora elaborati;
- costruire audit;
- riprodurre esperimenti;
- analizzare una catena tramite `correlation_id`.

L'arresto del Machine Simulator non cancella la telemetria già pubblicata. Allo stesso modo, l'arresto del Maintenance Agent non elimina decisioni e comandi già presenti.

---

## 10. Topic e partizioni

Ogni topic del progetto è stato creato con:

```text
PARTITIONS = 3
REPLICAS = 1
```

La replica è `1` perché l'ambiente locale utilizza un solo broker Redpanda.

Le partizioni dividono il log di un topic in sequenze ordinate indipendenti.

La presenza di più partizioni consente:

- distribuzione dei dati;
- elaborazione parallela;
- scalabilità dei consumer group;
- separazione del carico.

L'ordinamento globale tra tutte le partizioni non è garantito. L'ordine è garantito all'interno della singola partizione.

---

## 11. Chiave e ordinamento per macchina

Tutti i producer usano `machine_id` come chiave:

```python
key=event["machine_id"].encode("utf-8")
```

oppure:

```python
key=command_result["machine_id"].encode("utf-8")
```

L'obiettivo è fare in modo che gli eventi della stessa macchina vengano instradati coerentemente nella stessa partizione del relativo topic.

Nel test con `machine-01`, gli eventi osservati sono stati registrati nella partizione `1`.

Questo permette di preservare l'ordine degli eventi riferiti alla stessa macchina:

```text
telemetria precedente
→ telemetria successiva
```

In una futura simulazione multi-macchina, chiavi differenti permetteranno di distribuire gli eventi tra le partizioni mantenendo l'ordine per singola macchina.

---

## 12. Offset

Ogni record riceve un offset immutabile all'interno della partizione.

Esempio:

```text
partition = 1

offset = 0
offset = 1
offset = 2
```

L'offset indica la posizione del record nel log della partizione.

Nel progetto abbiamo osservato:

```text
factory.telemetry
partizione 1
LOG-END-OFFSET = 5
```

Questo indica che nella partizione erano stati aggiunti cinque record e che l'offset successivo disponibile era `5`.

L'offset non sostituisce `event_id` o `correlation_id`:

```text
offset
Posizione tecnica nel log della partizione.

event_id
Identità dell'evento applicativo.

correlation_id
Collegamento logico tra eventi su topic differenti.
```

---

## 13. Commit manuale degli offset

Il Maintenance Agent configura:

```python
"enable.auto.commit": False
```

Questa scelta impedisce il commit automatico.

Dopo l'elaborazione viene eseguito un commit esplicito:

```python
consumer.commit(
    message=message,
    asynchronous=False,
)
```

Il significato è:

```text
1. il consumer legge il messaggio;
2. il consumer elabora il messaggio;
3. il consumer pubblica eventuali eventi derivati;
4. il consumer registra l'offset elaborato.
```

Questo ordine riduce il rischio di segnare un evento come completato prima che l'elaborazione sia realmente terminata.

Il prototipo non implementa una transazione distribuita tra input e output, quindi non garantisce esattamente una sola elaborazione in ogni possibile scenario di guasto. Il comportamento è più vicino a una gestione manuale orientata all'at-least-once.

---

## 14. Consumer group

Un consumer group identifica una o più istanze della stessa applicazione consumer.

Il Maintenance Agent usa:

```text
maintenance-agent-group
```

Il Machine Controller usa:

```text
machine-controller-group
```

La configurazione è:

```python
configuration = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
```

Il consumer group permette di:

- registrare gli offset;
- riprendere dopo un riavvio;
- dividere le partizioni tra più istanze;
- osservare il lag;
- scalare orizzontalmente.

Nel progetto entrambi i gruppi risultano nello stato:

```text
Stable
```

Questo indica che i membri sono registrati e che l'assegnazione delle partizioni è stabile.

---

## 15. Auto offset reset

Il Maintenance Agent usa:

```python
"auto.offset.reset": "earliest"
```

Questa opzione viene applicata quando il consumer group non possiede ancora un offset valido. In quel caso, il consumer parte dai record più vecchi disponibili.

Il Machine Controller usa:

```python
"auto.offset.reset": "latest"
```

Con un nuovo consumer group, il Controller inizia dai record successivi alla posizione più recente disponibile.

Nel progetto questa scelta è stata usata per evitare che un nuovo Controller elaborasse tutti i numerosi comandi storici prodotti durante i primi test.

Dopo il primo commit valido degli offset, la ripartenza utilizza la posizione salvata del consumer group.

---

## 16. Lag

Il lag misura la distanza tra:

```text
ultimo record disponibile
```

e:

```text
ultimo record elaborato dal consumer group
```

Nel progetto sono stati osservati:

```text
maintenance-agent-group
TOTAL-LAG = 0
```

```text
machine-controller-group
TOTAL-LAG = 0
```

Per il Maintenance Agent:

```text
factory.telemetry
CURRENT-OFFSET = 5
LOG-END-OFFSET = 5
LAG = 0
```

Per il Machine Controller:

```text
factory.commands
CURRENT-OFFSET = 3
LOG-END-OFFSET = 3
LAG = 0
```

Questo dimostra che:

1. erano disponibili cinque telemetrie;
2. il Maintenance Agent le aveva elaborate tutte;
3. tre decisioni avevano prodotto comandi;
4. il Machine Controller aveva elaborato tutti e tre i comandi;
5. non erano presenti record applicativi in attesa.

---

## 17. Relazione tra quantità di eventi

Il numero di eventi può cambiare tra i topic.

Un esempio del progetto è:

```text
5 telemetrie
→ 5 decisioni
→ 3 comandi
→ 3 risultati
→ 3 feedback
```

La differenza è causata dalla politica decisionale:

```text
NO_ACTION
→ nessun comando

MONITOR
→ nessun comando

REDUCE_SPEED
→ comando

REQUEST_INSPECTION
→ comando

EMERGENCY_STOP
→ comando
```

Il data plane non copia semplicemente ogni evento in tutti i topic. Trasporta eventi derivati in base alla logica applicativa dell'agente.

---

## 18. Correlation ID

Il `correlation_id` permette di seguire una singola catena attraverso topic differenti.

Esempio:

```text
correlation_id = abc-125
```

La ricerca dello stesso identificativo consente di trovare:

```text
factory.telemetry
Telemetria iniziale.

factory.agent-decisions
Decisione derivata.

factory.commands
Comando operativo.

factory.command-results
Risultato dell'esecuzione.

factory.agent-feedback
Conferma dell'acquisizione del risultato.
```

Il `correlation_id` risolve un problema che gli offset non possono risolvere. Ogni topic possiede partizioni e offset propri, quindi un offset di `factory.telemetry` non può identificare direttamente un record in `factory.command-results`.

---

## 19. Identificativi applicativi

Ogni fase usa un identificativo specifico:

```text
event_id
Identifica la telemetria.

decision_id
Identifica la decisione.

command_id
Identifica il comando.

result_id
Identifica il risultato.

feedback_id
Identifica il feedback.

correlation_id
Collega l'intera catena.
```

Questa struttura rende possibile auditare sia il singolo record sia il processo completo.

---

## 20. Ciclo di feedback

Il ciclo non termina con l'invio del comando.

Il flusso completo è:

```text
Telemetria
→ Decisione
→ Comando
→ Risultato
→ Feedback
```

Il Machine Controller pubblica il risultato su:

```text
factory.command-results
```

Il Maintenance Agent consuma il risultato e aggiorna lo stato interno:

```python
state.update_command_result(command_result)
```

Infine pubblica su:

```text
factory.agent-feedback
```

Il feedback rende osservabile che il risultato è stato acquisito dall'agente.

---

## 21. Successo e fallimento

L'invio di un comando non garantisce che l'azione sia stata applicata.

Il Controller può produrre:

```text
SUCCESS
FAILED
```

Un risultato positivo può contenere:

```json
{
  "action": "REDUCE_SPEED",
  "result": "SUCCESS",
  "previous_speed": 1400,
  "current_speed": 900,
  "machine_status": "REDUCED_SPEED"
}
```

Un risultato negativo può contenere:

```json
{
  "action": "EMERGENCY_STOP",
  "result": "FAILED",
  "failure_reason": "Simulated actuator communication failure",
  "machine_status": "RUNNING"
}
```

Il feedback consente all'agente di distinguere tra azione richiesta e azione realmente riuscita.

---

## 22. Disaccoppiamento temporale

I componenti non devono essere attivi nello stesso istante per tutta la durata del sistema.

Per esempio:

```text
1. il Machine Simulator pubblica una telemetria;
2. il Maintenance Agent è temporaneamente arrestato;
3. Redpanda conserva la telemetria;
4. il Maintenance Agent riparte;
5. il consumer group riprende dall'offset salvato;
6. l'evento viene elaborato.
```

Questa proprietà si chiama disaccoppiamento temporale.

Il disaccoppiamento aumenta la resilienza rispetto a una chiamata diretta, che fallirebbe immediatamente se il destinatario non fosse disponibile.

---

## 23. Configurazione Docker del data plane

Il broker interno è raggiungibile tramite:

```text
redpanda:9092
```

Le applicazioni containerizzate configurano:

```yaml
environment:
  KAFKA_BROKER: redpanda:9092
```

Dall'host, il broker è invece esposto su:

```text
localhost:19092
```

Questa distinzione è necessaria perché:

- i container comunicano tramite la rete Docker;
- gli strumenti eseguiti dall'host usano la porta pubblicata.

I servizi appartengono alla rete:

```text
factory-network
```

Il volume:

```text
redpanda-data
```

conserva i dati del broker tra arresti e riavvii, finché il volume non viene eliminato esplicitamente.

---

## 24. Health check e ordine di avvio

Redpanda dispone di un health check:

```yaml
healthcheck:
  test:
    - CMD-SHELL
    - rpk cluster health | grep -q 'Healthy:.*true'
  interval: 10s
  timeout: 5s
  retries: 10
```

Gli altri servizi dichiarano:

```yaml
depends_on:
  redpanda:
    condition: service_healthy
```

Questo impedisce l'avvio immediato dei servizi applicativi prima che il broker sia considerato disponibile.

Il health check non sostituisce completamente retry e gestione degli errori lato applicazione, ma migliora l'avvio coordinato dell'ambiente locale.

---

## 25. Data plane condiviso

Tutti i componenti utilizzano lo stesso data plane, ma leggono solo i flussi necessari.

```text
Machine Simulator
Scrive telemetria.

Maintenance Agent
Legge telemetria e risultati.
Scrive decisioni, comandi e feedback.

Machine Controller
Legge comandi.
Scrive risultati.
```

Questa organizzazione evita un topic diverso per ogni collegamento individuale tra processi e mantiene il sistema estendibile.

Un nuovo servizio di analytics potrebbe, per esempio, consumare `factory.agent-decisions` senza modificare il Maintenance Agent.

---

## 26. Scalabilità orizzontale

Le tre partizioni preparano il progetto alla scalabilità.

È possibile avviare più istanze dello stesso servizio con lo stesso `group.id`.

Il consumer group assegnerà le partizioni ai membri disponibili.

Esempio concettuale:

```text
3 partizioni
2 istanze del Maintenance Agent

Istanza A → una o due partizioni
Istanza B → le partizioni rimanenti
```

All'interno dello stesso consumer group, una partizione viene elaborata da un solo membro alla volta.

Se esistono più consumer che partizioni, alcuni consumer possono rimanere senza assegnazione.

---

## 27. Estensione multi-macchina

Il progetto attuale utilizza principalmente:

```text
machine-01
```

L'uso di `machine_id` come chiave prepara però il sistema a più macchine.

Un'estensione futura potrebbe usare:

```text
machine-01
machine-02
machine-03
```

Il Maintenance Agent manterrebbe stati distinti:

```python
machine_states = {
    "machine-01": MachineState(...),
    "machine-02": MachineState(...),
    "machine-03": MachineState(...),
}
```

Il data plane resterebbe condiviso. Non sarebbe necessario creare un topic separato per ogni macchina.

Questa caratteristica mostra il valore del partizionamento per chiave e dell'isolamento dello stato applicativo.

---

## 28. Osservabilità

L'osservabilità del progetto deriva da più fonti:

### Log dei container

Permettono di osservare:

- avvio dei servizi;
- rischio calcolato;
- azione selezionata;
- risultato del comando;
- feedback ricevuto;
- errori applicativi.

### Redpanda Console

Permette di osservare:

- messaggi persistenti;
- chiavi;
- partizioni;
- offset;
- consumer group;
- lag;
- contenuto JSON.

### Correlation ID

Permette di collegare record presenti in topic diversi.

Log, Console e correlazione svolgono ruoli complementari.

---

## 29. Resilienza e recupero

Il data plane migliora la resilienza perché conserva eventi e offset.

Uno scenario di test può essere:

```text
1. arrestare il Maintenance Agent;
2. pubblicare nuove telemetrie;
3. verificare che Redpanda conservi i record;
4. riavviare il Maintenance Agent;
5. verificare la ripresa dell'elaborazione;
6. controllare che il lag torni a zero.
```

Il ripristino dipende dalla corretta gestione degli offset e dalla retention dei topic.

---

## 30. Limiti del prototipo

Il progetto presenta alcuni limiti intenzionali.

### Un solo broker

La replica è `1`, quindi l'ambiente locale non dimostra la tolleranza al guasto di un cluster multi-broker.

### Stato applicativo in memoria

Lo stato del Maintenance Agent e del Machine Controller viene mantenuto in memoria. La ricreazione del container può azzerarlo.

### Nessuna transazione end-to-end

Il consumo di un evento, la produzione dell'evento derivato e il commit dell'offset non sono racchiusi in un'unica transazione distribuita.

### Nessun dead-letter topic

Gli eventi non validi vengono registrati nei log, ma non sono ancora inoltrati a un topic dedicato.

### Nessuno schema formale

I messaggi JSON vengono validati dall'applicazione, ma non è ancora presente uno Schema Registry.

### Sicurezza locale semplificata

L'ambiente di sviluppo non configura autenticazione, autorizzazione o cifratura TLS.

Questi limiti non impediscono la dimostrazione del data plane, ma indicano possibili sviluppi futuri.

---

## 31. Miglioramenti futuri

Possibili estensioni includono:

- cluster Redpanda con più broker;
- replica maggiore di uno;
- più macchine simulate;
- più istanze del Maintenance Agent;
- retry dei comandi falliti;
- dead-letter topic;
- Schema Registry;
- autenticazione SASL;
- TLS;
- ricostruzione dello stato dagli eventi;
- metriche Prometheus;
- dashboard per rischio, lag e risultati;
- conservazione a lungo termine degli eventi;
- policy decisionali configurabili.

---

## 32. Evidenze sperimentali del progetto

Durante la verifica sono stati rilevati cinque topic applicativi:

```text
factory.agent-decisions
factory.agent-feedback
factory.command-results
factory.commands
factory.telemetry
```

Ogni topic disponeva di:

```text
3 partizioni
1 replica
```

I consumer group erano:

```text
maintenance-agent-group
machine-controller-group
```

Entrambi risultavano:

```text
STATE = Stable
TOTAL-LAG = 0
```

Il Maintenance Agent aveva elaborato:

```text
factory.telemetry
CURRENT-OFFSET = 5
LOG-END-OFFSET = 5
LAG = 0
```

Il Machine Controller aveva elaborato:

```text
factory.commands
CURRENT-OFFSET = 3
LOG-END-OFFSET = 3
LAG = 0
```

Questi dati costituiscono una prova osservabile del corretto funzionamento del data plane.

---

## 33. Conclusione

Nel progetto Redpanda Agentic Smart Factory, il data plane è l'infrastruttura che collega componenti indipendenti attraverso eventi persistenti.

Il data plane permette di trasportare:

```text
percezioni
→ decisioni
→ comandi
→ risultati
→ feedback
```

Redpanda offre il broker, i topic, le partizioni, gli offset e i consumer group necessari al funzionamento del flusso.

Il `machine_id` preserva la coerenza degli eventi per macchina, mentre il `correlation_id` collega una singola telemetria a tutti gli eventi derivati.

Il sistema dimostra quindi le proprietà principali di un agentic data plane:

- disaccoppiamento;
- comunicazione asincrona;
- persistenza;
- tracciabilità;
- ordinamento per chiave;
- elaborazione tramite consumer group;
- osservabilità del lag;
- supporto al ciclo percezione, decisione, azione e feedback;
- estendibilità verso più macchine e più agenti.

Il data plane non prende decisioni al posto dell'agente. Fornisce però il supporto affidabile attraverso cui l'agente può percepire, agire e osservare le conseguenze delle proprie azioni.

---

## Riferimenti

- Redpanda Documentation, Kafka client compatibility: <https://docs.redpanda.com/streaming/current/develop/kafka-clients/>
- Redpanda Documentation, consumer groups and rpk group: <https://docs.redpanda.com/streaming/current/reference/rpk/rpk-group/rpk-group/>
- Redpanda Documentation, consumer offsets: <https://docs.redpanda.com/streaming/current/develop/consume-data/consumer-offsets/>
- Apache Kafka Documentation, distribution and consumer offset tracking: <https://kafka.apache.org/43/implementation/distribution/>
- Confluent Documentation, consumer design, groups and offsets: <https://docs.confluent.io/kafka/design/consumer-design.html>
