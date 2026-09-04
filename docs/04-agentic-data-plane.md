# Agentic Data Plane nella Smart Factory

## Che cos'è un data plane

Un **data plane** è l'insieme dei componenti e dei meccanismi che permettono ai dati operativi di attraversare un sistema durante la sua esecuzione.

In un sistema distribuito, il data plane gestisce il flusso concreto delle informazioni tra i diversi servizi. Non stabilisce necessariamente le regole del sistema, ma permette ai dati prodotti da un componente di raggiungere i componenti che devono elaborarli.

Il suo funzionamento generale può essere rappresentato così:

```mermaid
flowchart LR
    A["Componente che produce un dato"]
    B["Canale di comunicazione"]
    C["Trasporto e conservazione del dato"]
    D["Componente che legge il dato"]
    E["Elaborazione"]

    A --> B
    B --> C
    C --> D
    D --> E
```

Nel progetto, i dati operativi comprendono:

- telemetrie della macchina;
- decisioni del Maintenance Agent;
- comandi operativi;
- risultati del Machine Controller;
- feedback acquisiti dall'agente.

---

## Differenza tra control plane e data plane

Il **control plane** e il **data plane** svolgono funzioni differenti.

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

Un esempio di configurazione è:

```yaml
STATE_WINDOW_SIZE: "5"
CONTROLLER_MODE: MIXED
```

Questi valori definiscono come devono comportarsi i componenti.

Il data plane entra invece in funzione quando vengono prodotti e consumati eventi:

```text
telemetria
        ↓
decisione
        ↓
comando
        ↓
risultato
        ↓
feedback
```

La distinzione può essere riassunta così:

> Le configurazioni stabiliscono come il sistema deve funzionare. Il data plane trasporta ciò che accade mentre il sistema è in funzione.

---

## Che cos'è un agentic data plane

Un **agentic data plane** è un data plane progettato per sostenere il ciclo operativo di uno o più agenti software.

Un agente non ha bisogno soltanto di ricevere dati. Deve poter:

1. percepire lo stato dell'ambiente;
2. aggiornare la propria memoria;
3. valutare la situazione;
4. prendere una decisione;
5. richiedere un'azione;
6. osservare il risultato dell'azione;
7. aggiornare nuovamente il proprio stato attraverso il feedback.

L'agentic data plane deve quindi trasportare più categorie di eventi:

```text
Percezioni
Decisioni
Comandi
Risultati
Feedback
```

Nel progetto, queste categorie corrispondono ai cinque topic applicativi:

```text
factory.telemetry
factory.agent-decisions
factory.commands
factory.command-results
factory.agent-feedback
```

---

## Differenza tra data plane tradizionale e agentic data plane

Un data plane tradizionale può limitarsi a trasportare dati tra applicazioni.

Esempio:

```text
Sensore
        ↓
dato di temperatura
        ↓
sistema di archiviazione
```

In questo caso, il dato viene trasferito e conservato, ma non è necessariamente parte di un ciclo decisionale.

Un agentic data plane trasporta invece anche gli **eventi prodotti dal ragionamento e dalle azioni di un agente:

```text
Telemetria
        ↓
valutazione del rischio
        ↓
decisione
        ↓
comando
        ↓
risultato
        ↓
feedback
```

La differenza principale è quindi la seguente:

>**Data plane tradizionale**:Trasporta dati tra componenti.

> **Agentic data plane**:Trasporta dati, decisioni, azioni e risultati necessari a chiudere il ciclo operativo dell'agente.


Il broker non rende automaticamente un sistema agentico. Il carattere agentico deriva dalla presenza di:

- un agente con memoria;
- una funzione di valutazione;
- una politica decisionale;
- azioni operative;
- risultati osservabili;
- un ciclo di feedback.

---

## Il data plane realizzato nel progetto

Nel progetto, il data plane è formato da:

- Redpanda come broker centrale;
- i topic applicativi;
- le partizioni;
- le chiavi dei record;
- gli offset;
- i producer;
- i consumer;
- i consumer group;
- gli eventi JSON scambiati tra i servizi.

Redpanda è quindi il **broker di event streaming che costituisce il cuore infrastrutturale del data plane**, ma non coincide da solo con l'intero data plane.

Il data plane completo comprende anche i componenti che producono e consumano gli eventi.

```mermaid
flowchart TD
    A["Machine Simulator"]
    B["factory.telemetry"]
    C["Maintenance Agent"]
    D["factory.agent-decisions"]
    E["factory.commands"]
    F["Machine Controller"]
    G["factory.command-results"]
    H["Maintenance Agent"]
    I["factory.agent-feedback"]

    A --> B
    B --> C
    C --> D
    C --> E
    E --> F
    F --> G
    G --> H
    H --> I
```

---

## Percezione dell'ambiente

La prima funzione dell'agentic data plane è trasportare le informazioni che descrivono lo stato dell'ambiente.

Nel progetto, il `Machine Simulator` genera una telemetria simile a:

```json
{
  "event_id": "uuid-evento",
  "correlation_id": "abc-125",
  "machine_id": "machine-01",
  "temperature": 93.94,
  "vibration": 6.82,
  "speed": 1450,
  "energy_consumption": 128.0,
  "phase": "DEGRADING"
}
```

L'evento viene pubblicato su:

```text
factory.telemetry
```

Il `Maintenance Agent` è configurato per controllare e leggere i messaggi disponibili nel topic della telemetria (legge i messagi anche dal topic `factory.command_results` per produrre dei feedback).

```python
consumer.subscribe(
    [
        TELEMETRY_TOPIC,
        COMMAND_RESULTS_TOPIC,
    ]
)
```

Per l'agente, ogni evento di telemetria rappresenta una nuova osservazione della macchina, per questa presenta tanti eventi quanto sono quelli riportati nel topic che salva la telemetria.

```text
Machine Simulator
        ↓
genera una misurazione
        ↓
factory.telemetry
        ↓
Maintenance Agent
        ↓
aggiorna la memoria
```

---

## Decisione dell'agente

Dopo aver ricevuto la telemetria, il Maintenance Agent:

1. aggiorna la memoria della macchina;
2. calcola le medie recenti;
3. verifica il trend;
4. calcola il `risk_score`;
5. seleziona un'azione.

Ogni valutazione viene pubblicata su:

```text
factory.agent-decisions
```

Un evento può contenere:

```json
{
  "correlation_id": "abc-125",
  "risk_score": 0.56,
  "previous_action": "MONITOR",
  "selected_action": "REDUCE_SPEED",
  "reason": "The risk level requires a speed reduction"
}
```

La decisione è parte del data plane perché viene prodotta durante l'esecuzione e resa disponibile come evento persistente.

---

## Comandi operativi

Non tutte le decisioni richiedono l'intervento del Controller.

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

Le azioni operative vengono pubblicate su `factory.commands`, in modo tale che il `Machine Controller` può leggere gli eventi da questo topic e simula l'esecuzione delle azioni.

La separazione tra decisione e comando permette di distinguere:

```text
factory.agent-decisions
Che cosa ha deciso l'agente.

factory.commands
Che cosa deve essere realmente eseguito.
```

---

## Risultati delle azioni

Dopo aver elaborato un comando, il Machine Controller pubblica il risultato su:

```text
factory.command-results
```

Un risultato positivo può essere:

```json
{
  "correlation_id": "abc-125",
  "action": "REDUCE_SPEED",
  "result": "SUCCESS",
  "machine_status": "REDUCED_SPEED",
  "previous_speed": 1400,
  "current_speed": 900
}
```

Un risultato negativo può essere:

```json
{
  "correlation_id": "abc-127",
  "action": "EMERGENCY_STOP",
  "result": "FAILED",
  "failure_reason": "Simulated actuator communication failure",
  "machine_status": "RUNNING"
}
```

Il risultato permette di distinguere tra azione richiesta e azione realmente eseguita.

---

## Ciclo di feedback

Il ciclo agentico non termina quando il Maintenance Agent pubblica il comando, infatti l'agente deve sapere se l'azione richiesta ha avuto successo oppure è fallita.

Per questo il Maintenance Agent legge anche:

```text
factory.command-results
```

Quando riceve il risultato, aggiorna il proprio stato interno:

```python
state.update_command_result(command_result)
```

Successivamente pubblica un feedback su:

```text
factory.agent-feedback
```

Un feedback può contenere:

```json
{
  "correlation_id": "abc-127",
  "action": "EMERGENCY_STOP",
  "command_result": "FAILED",
  "machine_status": "RUNNING",
  "feedback_status": "PROCESSED"
}
```

È importante distinguere i due campi:

```text
command_result = FAILED
Il Controller non è riuscito a eseguire il comando.

feedback_status = PROCESSED
L'agente ha ricevuto e interpretato correttamente il risultato negativo.
```

Il feedback chiude il ciclo:

```text
Percezione
        ↓
Decisione
        ↓
Azione
        ↓
Risultato
        ↓
Feedback
```

---

## Comunicazione asincrona

I componenti non si chiamano direttamente.

Il Machine Simulator non invia una richiesta HTTP al Maintenance Agent. Il Maintenance Agent non chiama direttamente il Machine Controller.

Ogni componente pubblica o consuma eventi attraverso il data plane.

```text
Comunicazione diretta
Simulator → Agent → Controller
```

```text
Comunicazione asincrona
Simulator → topic → Agent → topic → Controller
```

Questo disaccoppiamento permette ai componenti di:

- funzionare con velocità differenti;
- essere riavviati separatamente;
- essere sostituiti senza cambiare gli altri servizi;
- rileggere eventi ancora disponibili;
- essere osservati tramite topic e log.

---

## Persistenza e recupero

Gli eventi non scompaiono subito dopo la lettura. Redpanda li conserva secondo la configurazione del broker e dei topic.

Questa proprietà permette a un consumer temporaneamente arrestato di **recuperare gli eventi dopo il riavvio**.

Esempio:

```text
Maintenance Agent arrestato
        ↓
il simulatore pubblica una telemetria
        ↓
Redpanda conserva l'evento
        ↓
il Maintenance Agent viene riavviato
        ↓
riprende dalla posizione registrata
        ↓
elabora la telemetria
```

Gli offset e i consumer group permettono di registrare l'avanzamento dei consumer.

I dettagli tecnici relativi a partizioni, offset, consumer group e volume persistente sono descritti nel documento `05-redpanda.md`.

---

## Tracciabilità end-to-end

Gli offset identificano la posizione dei record all'interno delle singole partizioni, ma non collegano automaticamente record presenti in topic differenti.

Il progetto usa quindi il campo:

```text
correlation_id
```

Lo stesso valore viene propagato lungo tutta la catena:

```text
factory.telemetry
        ↓
factory.agent-decisions
        ↓
factory.commands
        ↓
factory.command-results
        ↓
factory.agent-feedback
```

Per esempio, cercando:

```text
abc-125
```

è possibile ricostruire:

1. quale telemetria è stata ricevuta;
2. quale rischio è stato calcolato;
3. quale decisione è stata presa;
4. quale comando è stato inviato;
5. quale risultato è stato prodotto;
6. quale feedback è stato acquisito.

Questa caratteristica rende il data plane auditabile e osservabile.

---

## Relazione tra eventi e topic

Il numero di record non deve essere uguale in tutti i topic.

Un esempio è:

```text
5 telemetrie
        ↓
5 decisioni
        ↓
3 comandi
        ↓
3 risultati
        ↓
3 feedback
```

La differenza dipende dalla logica dell'agente:

```text
NO_ACTION e MONITOR
Non richiedono l'intervento del Controller.

REDUCE_SPEED, REQUEST_INSPECTION ed EMERGENCY_STOP
Producono un comando e, successivamente, un risultato e un feedback.
```

Il data plane non copia semplicemente ogni messaggio in tutti i topic. Trasporta eventi derivati sulla base delle decisioni applicative.

---

## Separazione delle responsabilità

Ogni componente mantiene una responsabilità precisa.

```text
Machine Simulator
Produce osservazioni dell'ambiente.

Maintenance Agent
Interpreta le osservazioni e prende decisioni.

Machine Controller
Esegue i comandi simulati.

Redpanda
Riceve, conserva e distribuisce gli eventi.

Agentic Data Plane
È l'insieme del flusso operativo che collega questi componenti.
```

Redpanda non calcola il rischio. Il Maintenance Agent non conserva direttamente i messaggi per gli altri servizi. Il Machine Controller non decide autonomamente quale azione sia necessaria.

Questa separazione rende l'architettura modulare e comprensibile.

---

## Adattamento del data plane al comportamento agentico

Nel progetto, un normale flusso di eventi è stato adattato alle necessità di un agente attraverso quattro scelte.

### 1. Memoria dell'agente

Il Maintenance Agent conserva una finestra delle misurazioni recenti invece di reagire soltanto all'ultimo evento.

### 2. Decisioni persistenti

Ogni valutazione viene pubblicata in `factory.agent-decisions`, comprese `NO_ACTION` e `MONITOR`.

### 3. Separazione tra decisione ed esecuzione

L'agente pubblica un comando, mentre il Machine Controller ne simula l'esecuzione.

### 4. Feedback osservabile

Il risultato ritorna all'agente e viene registrato in `factory.agent-feedback`.

Queste caratteristiche trasformano una semplice pipeline di telemetria in un agentic data plane.

---

## Riferimenti

- Redpanda Documentation, Introduction to Redpanda: <https://docs.redpanda.com/streaming/current/get-started/intro-to-events/>
- Redpanda Documentation, How Redpanda Works: <https://docs.redpanda.com/streaming/current/get-started/architecture/>
- Redpanda Documentation, Consumer Offsets: <https://docs.redpanda.com/streaming/current/develop/consume-data/consumer-offsets/>
