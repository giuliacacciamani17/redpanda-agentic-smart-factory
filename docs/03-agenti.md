# Agenti software




> **Idea chiave:** un agente non si limita a trasferire dati. Osserva un ambiente, mantiene uno stato, applica una politica decisionale, produce azioni e usa il risultato delle azioni come feedback.

---

## 1. Che cos'è un agente

Un **agente software** è un sistema che riceve informazioni da un ambiente, le interpreta rispetto a un obiettivo e sceglie un'azione. Un agente può funzionare con regole deterministiche, modelli statistici, tecniche di machine learning oppure modelli linguistici. Un LLM non è quindi un requisito obbligatorio.

Le architetture agentiche possono includere percezione, elaborazione, decisione, azione, memoria e feedback. La memoria permette di conservare il contesto e di non trattare ogni input come un evento completamente isolato, risultano quindi elementi centrali dei sistemi agentici.

### 1.1 Modello generale

```mermaid
flowchart LR
    E[Ambiente] -->|percezioni| P[Percezione]
    P --> M[Memoria e stato]
    M --> R[Valutazione]
    R --> D[Decisione]
    D -->|azione| E
    E -->|risultato| F[Feedback]
    F --> M
```

<!--
GitHub visualizza i diagrammi Mermaid direttamente nei file Markdown, quindi il diagramma rimane modificabile insieme al codice sorgente. [GitHub Docs, Creating diagrams](https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/creating-diagrams)
-->
---

## 2. Componenti fondamentali di un agente

| Componente | Domanda | Implementazione nel progetto |
|---|---|---|
| Percezione | Che cosa sta accadendo? | Consumo di `factory.telemetry` |
| Memoria | Che cosa è accaduto di recente? | `MachineState` con finestre di temperatura e vibrazione |
| Valutazione | Quanto è rischiosa la situazione? | `risk_engine.py` |
| Decisione | Quale comportamento è opportuno? | `policy.py` |
| Azione | Quale comando deve essere eseguito? | Pubblicazione su `factory.commands` |
| Feedback | L'azione è stata eseguita? | Consumo di `factory.command-results` |
| Audit | Come ricostruisco il processo? | `factory.agent-decisions`, `factory.agent-feedback` e `correlation_id` |

### 2.1 Percezione

E` il momento in cui l’agente acquisisce informazioni sull’ambiente prima di aggiornare la memoria, calcolare il rischio e prendere una decisione. 

Nel progetto, il `Machine Simulator` pubblica misurazioni strutturate come:

- temperatura;
- vibrazione;
- velocità;
- consumo energetico;
- fase operativa.

Questi dati vengono inseriti in un messaggio JSON e pubblicati sul topic `factory.telemetry`.

Nel progetto un evento di telemetria ha questa struttura:

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

Il `Maintenance Agent` si iscrive al topic di telemetria e riceve ogni nuovo messaggio.

Per l’agente, quindi, ogni messaggio di telemetria rappresenta una nuova osservazione dello stato della macchina:

```python
consumer.subscribe(
    [
        TELEMETRY_TOPIC,
    ]
)
```

L'agente non legge soltanto sensori fisici. In un sistema event-driven, un topic può costituire l'interfaccia percettiva dell'agente.

Il flusso è:

```mermaid
flowchart TD
    A["Machine Simulator"]
    B["factory.telemetry"]
    C["Redpanda conserva e rende<br/>disponibile l'evento"]
    D["Maintenance Agent"]
    E["Aggiorna la memoria e<br/>interpreta la misurazione"]

    A -->|"Genera una misurazione"| B
    B --> C
    C -->|"Distribuisce l'evento"| D
    D --> E
```

### 2.2 Memoria e stato interno

Viene conservata una finestra delle ultime misurazioni:

```python
@dataclass
class MachineState:
    machine_id: str
    window_size: int
    temperatures: deque[float] = field(init=False)
    vibrations: deque[float] = field(init=False)
    last_action: str = "NO_ACTION"
```

Le code hanno dimensione massima configurabile:

```python
self.temperatures = deque(maxlen=self.window_size)
self.vibrations = deque(maxlen=self.window_size)
```

Questa è una forma di **memoria a breve termine**. Consente di calcolare medie e trend recenti senza conservare indefinitamente tutti gli eventi. La memoria agentica serve proprio a **mantenere contesto**, ricordare **azioni precedenti** e utilizzare **risultati passati** nelle valutazioni successive.

### 2.3 Valutazione del rischio

Il file `risk_engine.py` trasforma lo stato della macchina in un punteggio compreso tra `0.0` e `1.0`:

```python
total_risk = (
    temperature_risk * TEMPERATURE_WEIGHT
    + vibration_risk * VIBRATION_WEIGHT
    + trend_risk * TREND_WEIGHT
)

return round(min(total_risk, 1.0), 2)
```

Nel prototipo i pesi sono:

```text
TEMPERATURE_WEIGHT = 0.45
VIBRATION_WEIGHT = 0.45
TREND_WEIGHT = 0.10
```

Questo significa che il 45% del rischio deriva dalla temperatura, 
45% del rischio deriva dalla vibrazione e infine il 10% del rischio deriva dal trend. Il trend indica semplicemente l’andamento recente dei valori nel tempo, verificando se i valori stanno aumentando progressimente nelle ultime misurazioni.

Mentre per quanto riguarda le componenti `temperature_risk`, `vibration_risk` e `trend_risk` vengono calcolate come segue:


```python
    temperature_risk = normalize(
        state.average_temperature(),
        NORMAL_TEMPERATURE,
        CRITICAL_TEMPERATURE,
    )
```

Il sistema con la funziona `normalize` recupera la temperatura media e la confronta con due soglie `NORMAL_TEMPERATURE = 65.0` e `CRITICAL_TEMPERATURE = 90.0` ed il comportamento è:
```text
Temperatura media minore o uguale a 65 °C
→ temperature_risk = 0.0

Temperatura media maggiore o uguale a 90 °C
→ temperature_risk = 1.0

Temperatura media compresa tra 65 °C e 90 °C
→ temperature_risk è un valore proporzionale tra 0.0 e 1.0
```

La seconda componente viene calcolata con:
```python   
    vibration_risk = normalize(
        state.average_vibration(),
        NORMAL_VIBRATION,
        CRITICAL_VIBRATION,
    )
```
Il sistema recupera la vibrazione media e la confronta con due soglie `NORMAL_VIBRATION= 2.0` e `CRITICAL_VIBRATION = 7.0` ed il comportamento è:
```text
Vibrazione media minore o uguale a 2.0
→ vibration_risk = 0.0

Vibrazione media maggiore o uguale a 7.0
→ vibration_risk = 1.0

Vibrazione media compresa tra 2.0 e 7.0
→ vibration_risk è un valore proporzionale tra 0.0 e 1.0
```

Le funzioni non usano direttamente soltanto l'ultima misurazione ricevuta, ma utilizzano le medie calcolate sulla finestra di memoria:

```python
state.average_temperature()
state.average_vibration()
```

Infine la terza componente viene calcolate con:
```python
    trend_risk = calculate_trend_risk(state)
```
Non considera soltanto quanto siano elevati i valori, ma controlla se temperatura e vibrazione stanno aumentando progressivamente nel tempo, considerando le ultime misurazioni. Se non sono ancora presenti tali valori, il sistema non dispone di informazioni sufficienti per stabilire un andamento.




Il punteggio di rischio finale non rappresenta una probabilità scientificamente calibrata di guasto, ma è un indice deterministico, progettato per rendere osservabile il processo decisionale.

### 2.4 Politica decisionale

Il file `policy.py` converte il rischio in un'azione:

```python
def select_action(risk_score: float) -> str:
    if risk_score >= 0.85:
        return EMERGENCY_STOP

    if risk_score >= 0.65:
        return REQUEST_INSPECTION

    if risk_score >= 0.45:
        return REDUCE_SPEED

    if risk_score >= 0.20:
        return MONITOR

    return NO_ACTION
```

| Intervallo del rischio | Decisione | Effetto operativo |
|---|---|---|
| `< 0.20` | `NO_ACTION` | Nessun comando |
| `0.20 - 0.44` | `MONITOR` | Osservazione più attenta |
| `0.45 - 0.64` | `REDUCE_SPEED` | Comando al controller |
| `0.65 - 0.84` | `REQUEST_INSPECTION` | Richiesta di manutenzione |
| `>= 0.85` | `EMERGENCY_STOP` | Arresto di emergenza |

Separare il calcolo del rischio dalla politica rende il sistema più leggibile e testabile. Le soglie possono cambiare senza riscrivere l'acquisizione degli eventi.

---



## 3. Decisione, comando, risultato e feedback

Questi concetti sono distinti.

| Elemento | Significato | Topic |
|---|---|---|
| Decisione | Valutazione dell'agente | `factory.agent-decisions` |
| Comando | Azione operativa richiesta | `factory.commands` |
| Risultato | Esito tecnico prodotto dal controller | `factory.command-results` |
| Feedback | Conferma che l'agente ha acquisito l'esito | `factory.agent-feedback` |

```mermaid
sequenceDiagram
    participant S as Machine Simulator
    participant T as factory.telemetry
    participant A as Maintenance Agent
    participant D as factory.agent-decisions
    participant C as factory.commands
    participant MC as Machine Controller
    participant R as factory.command-results
    participant F as factory.agent-feedback

    S->>T: Telemetria
    T->>A: Percezione
    A->>D: Decisione e motivazione
    alt Azione operativa
        A->>C: Comando
        C->>MC: Richiesta di esecuzione
        MC->>R: SUCCESS oppure FAILED
        R->>A: Risultato
        A->>F: Feedback processato
    else NO_ACTION o MONITOR
        Note over A,C: Nessun comando operativo
    end
```

### 3.1 Feedback positivo

```json
{
  "correlation_id": "abc-131",
  "action": "REQUEST_INSPECTION",
  "command_result": "SUCCESS",
  "machine_status": "INSPECTION_REQUIRED",
  "feedback_status": "PROCESSED"
}
```

### 3.2 Feedback negativo

```json
{
  "correlation_id": "abc-130",
  "action": "REDUCE_SPEED",
  "command_result": "FAILED",
  "machine_status": "RUNNING",
  "feedback_status": "PROCESSED"
}
```

`FAILED` e `PROCESSED` non sono in contraddizione:

- `command_result = FAILED` indica che il controller non ha applicato l'azione;
- `feedback_status = PROCESSED` indica che l'agente ha ricevuto e registrato correttamente il fallimento.

Senza feedback, l'agente conoscerebbe soltanto l'intenzione di agire. Con il feedback, l'agente può distinguere tra comando inviato e cambiamento realmente applicato.

---

## Perché il Maintenance Agent è un agente

Il `Maintenance Agent` non è un semplice inoltro di messaggi perché:

1. percepisce telemetria da un ambiente esterno;
2. conserva misurazioni precedenti;
3. calcola uno stato sintetico di rischio;
4. sceglie tra più azioni possibili;
5. spiega la decisione;
6. evita comandi operativi duplicati;
7. riceve l'esito delle azioni;
8. aggiorna lo stato interno dopo il feedback.

```mermaid
stateDiagram-v2
    [*] --> NO_ACTION
    NO_ACTION --> MONITOR: rischio >= 0.20
    MONITOR --> REDUCE_SPEED: rischio >= 0.45
    REDUCE_SPEED --> REQUEST_INSPECTION: rischio >= 0.65
    REQUEST_INSPECTION --> EMERGENCY_STOP: rischio >= 0.85
    EMERGENCY_STOP --> REQUEST_INSPECTION: rischio diminuisce
    REQUEST_INSPECTION --> REDUCE_SPEED: recupero
    REDUCE_SPEED --> MONITOR: recupero
    MONITOR --> NO_ACTION: condizioni normali
```

### Deduplicazione dei comandi

L'agente registra ogni decisione nell'audit, ma non invia ripetutamente lo stesso comando:

```python
def should_publish_command(
    action: str,
    previous_action: str,
) -> bool:
    requires_intervention = action not in {
        NO_ACTION,
        MONITOR,
    }

    action_has_changed = action != previous_action

    return requires_intervention and action_has_changed
```

La distinzione è nei topic:

- `factory.agent-decisions` conserva ogni valutazione, ho una valutazione per ogni messaggio generato nel topic `factory.telemetry`;
- `factory.commands` contiene soltanto nuove azioni operative che il controller dovrà leggere per effettuare modifiche nella macchina.

In ogni messaggio del topic `factory.agent-decisions` sono presenti i campi `previous_action` che rappresenta l'azione memorizzata prima dell'evento corrente e `selected_action` che è la nuova decisione prodotta dopo l'aggiornamento dello stato.

```json
{
  "previous_action": "MONITOR",
  "selected_action": "REDUCE_SPEED",
  "risk_score": 0.56
}
```

Il campo da osservare per sapere che cosa ha deciso l'agente **adesso** è `selected_action`. Il confronto tra i due campi descrive la transizione decisionale.

## Tracciabilità end-to-end

Il progetto utilizza più identificativi con responsabilità diverse.

| Campo | Responsabilità |
|---|---|
| `event_id` | Identifica la telemetria |
| `decision_id` | Identifica la decisione |
| `command_id` | Identifica il comando |
| `result_id` | Identifica il risultato |
| `feedback_id` | Identifica il feedback |
| `correlation_id` | Collega l'intera catena |
| `sequence_number` | Ordina gli eventi nella singola simulazione |


Si può fare una ricerca sulla base del `correlation_id` in modo tale da riuscire a capire il ciclo completo che un determinato evento fra tra i vari topic.

```text
factory.telemetry
→ misurazioni che hanno originato il caso

factory.agent-decisions
→ rischio e azione selezionata

factory.commands
→ comando inviato

factory.command-results
→ esito tecnico

factory.agent-feedback
→ esito acquisito dall'agente
```

---
<!--
## Le componenti del progetto

### Machine Simulator

- rappresenta il macchinario;
- produce temperatura, vibrazione, velocità e consumo;
- non calcola il rischio;
- non decide azioni.

### Maintenance Agent

- percepisce la telemetria;
- conserva memoria recente;
- calcola il rischio;
- sceglie un'azione;
- pubblica comandi;
- acquisisce il feedback.

### Machine Controller

- non valuta la telemetria;
- non decide quale azione sia migliore;
- riceve un comando già scelto;
- simula l'esecuzione;
- pubblica `SUCCESS` oppure `FAILED`.

### Redpanda

- disaccoppia producer e consumer;
- conserva gli eventi;
- consente ai componenti di funzionare in modo asincrono;
- mantiene offset e consumer group;
- rende osservabile e ricostruibile il flusso.
!-->
---


Il `Maintenance Agent` è principalmente **reattivo**:

```text
riceve una telemetria
→ aggiorna lo stato
→ calcola il rischio
→ reagisce
```

Possiede però una componente più evoluta rispetto a una semplice regola istantanea, perché utilizza:

- una finestra temporale;
- medie recenti;
- trend crescenti;
- ultima azione;
- feedback del controller.

---

## Riferimenti

- IBM, [What are the components of AI agents?](https://www.ibm.com/think/topics/components-of-ai-agents)
- IBM, [What is AI agent memory?](https://www.ibm.com/think/topics/ai-agent-memory)
- Microsoft, [Memory for AI Agents](https://microsoft.github.io/ai-agents-for-beginners/13-agent-memory/)
- Microsoft Learn, [AI Agent Orchestration Patterns](https://learn.microsoft.com/en-us/azure/architecture/ai-ml/guide/ai-agent-design-patterns)
- GitHub Docs, [Creating diagrams](https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/creating-diagrams)
