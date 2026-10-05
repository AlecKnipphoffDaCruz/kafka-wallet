# Projeto de prática: Carteira Digital com Kafka (Wallet + Ledger)

> Documento de instruções para guiar a construção do projeto. A ideia é seguir as fases em ordem, marcar os checklists e ir commitando a cada fase concluída.

---

## 1. Objetivo

Construir um sistema de transferências entre contas orientado a eventos, usando **Spring Boot + Kafka + PostgreSQL**, para praticar Kafka de forma realista e deixar um projeto forte no portfólio.

O foco não é a regra de negócio (que é simples de propósito), e sim os **problemas reais de mensageria**:

- Garantir que o evento só seja publicado se a transação no banco der certo (**Outbox Pattern**)
- Lidar com mensagens duplicadas (**consumidor idempotente**)
- Lidar com falhas (**retry com backoff e DLQ**)
- Entender partições, chaves, consumer groups e ordenação
- Observar o sistema funcionando (**consumer lag, métricas**)

## 2. O que você vai praticar

| Tema | Onde aparece |
|---|---|
| Producer/consumer, tópicos, partições, offsets | Fase 1 |
| Consumer groups e rebalanceamento | Fase 1 |
| Chave de partição e garantia de ordem | Fases 1 e 3 |
| Transação no banco + ledger (partida dobrada) | Fase 2 |
| Idempotência de requisição (`Idempotency-Key`) | Fase 2 |
| Outbox Pattern | Fase 3 |
| Idempotent consumer, retry, DLQ | Fase 4 |
| Kafka Streams (janela de tempo) | Fase 5 (opcional) |
| Métricas, lag, Prometheus/Grafana | Fase 6 |
| Testcontainers (Kafka + Postgres) | Fase 7 |
| CI e documentação | Fase 8 |

## 3. Arquitetura

```
 Cliente
    |
    v  POST /transfers  (Idempotency-Key)
+----------------+       mesma transação        +-----------+
|  wallet-api    | ---------------------------> | PostgreSQL|
| (Spring Boot)  |  transfers + ledger + outbox +-----------+
+----------------+                                    |
        |                                             |
        |  outbox relay (poller)  <-------------------+
        v
   [ Kafka ]
   topic: transfer.completed  (chave = accountId de origem)
        |
        +--------------------+---------------------+
        v                    v                     v
 notification-service   fraud-service        (futuro) statement-service
 (consumer idempotente) (regras simples)     (read model / extrato)
        |                    |
   retry + DLQ          publica fraud.alert
```

### Serviços

- **wallet-api**: expõe a API REST, mantém contas, saldos, transferências e o ledger. Grava o evento na tabela `outbox_events` na mesma transação da transferência. Um *relay* lê o outbox e publica no Kafka.
- **notification-service**: consome `transfer.completed` e simula o envio de uma notificação (log ou tabela). Aqui você pratica idempotência, retry e DLQ.
- **fraud-service**: consome `transfer.completed`, aplica regras simples (valor acima de um limite, muitas transferências em pouco tempo) e publica em `fraud.alert`.

> Dica: comece com `wallet-api` + `notification-service`. Só adicione o `fraud-service` quando as fases 1 a 4 estiverem prontas.

## 4. Stack

- Java 21 (ou 25), Spring Boot 3.x
- Spring Web, Spring Data JPA, Validation
- Spring for Apache Kafka
- PostgreSQL + Flyway (migrations)
- Docker e Docker Compose
- Actuator + Micrometer + Prometheus + Grafana
- JUnit 5, Testcontainers (Kafka e PostgreSQL), Awaitility
- GitHub Actions

> Confirme as versões atuais das dependências e imagens Docker antes de começar.

## 5. Estrutura sugerida (monorepo, multi-módulo)

```
kafka-wallet/
├── docker-compose.yml
├── README.md
├── docs/
│   └── architecture.png
├── wallet-api/
│   └── src/main/java/.../wallet
│       ├── domain/            # entidades e regras (Account, Transfer, LedgerEntry)
│       ├── application/
│       │   ├── port/in/       # casos de uso (TransferMoneyUseCase)
│       │   └── port/out/      # contratos (AccountRepository, OutboxPort)
│       └── adapter/
│           ├── in/rest/       # controllers + DTOs
│           └── out/
│               ├── persistence/
│               └── outbox/    # relay que publica no Kafka
├── notification-service/
├── fraud-service/
└── common-events/             # classes dos eventos compartilhados
```

Arquitetura hexagonal: o domínio não conhece Spring, Kafka nem JPA.

## 6. Modelo de dados (wallet-api)

```sql
accounts        (id UUID PK, owner_name, balance NUMERIC(19,4), version BIGINT)
transfers       (id UUID PK, idempotency_key UNIQUE, from_account, to_account,
                 amount NUMERIC(19,4), status, created_at)
ledger_entries  (id UUID PK, transfer_id, account_id, type /* DEBIT|CREDIT */,
                 amount NUMERIC(19,4), created_at)
outbox_events   (id UUID PK, aggregate_id, event_type, payload JSONB,
                 created_at, published_at NULL)
```

Regras:

- Use `BigDecimal` (nunca `double`) para dinheiro.
- A coluna `version` em `accounts` serve para **lock otimista** (`@Version`).
- Toda transferência gera **duas linhas** no ledger (débito e crédito) que somam zero.
- `idempotency_key` é `UNIQUE`: repetir a mesma requisição retorna a transferência original, sem duplicar.

## 7. Tópicos Kafka

| Tópico | Chave | Partições | Produtor | Consumidores |
|---|---|---|---|---|
| `transfer.completed` | accountId de origem | 3 | wallet-api (relay) | notification, fraud |
| `transfer.completed.dlq` | mesma | 1 | erro handler | (inspeção manual) |
| `fraud.alert` | accountId | 3 | fraud-service | (futuro) |

Exemplo de evento:

```json
{
  "eventId": "7f3c2a1e-...",
  "eventType": "TransferCompleted",
  "occurredAt": "2026-10-05T14:30:00Z",
  "payload": {
    "transferId": "b1c2...",
    "fromAccount": "a111...",
    "toAccount": "a222...",
    "amount": "150.00"
  }
}
```

> O `eventId` é essencial: é ele que o consumidor usa para detectar duplicatas.

---

## 8. Roadmap por fases

### Fase 0: Ambiente

- [ ] Criar o repositório e a estrutura multi-módulo
- [ ] `docker-compose.yml` com Kafka (modo KRaft), Kafka UI e PostgreSQL
- [ ] Subir tudo com `docker compose up -d` e acessar a Kafka UI

Ponto de partida do compose (ajuste os listeners se for rodar os serviços dentro do Docker também):

```yaml
services:
  kafka:
    image: apache/kafka:latest
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0

  kafka-ui:
    image: kafbat/kafka-ui:latest
    ports: ["8089:8080"]
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
    depends_on: [kafka]

  postgres:
    image: postgres:16
    ports: ["5432:5432"]
    environment:
      POSTGRES_USER: wallet
      POSTGRES_PASSWORD: wallet
      POSTGRES_DB: wallet
```

### Fase 1: Kafka na prática (sem regra de negócio)

Objetivo: *sentir* como o Kafka funciona antes de construir em cima dele.

- [ ] Criar o tópico `transfer.completed` com 3 partições
- [ ] Criar um producer simples que envia mensagens com chave
- [ ] Criar um consumer simples com `group-id`
- [ ] **Experimento 1:** subir 2 consumers no mesmo grupo e ver as partições sendo divididas
- [ ] **Experimento 2:** derrubar um consumer e observar o rebalanceamento
- [ ] **Experimento 3:** enviar várias mensagens com a mesma chave e confirmar que caem na mesma partição (ordem preservada)
- [ ] **Experimento 4:** subir consumers em grupos diferentes e ver que ambos recebem tudo
- [ ] **Experimento 5:** resetar offsets de um grupo pela CLI e reprocessar mensagens

Anote o que observou em `docs/experimentos.md`. Isso vira ótimo material de entrevista.

### Fase 2: Wallet API (sem Kafka ainda)

- [ ] Migrations Flyway das 4 tabelas
- [ ] `POST /accounts`, `GET /accounts/{id}`
- [ ] `POST /transfers` com header `Idempotency-Key`
- [ ] Débito + crédito + ledger na **mesma transação**
- [ ] Validar saldo insuficiente e contas iguais
- [ ] Lock otimista em `accounts` e tratamento da exceção de concorrência (retry ou 409)
- [ ] Teste: 50 transferências concorrentes na mesma conta e o saldo final tem que bater

### Fase 3: Outbox Pattern

Problema: se você grava no banco e depois publica no Kafka, uma das duas operações pode falhar e deixar o sistema inconsistente. A solução é gravar o evento no banco (tabela `outbox_events`) na mesma transação e publicar depois.

- [ ] Gravar `TransferCompleted` no outbox dentro da transação da transferência
- [ ] Criar o *relay*: um `@Scheduled` que lê eventos com `published_at IS NULL`, publica no Kafka e marca como publicado
- [ ] Usar `SELECT ... FOR UPDATE SKIP LOCKED` para permitir mais de uma instância do relay
- [ ] Configurar o producer: `acks=all` e `enable.idempotence=true`
- [ ] Usar o `accountId` de origem como chave da mensagem
- [ ] Teste: derrubar o Kafka, fazer transferências, subir o Kafka e confirmar que os eventos chegam

> Entenda: o relay entrega **pelo menos uma vez** (at-least-once). Pode haver duplicatas. É por isso que a Fase 4 existe.

### Fase 4: Consumidores confiáveis

- [ ] `notification-service` consumindo `transfer.completed`
- [ ] Desligar o auto-commit (`enable.auto.commit=false`) e confirmar o offset só após processar
- [ ] Tabela `processed_events (event_id, consumer, processed_at)` para **idempotência**
- [ ] Retry com backoff exponencial (`DefaultErrorHandler` + `ExponentialBackOff`)
- [ ] Mensagens que esgotam as tentativas vão para `transfer.completed.dlq` (`DeadLetterPublishingRecoverer`)
- [ ] Simular falha (exceção forçada) e ver o retry e a DLQ funcionando na Kafka UI
- [ ] Reenviar o mesmo evento duas vezes e confirmar que a notificação sai **uma vez só**

Depois de funcionar, estude também `@RetryableTopic` e compare as abordagens.

### Fase 5 (opcional): fraud-service com Kafka Streams

- [ ] Regra 1: transferência acima de um valor limite gera alerta
- [ ] Regra 2: mais de N transferências da mesma conta em 1 minuto (janela de tempo com Kafka Streams)
- [ ] Publicar em `fraud.alert`

### Fase 6: Observabilidade

- [ ] Actuator + Micrometer + Prometheus em todos os serviços
- [ ] Grafana com um painel simples (requisições, erros, tempo de resposta)
- [ ] Acompanhar **consumer lag** (pela Kafka UI ou métricas)
- [ ] Gerar carga e ver o lag subir e descer
- [ ] Logs com `eventId` / `transferId` (correlação)

### Fase 7: Testes

- [ ] Testcontainers com PostgreSQL e Kafka
- [ ] Teste de integração do fluxo completo: `POST /transfers` → evento no Kafka → notificação registrada (usar Awaitility)
- [ ] Teste de idempotência da API
- [ ] Teste de idempotência do consumidor
- [ ] Teste de concorrência (Fase 2)

### Fase 8: Acabamento

- [ ] GitHub Actions rodando build e testes
- [ ] `README.md` em inglês (ver seção 10)
- [ ] Diagrama de arquitetura em `docs/`
- [ ] Coleção de requests (Bruno, Postman ou arquivo `.http`)
- [ ] Tag `v1.0.0`

---

## 9. Conceitos que você deve saber explicar no final

Use como checklist de estudo e de entrevista:

1. O que é uma partição e por que a ordem só é garantida **dentro** dela?
2. Para que serve a chave da mensagem?
3. Qual a diferença entre consumer group e consumer?
4. O que acontece quando um consumer entra ou sai do grupo?
5. O que é offset e quando fazer o commit?
6. At-most-once, at-least-once e exactly-once: o que cada um significa e qual você usou?
7. Por que o Outbox Pattern existe? O que acontece sem ele?
8. Como garantir idempotência no consumidor?
9. Para que serve a DLQ e o que fazer com o que cai nela?
10. O que é consumer lag e o que causa lag alto?
11. Por que `acks=all` e `enable.idempotence=true` no producer?
12. Quantas partições escolher e o que acontece se aumentar depois?

## 10. Checklist do README (em inglês)

- [ ] Descrição em 2 ou 3 linhas do que o projeto resolve
- [ ] Diagrama de arquitetura
- [ ] Como rodar (`docker compose up` + comandos)
- [ ] Exemplos de requests e respostas
- [ ] Seção **Design decisions**: por que Outbox, por que essa chave de partição, por que lock otimista
- [ ] Seção **Trade-offs and limitations**: o que ficaria diferente em produção
- [ ] Como rodar os testes

## 11. Evoluções futuras

- `statement-service` com um read model (extrato) alimentado pelos eventos (CQRS)
- Schema Registry com Avro ou Protobuf
- Autenticação com Keycloak (JWT) protegendo a API
- Deploy na AWS (ECS ou EC2 + RDS) com Terraform
- Comparar o relay por polling com CDC (Debezium)

## 12. Cuidados

- Não suba senhas, tokens ou IDs de conta AWS no repositório
- Dinheiro sempre em `BigDecimal` / `NUMERIC`
- Commits pequenos e descritivos, um por etapa do checklist
- Prefira um projeto pequeno e **funcionando** a um grande pela metade
