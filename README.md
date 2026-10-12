# API de Ingestão — Monitoramento de Frota

Parte implementada: receber telemetria via HTTP, validar e publicar em Redpanda/Kafka.
Compatível com o plano de API de Ingestão fornecido: .NET 8 Minimal API,
FluentValidation, Confluent.Kafka, Serilog JSON, correlation ID, health e Docker.
Inclui laboratório isolado para desenvolver esta parte antes da integração do grupo.

## 1. Pré-requisitos

- Docker Desktop com containers Linux e Docker Compose v2.
- No Windows, habilitar WSL 2 conforme a instalação do Docker Desktop.
- VS Code ou Visual Studio para editar.
- SDK .NET 8 somente se quiser executar a API fora do Docker.
- Python 3 somente para o script opcional de testes.

## 2. Executar com um comando

Extraia o ZIP, abra um terminal na pasta que contém compose.yaml e execute:

```bash
docker compose up --build -d
```

Na primeira execução haverá download de imagens e pacotes NuGet.
O Compose sobe Redpanda, cria dois tópicos com seis partições e sobe a API.

```bash
docker compose ps
docker compose logs -f api-ingestao
```

Abra http://localhost:8081/health (esperado: Healthy).
Abra http://localhost:8081/health/ready (esperado: Healthy quando broker e tópicos existem).
Health não é uma interface de cadastro; o recebimento é pelo POST abaixo.
Se a inicialização falhar, examine `docker compose logs redpanda init-topics`.

## 3. Enviar pelo Postman/Insomnia

- Método: POST.
- URL: http://localhost:8081/api/telemetry.
- Body: raw, JSON.
- Content-Type: application/json.
- X-Correlation-ID opcional: um UUID; valores não UUID são substituídos.

```json
{
  "id_evento": "c12b3efe-f5ef-4857-bad4-a2a791494e01",
  "id_veiculo": "CAM-001",
  "sequencia": 123,
  "timestamp": "2026-10-06T19:30:00Z",
  "latitude": -19.9191,
  "longitude": -43.9386,
  "velocidade": 72.5,
  "motor_ligado": true
}
```

202 significa que o broker confirmou a publicação; não que o Worker já processou.
O retorno informa id_evento, correlation_id, particao e offset.
Também existe requests.http para a extensão REST Client do VS Code.
Cada leitura nova deve ter UUID novo. Um reenvio da mesma leitura preserva UUID e sequência.
Velocidade em km/h; timestamp ISO 8601 com Z ou offset explícito.
Sequência não negativa e crescente por veículo; persistência da sequência/reinício do
rastreador deve ser combinada com os colegas. A API não impõe ordem crescente.

## 4. Conferir a mensagem publicada

```bash
docker compose exec redpanda rpk topic consume frota.telemetria -o start -n 1
```

O comando lê uma mensagem do histórico. Para acompanhar mensagens novas:

```bash
docker compose exec redpanda rpk topic consume frota.telemetria -o end
```

O consumidor do rpk serve para inspeção, não substitui o Worker do grupo.

## 5. Testar DLQ

Envie o exemplo com latitude 999. Retorno: 400 e motivos da rejeição.
Envie também JSON quebrado: `{broken`. Deve retornar 400.

```bash
docker compose exec redpanda rpk topic consume frota.telemetria.dlq -o start -n 1
```

A DLQ recebe payload_original, motivos, recebido_em e correlation_id.
O corpo é lido antes da desserialização para capturar JSON corrompido.
Nenhuma mensagem inválida é intencionalmente encaminhada ao tópico de telemetria.
O limite HTTP é 64 KiB: corpos maiores retornam 413, sem DLQ, para proteger memória.
Erros de transporte/cliente desconectado não são pacotes de negócio para DLQ.

## 6. Testar indisponibilidade

```bash
docker compose stop redpanda
```

Envie um pacote: espere até cerca de dez segundos para o timeout do produtor.
Resultado esperado: 503, nunca confirmação 202 sem confirmação do broker.
/health continua Healthy e /health/ready fica Unhealthy (HTTP 503).

```bash
docker compose start redpanda
```

O simulador precisa fazer retry com backoff e preservar id_evento. Uma resposta 503
ou conexão interrompida pode ter resultado ambíguo: a mensagem pode ter sido entregue,
mas a confirmação não chegou. O Worker deve deduplicar por id_evento.
A API não guarda fila durável local; requisições ainda não confirmadas precisam ser
retidas/reenviadas pelo simulador para sustentar o requisito de não perder leituras.

## 7. Testes automatizados de integração

Com todos os serviços saudáveis, execute na raiz do projeto:

```bash
python tests/integration.py
```

Sete casos: publicação confirmada e leitura do evento no broker, latitude inválida,
JSON corrompido, campo ausente, zero/false válidos, evento atrasado e timestamp sem fuso.
Os testes criam mensagens reais. O primeiro usa partição/offset retornados para
confirmar que o evento correspondente está no Redpanda.
Para comprovar as DLQs, use também a inspeção manual descrita acima.

## 8. Executar a API fora do Docker

Suba somente o broker e os tópicos:

```bash
docker compose up -d redpanda init-topics
dotnet restore
dotnet run --urls http://localhost:8081
```

Não execute simultaneamente a API do Compose na mesma porta.
appsettings.json usa localhost:19092; no Docker, a variável
Kafka__BootstrapServers sobrescreve esse valor para redpanda:9092.

## 9. Integração com os colegas

Compartilhe primeiro contrato JSON e nomes dos tópicos.
Simulador: POST /api/telemetry, retry de 503/timeouts preservando id_evento.
Worker: consumir frota.telemetria; chave=id_veiculo; header X-Correlation-ID;
tratar reenvios e leituras atrasadas; commit após persistência durável.
Infra: reutilizar o Dockerfile; porta interna 8080; ajustar Kafka__BootstrapServers;
criar tópicos antes da API; configurar health/ready no balanceador.
Este Compose isolado não deve criar outro broker no ambiente completo do grupo.
Réplicas da API podem usar o mesmo tópico; portas externas devem ser gerenciadas
pelo balanceador do grupo. Não aumentar partições durante a demonstração de ordem,
pois isso pode mudar a partição associada a uma chave.

## 10. Garantias e limites

- Producer singleton, EnableIdempotence=true, Acks.All; aguardamos ProduceAsync.
- Idempotência Kafka protege retries do produtor, não requests HTTP repetidas
  nem publicações feitas por produtores diferentes.
- Mesma chave mantém afinidade de partição; Kafka não ordena timestamps atrasados.
  Requisições concorrentes/réplicas também podem chegar em ordem diferente da geração.
- Eventos atrasados são aceitos. O Worker precisa definir janela/reordenação e
  evitar sobrescrever a posição atual com uma leitura antiga.
- Um broker e fator de replicação 1 são adequados ao laboratório, não oferecem
  disponibilidade sob falha do broker ou perda de disco. Acks.All não cria réplicas.
- Volume preserva dados em reinícios normais; `docker compose down -v` apaga dados.
- Retenção Kafka é finita; o grupo deve definir retenção/capacidade compatíveis com
  tempo máximo de indisponibilidade e consumo do histórico.
- Sem autenticação/TLS neste laboratório; portas publicadas apenas em localhost.
- Log por evento pode aumentar I/O em teste de carga; ajustar nível/amostragem ao medir.
- Não implementa Workers, alertas, Redis, PostgreSQL, painel, k6 ou escalonamento.

## 11. Explicação para apresentação

“A API recebe um pacote do simulador, valida o contrato e publica no Redpanda.
Dados válidos vão para telemetria, inválidos para DLQ. O identificador do veículo
é a chave da mensagem. A resposta 202 só ocorre após confirmação do broker.
A API não acessa os bancos nem calcula infrações: isso fica com o Consumer Group.
Usamos correlation ID para rastrear o pacote, logs estruturados e health checks.”

## 12. Estado de verificação desta entrega

Os arquivos foram revisados e os formatos JSON, XML, YAML e sintaxe Python foram
verificados no ambiente de criação. Esse ambiente não possui SDK .NET nem Docker:
a compilação C#, restauração NuGet e os testes de integração ainda precisam ser
executados pelo comando acima. Não foi comprovada vazão, p95 ou tolerância a falhas
por execução neste ambiente. As versões de dependências/imagens estão explícitas
para permitir reprodução; atualizações devem ser validadas pelo grupo.
