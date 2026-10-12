using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Frota.Api.Ingestao.Services;

public sealed class KafkaPublisher : IDisposable
{
    private readonly IProducer<string, string> _producer;
    public string Topic { get; }
    public string DlqTopic { get; }

    public KafkaPublisher(IConfiguration configuration)
    {
        Topic = configuration["Kafka:Topic"] ?? "frota.telemetria";
        DlqTopic = configuration["Kafka:DlqTopic"] ?? "frota.telemetria.dlq";
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:19092",
            EnableIdempotence = true,
            Acks = Acks.All,
            MessageTimeoutMs = 10000,
            LingerMs = 5,
            CompressionType = CompressionType.Lz4,
            AllowAutoCreateTopics = false
        }).Build();
    }

    public Task<DeliveryResult<string, string>> PublishAsync(
        string topic, string key, string payload, string correlationId)
    {
        // Não cancelar a espera quando o cliente HTTP desconectar: uma mensagem
        // enfileirada pode ser publicada mesmo depois de RequestAborted.
        return _producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = key,
            Value = payload,
            Headers = new Headers
            {
                { "X-Correlation-ID", Encoding.UTF8.GetBytes(correlationId) }
            }
        });
    }

    public void Dispose()
    {
        try { _producer.Flush(TimeSpan.FromSeconds(10)); }
        finally { _producer.Dispose(); }
    }
}

public sealed class BrokerHealthCheck : IHealthCheck, IDisposable
{
    private readonly IAdminClient _admin;
    private readonly string[] _topics;

    public BrokerHealthCheck(IConfiguration configuration)
    {
        _admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:19092"
        }).Build();
        _topics = new[]
        {
            configuration["Kafka:Topic"] ?? "frota.telemetria",
            configuration["Kafka:DlqTopic"] ?? "frota.telemetria.dlq"
        };
    }

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            // Metadata é uma consulta síncrona limitada a três segundos.
            var metadata = _admin.GetMetadata(TimeSpan.FromSeconds(3));
            bool ready = metadata.Brokers.Count > 0 && _topics.All(name =>
                metadata.Topics.Any(t => t.Topic == name && !t.Error.IsError &&
                    t.Partitions.Count > 0 && t.Partitions.All(p => !p.Error.IsError && p.Leader >= 0)));
            return Task.FromResult(ready ? HealthCheckResult.Healthy() :
                HealthCheckResult.Unhealthy("Broker ou tópicos não disponíveis."));
        }
        catch (KafkaException)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy("Broker não disponível."));
        }
    }

    public void Dispose() => _admin.Dispose();
}
