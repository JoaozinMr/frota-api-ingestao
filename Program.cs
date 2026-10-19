using System.Text.Json;
using Confluent.Kafka;
using FluentValidation;
using Frota.Api.Ingestao.Models;
using Frota.Api.Ingestao.Services;
using Frota.Api.Ingestao.Validators;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Serilog;
using Serilog.Context;
using Serilog.Events;
using Serilog.Formatting.Compact;

const string ServiceName = "frota-api-ingestao";

// Mesmo padrão de logging do Worker e da API de Alertas (Service, RenderedCompactJsonFormatter),
// para que os logs dos três serviços possam ser cruzados por CorrelationId/IdVeiculo/Service.
Log.Logger = new LoggerConfiguration()
    .Enrich.FromLogContext()
    .Enrich.WithProperty("Service", ServiceName)
    .WriteTo.Console(new RenderedCompactJsonFormatter())
    .CreateBootstrapLogger();

try
{
    var builder = WebApplication.CreateBuilder(args);
    // Corpo limitado para proteger memória e mensagens Kafka. Oversize retorna 413,
    // não DLQ: essa exceção de transporte é documentada no README.
    builder.WebHost.ConfigureKestrel(options => options.Limits.MaxRequestBodySize = 65536);
    builder.Host.UseSerilog((context, logger) => logger
        .MinimumLevel.Information()
        .Enrich.FromLogContext()
        .Enrich.WithProperty("Service", ServiceName)
        .WriteTo.Console(new RenderedCompactJsonFormatter()));
    builder.Services.AddSingleton<IValidator<Telemetria>, TelemetriaValidator>();
    builder.Services.AddSingleton<KafkaPublisher>();
    builder.Services.AddHealthChecks().AddCheck<BrokerHealthCheck>("broker", tags: new[] { "ready" });
    var app = builder.Build();

    app.Use(async (context, next) =>
    {
        var supplied = context.Request.Headers["X-Correlation-ID"].ToString();
        // GUID canônico evita inserir conteúdo arbitrário do cliente nos logs/headers.
        var correlationId = Guid.TryParse(supplied, out var id) ? id.ToString() : Guid.NewGuid().ToString();
        context.Items["CorrelationId"] = correlationId;
        context.Response.Headers["X-Correlation-ID"] = correlationId;
        using (LogContext.PushProperty("CorrelationId", correlationId))
            await next(context);
    });
    app.UseSerilogRequestLogging(o =>
    {
        // Health é consultado com frequência por Docker/balanceador: rebaixa para Debug.
        o.GetLevel = (ctx, _, ex) =>
            ex is not null || ctx.Response.StatusCode >= 500 ? LogEventLevel.Error
            : ctx.Request.Path.StartsWithSegments("/health") ? LogEventLevel.Debug
            : LogEventLevel.Information;
    });

    app.MapGet("/", () => Results.Ok(new
    {
        servico = "Frota API Ingestao",
        status = "online",
        endpoints = new
        {
            health = "/health",
            readiness = "/health/ready",
            telemetria = "POST /api/telemetry"
        }
    }));

    app.MapHealthChecks("/health", new HealthCheckOptions { Predicate = _ => false });
    app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = x => x.Tags.Contains("ready") });

    app.MapPost("/api/telemetry", async (
        HttpContext context, IValidator<Telemetria> validator,
        KafkaPublisher publisher, ILogger<Program> logger) =>
    {
        var correlationId = (string)context.Items["CorrelationId"]!;
        using var reader = new StreamReader(context.Request.Body);
        var raw = await reader.ReadToEndAsync(context.RequestAborted);
        Telemetria? telemetry = null;
        var errors = new List<string>();
        var errorType = "ValidationFailure";
        try
        {
            using var document = JsonDocument.Parse(raw);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
                errors.Add("O corpo deve ser um objeto JSON.");
            else
            {
                // DateTimeOffset pode aceitar timestamps sem fuso; o contrato exige
                // UTC Z ou offset explícito, evitando depender do fuso do servidor.
                if (!document.RootElement.TryGetProperty("timestamp", out var stamp) ||
                    stamp.ValueKind != JsonValueKind.String ||
                    !System.Text.RegularExpressions.Regex.IsMatch(stamp.GetString()!,
                        @"T.*(?:Z|[+-]\d{2}:\d{2})$"))
                    errors.Add("timestamp deve usar ISO 8601 com Z ou offset explícito.");
                telemetry = JsonSerializer.Deserialize<Telemetria>(raw);
                if (telemetry is null) errors.Add("Telemetria vazia.");
                else
                {
                    var validation = await validator.ValidateAsync(telemetry, context.RequestAborted);
                    errors.AddRange(validation.Errors.Select(x => x.ErrorMessage));
                }
            }
        }
        catch (JsonException)
        {
            errors.Add("JSON inválido ou tipos de campos incompatíveis.");
            errorType = nameof(JsonException);
        }

        var invalid = errors.Count > 0;
        var motivos = errors.Distinct().ToArray();
        // IdVeiculo no LogContext: mesmo nome de propriedade usado pelo Worker e pela API de Alertas.
        using var vehicleScope = invalid ? null : LogContext.PushProperty("IdVeiculo", telemetry!.IdVeiculo);
        var topic = invalid ? publisher.DlqTopic : publisher.Topic;
        var key = invalid ? correlationId : telemetry!.IdVeiculo!;
        var value = invalid
            ? JsonSerializer.Serialize(new
            {
                payload_original = raw,
                motivos,
                recebido_em = DateTimeOffset.UtcNow,
                correlation_id = correlationId
            })
            // Contrato Kafka explícito: inclui status_motor ("LIGADO"/"DESLIGADO") esperado pelo Worker.
            : JsonSerializer.Serialize(TelemetriaEvento.De(telemetry!));
        // Mesmos headers dlq-* gravados pelo Worker, para identificar a origem na DLQ compartilhada.
        var dlqHeaders = invalid
            ? new Dictionary<string, string>
            {
                ["dlq-error"] = string.Join("; ", motivos),
                ["dlq-exception-type"] = errorType,
                ["dlq-origin"] = ServiceName
            }
            : null;
        try
        {
            var delivery = await publisher.PublishAsync(topic, key, value, correlationId, dlqHeaders);
            if (invalid)
            {
                logger.LogWarning("Pacote encaminhado para DLQ {Topic} {Partition} {Offset}",
                    topic, delivery.Partition.Value, delivery.Offset.Value);
                return Results.BadRequest(new { status = "rejeitado", erros = motivos, correlation_id = correlationId });
            }
            logger.LogInformation("Evento {IdEvento} de {IdVeiculo} publicado em {Partition} {Offset}",
                telemetry!.IdEvento, telemetry.IdVeiculo, delivery.Partition.Value, delivery.Offset.Value);
            return Results.Accepted(value: new
            {
                status = "aceito", id_evento = telemetry.IdEvento,
                correlation_id = correlationId, particao = delivery.Partition.Value,
                offset = delivery.Offset.Value
            });
        }
        catch (KafkaException exception)
        {
            logger.LogError(exception, "Não foi possível confirmar publicação em {Topic}", topic);
            context.Response.Headers["Retry-After"] = "2";
            return Results.Json(new { status = "publicacao_nao_confirmada", correlation_id = correlationId }, statusCode: 503);
        }
    });
    app.Run();
}
catch (Exception ex) when (ex is not HostAbortedException)
{
    Log.Fatal(ex, "API de Ingestão encerrada inesperadamente");
    throw;
}
finally
{
    Log.CloseAndFlush();
}
