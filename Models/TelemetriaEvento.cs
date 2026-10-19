using System.Text.Json.Serialization;

namespace Frota.Api.Ingestao.Models;

/// <summary>
/// Contrato publicado em frota.telemetria (consumido pelo Worker de Processamento).
/// Separado do DTO HTTP (<see cref="Telemetria"/>) para que o contrato Kafka seja explícito.
/// </summary>
public sealed record TelemetriaEvento
{
    public const string MotorLigadoStatus = "LIGADO";
    public const string MotorDesligadoStatus = "DESLIGADO";

    [JsonPropertyName("id_evento")] public Guid IdEvento { get; init; }
    [JsonPropertyName("id_veiculo")] public string IdVeiculo { get; init; } = "";
    [JsonPropertyName("sequencia")] public long Sequencia { get; init; }
    [JsonPropertyName("timestamp")] public DateTimeOffset Timestamp { get; init; }
    [JsonPropertyName("latitude")] public double Latitude { get; init; }
    [JsonPropertyName("longitude")] public double Longitude { get; init; }
    [JsonPropertyName("velocidade")] public double Velocidade { get; init; }
    // Mantido para leitores que já usam o booleano.
    [JsonPropertyName("motor_ligado")] public bool MotorLigado { get; init; }
    // Formato esperado pelo Worker (TelemetriaMensagem.StatusMotor / DeadSensorService).
    [JsonPropertyName("status_motor")] public string StatusMotor { get; init; } = "";

    /// <summary>Mapeia uma telemetria já validada (todos os campos presentes).</summary>
    public static TelemetriaEvento De(Telemetria t) => new()
    {
        IdEvento = t.IdEvento!.Value,
        IdVeiculo = t.IdVeiculo!,
        Sequencia = t.Sequencia!.Value,
        Timestamp = t.Timestamp!.Value,
        Latitude = t.Latitude!.Value,
        Longitude = t.Longitude!.Value,
        Velocidade = t.Velocidade!.Value,
        MotorLigado = t.MotorLigado!.Value,
        StatusMotor = t.MotorLigado!.Value ? MotorLigadoStatus : MotorDesligadoStatus
    };
}
