using System.Text.Json.Serialization;

namespace Frota.Api.Ingestao.Models;

// Nullable permite distinguir um campo ausente de zero/false.
public sealed record Telemetria
{
    [JsonPropertyName("id_evento")] public Guid? IdEvento { get; init; }
    [JsonPropertyName("id_veiculo")] public string? IdVeiculo { get; init; }
    [JsonPropertyName("sequencia")] public long? Sequencia { get; init; }
    [JsonPropertyName("timestamp")] public DateTimeOffset? Timestamp { get; init; }
    [JsonPropertyName("latitude")] public double? Latitude { get; init; }
    [JsonPropertyName("longitude")] public double? Longitude { get; init; }
    [JsonPropertyName("velocidade")] public double? Velocidade { get; init; }
    [JsonPropertyName("motor_ligado")] public bool? MotorLigado { get; init; }
}
