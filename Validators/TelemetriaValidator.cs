using FluentValidation;
using Frota.Api.Ingestao.Models;

namespace Frota.Api.Ingestao.Validators;

public sealed class TelemetriaValidator : AbstractValidator<Telemetria>
{
    public TelemetriaValidator()
    {
        RuleFor(x => x.IdEvento).NotNull().Must(x => x != Guid.Empty)
            .WithMessage("id_evento deve ser um UUID não vazio.");
        RuleFor(x => x.IdVeiculo).NotEmpty().MaximumLength(100)
            .Matches(@"^[A-Za-z0-9_-]+$")
            .WithMessage("id_veiculo aceita letras, números, hífen e underscore (até 100).");
        RuleFor(x => x.Sequencia).NotNull().Must(x => x is >= 0)
            .WithMessage("sequencia deve ser um inteiro não negativo.");
        RuleFor(x => x.Timestamp).NotNull().Must(x => x != DateTimeOffset.MinValue)
            .WithMessage("timestamp é obrigatório.");
        RuleFor(x => x.Latitude).NotNull()
            .Must(x => x.HasValue && double.IsFinite(x.Value) && x.Value >= -90 && x.Value <= 90)
            .WithMessage("latitude deve estar entre -90 e 90.");
        RuleFor(x => x.Longitude).NotNull()
            .Must(x => x.HasValue && double.IsFinite(x.Value) && x.Value >= -180 && x.Value <= 180)
            .WithMessage("longitude deve estar entre -180 e 180.");
        RuleFor(x => x.Velocidade).NotNull()
            .Must(x => x.HasValue && double.IsFinite(x.Value) && x.Value >= 0)
            .WithMessage("velocidade deve ser um número finito não negativo.");
        RuleFor(x => x.MotorLigado).NotNull();
    }
}
