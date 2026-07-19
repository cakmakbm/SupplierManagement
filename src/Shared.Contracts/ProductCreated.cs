namespace Shared.Contracts;

public sealed record ProductCreated(
    Guid EventId,
    Guid ProductId,
    Guid SupplierId,
    string Name,
    string SKU,
    decimal Price,
    int Stock,
    DateTimeOffset OccurredAt);
