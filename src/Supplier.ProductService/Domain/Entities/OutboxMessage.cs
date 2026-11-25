namespace Supplier.ProductService.Domain.Entities;

/// <summary>
/// An event waiting to be published to Kafka. It is written in the same database
/// transaction as the business change, so the change and its event can never diverge.
/// </summary>
public sealed class OutboxMessage
{
    private OutboxMessage() { }

    public OutboxMessage(Guid eventId, string type, string key, string payload, DateTimeOffset occurredAt)
    {
        EventId = eventId;
        Type = type;
        Key = key;
        Payload = payload;
        OccurredAt = occurredAt;
    }

    /// <summary>Insertion order; messages are published in this order.</summary>
    public long Id { get; private set; }
    public Guid EventId { get; private set; }
    public string Type { get; private set; } = string.Empty;
    public string Key { get; private set; } = string.Empty;
    public string Payload { get; private set; } = string.Empty;
    public DateTimeOffset OccurredAt { get; private set; }
    public DateTimeOffset? ProcessedAt { get; private set; }
    public int Attempts { get; private set; }
    public string? LastError { get; private set; }

    public void MarkProcessed(DateTimeOffset processedAt)
    {
        ProcessedAt = processedAt;
        LastError = null;
    }

    public void MarkFailed(string error)
    {
        Attempts++;
        LastError = error.Length > 2000 ? error[..2000] : error;
    }
}
