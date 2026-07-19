namespace Catalog.ReadService.Domain.Entities;

public sealed class ProcessedEvent
{
    private ProcessedEvent() { }
    public ProcessedEvent(Guid eventId) { EventId = eventId; ProcessedAt = DateTimeOffset.UtcNow; }
    public Guid EventId { get; private set; }
    public DateTimeOffset ProcessedAt { get; private set; }
}
