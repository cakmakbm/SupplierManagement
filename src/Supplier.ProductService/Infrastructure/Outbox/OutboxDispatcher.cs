using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Shared.Contracts;
using Supplier.ProductService.Application.Abstractions;
using Supplier.ProductService.Infrastructure.Persistence;

namespace Supplier.ProductService.Infrastructure.Outbox;

/// <summary>
/// Publishes unsent outbox messages to Kafka in insertion order.
/// Delivery is at-least-once: if the process stops after Kafka accepts a message but before
/// ProcessedAt is saved, the message is published again with the same EventId, and the
/// catalog consumer's EventId check ignores the duplicate.
/// </summary>
public sealed class OutboxDispatcher(
    ProductDbContext dbContext,
    IProductEventPublisher publisher,
    ILogger<OutboxDispatcher> logger)
{
    public const int BatchSize = 50;

    /// <returns>The number of messages published in this pass.</returns>
    public async Task<int> DispatchPendingAsync(CancellationToken cancellationToken)
    {
        var pending = await dbContext.OutboxMessages
            .Where(x => x.ProcessedAt == null)
            .OrderBy(x => x.Id)
            .Take(BatchSize)
            .ToListAsync(cancellationToken);

        var published = 0;
        foreach (var outboxMessage in pending)
        {
            try
            {
                var message = outboxMessage.Type switch
                {
                    nameof(ProductCreated) => JsonSerializer.Deserialize<ProductCreated>(outboxMessage.Payload)
                        ?? throw new JsonException("ProductCreated payload is null."),
                    _ => throw new InvalidOperationException($"Unknown outbox message type '{outboxMessage.Type}'.")
                };

                await publisher.PublishAsync(message, cancellationToken);
                outboxMessage.MarkProcessed(DateTimeOffset.UtcNow);
                published++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                outboxMessage.MarkFailed(ex.Message);
                logger.LogWarning(ex, "Outbox publish failed; will retry. OutboxId={OutboxId} EventId={EventId} Attempts={Attempts}",
                    outboxMessage.Id, outboxMessage.EventId, outboxMessage.Attempts);
                // Stop at the first failure so later events are not published ahead of this one.
                break;
            }
        }

        await dbContext.SaveChangesAsync(cancellationToken);
        return published;
    }
}
