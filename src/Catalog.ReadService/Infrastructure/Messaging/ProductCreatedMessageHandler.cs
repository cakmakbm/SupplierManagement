using System.Text.Json;
using Catalog.ReadService.Domain.Entities;
using Catalog.ReadService.Infrastructure.Persistence;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Shared.Contracts;

namespace Catalog.ReadService.Infrastructure.Messaging;

public enum MessageOutcome
{
    /// <summary>The event was applied to the read model and its offset committed.</summary>
    Applied,
    /// <summary>The event was already processed; its offset was committed without changes.</summary>
    Duplicate,
    /// <summary>The payload could not be parsed; its offset was committed so the partition is not blocked.</summary>
    Skipped,
    /// <summary>Processing failed; the consumer was rewound so the same message is read again.</summary>
    Retry
}

/// <summary>
/// Handles one Kafka message: applies it idempotently and decides what happens to the offset.
/// Kept separate from the polling loop so the offset rules can be unit tested.
/// </summary>
public sealed class ProductCreatedMessageHandler(
    IServiceScopeFactory scopeFactory,
    ILogger<ProductCreatedMessageHandler> logger)
{
    public async Task<MessageOutcome> HandleAsync(
        IConsumer<string, string> consumer,
        ConsumeResult<string, string> result,
        CancellationToken cancellationToken)
    {
        ProductCreated? message;
        try
        {
            message = JsonSerializer.Deserialize<ProductCreated>(result.Message.Value);
            if (message is null) throw new JsonException("ProductCreated payload is null.");
        }
        catch (JsonException ex)
        {
            logger.LogError(ex, "Invalid Kafka message skipped. Topic={Topic} Partition={Partition} Offset={Offset}",
                result.Topic, result.Partition.Value, result.Offset.Value);
            // A malformed message will never succeed; commit it so the partition is not blocked.
            // A later phase can route it to a dead-letter topic instead.
            Commit(consumer, result);
            return MessageOutcome.Skipped;
        }

        try
        {
            var applied = await ApplyAsync(message, cancellationToken);
            Commit(consumer, result);

            if (applied)
                logger.LogInformation("ProductCreated consumed. EventId={EventId} ProductId={ProductId}",
                    message.EventId, message.ProductId);
            else
                logger.LogWarning("Duplicate event ignored. EventId={EventId} ProductId={ProductId}",
                    message.EventId, message.ProductId);

            return applied ? MessageOutcome.Applied : MessageOutcome.Duplicate;
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            // Consume() has already moved the in-memory position past this message. Without a seek,
            // the next Consume() returns the following message, and committing that one would skip
            // this message for good. Rewind so the same message is retried.
            consumer.Seek(result.TopicPartitionOffset);
            logger.LogError(ex, "ProductCreated processing failed; will retry. EventId={EventId} Partition={Partition} Offset={Offset}",
                message.EventId, result.Partition.Value, result.Offset.Value);
            return MessageOutcome.Retry;
        }
    }

    private async Task<bool> ApplyAsync(ProductCreated message, CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<CatalogDbContext>();

        if (await db.ProcessedEvents.AnyAsync(x => x.EventId == message.EventId, cancellationToken))
            return false;

        if (!await db.Products.AnyAsync(x => x.ProductId == message.ProductId, cancellationToken))
            db.Products.Add(new CatalogProduct(message.ProductId, message.SupplierId, message.Name,
                message.SKU, message.Price, message.Stock, message.OccurredAt));

        // The product and the processed-event marker are saved in one transaction.
        db.ProcessedEvents.Add(new ProcessedEvent(message.EventId));
        await db.SaveChangesAsync(cancellationToken);
        return true;
    }

    private static void Commit(IConsumer<string, string> consumer, ConsumeResult<string, string> result)
    {
        consumer.StoreOffset(result);
        consumer.Commit(result);
    }
}
