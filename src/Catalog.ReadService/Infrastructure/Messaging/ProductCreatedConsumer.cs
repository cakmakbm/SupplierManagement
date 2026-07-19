using System.Text.Json;
using Catalog.ReadService.Domain.Entities;
using Catalog.ReadService.Infrastructure.Persistence;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Shared.Contracts;

namespace Catalog.ReadService.Infrastructure.Messaging;

public sealed class ProductCreatedConsumer(
    IServiceScopeFactory scopeFactory,
    IConfiguration configuration,
    ILogger<ProductCreatedConsumer> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topic = configuration["Kafka:Topic"] ?? "product-created";
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServers"],
            GroupId = configuration["Kafka:ConsumerGroup"] ?? "catalog-read-service",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        }).Build();

        consumer.Subscribe(topic);
        logger.LogInformation("Kafka consumer subscribed. Topic={Topic}", topic);

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, string>? result = null;
            try
            {
                result = consumer.Consume(stoppingToken);
                var message = JsonSerializer.Deserialize<ProductCreated>(result.Message.Value);
                if (message is null) throw new JsonException("ProductCreated payload is null.");

                await using var scope = scopeFactory.CreateAsyncScope();
                var db = scope.ServiceProvider.GetRequiredService<CatalogDbContext>();
                if (!await db.ProcessedEvents.AnyAsync(x => x.EventId == message.EventId, stoppingToken))
                {
                    if (!await db.Products.AnyAsync(x => x.ProductId == message.ProductId, stoppingToken))
                        db.Products.Add(new CatalogProduct(message.ProductId, message.SupplierId, message.Name,
                            message.SKU, message.Price, message.Stock, message.OccurredAt));

                    db.ProcessedEvents.Add(new ProcessedEvent(message.EventId));
                    await db.SaveChangesAsync(stoppingToken);
                    logger.LogInformation("ProductCreated consumed. EventId={EventId} ProductId={ProductId} Topic={Topic}",
                        message.EventId, message.ProductId, topic);
                }
                else
                {
                    logger.LogWarning("Duplicate event ignored. EventId={EventId} ProductId={ProductId} Topic={Topic}",
                        message.EventId, message.ProductId, topic);
                }

                consumer.StoreOffset(result);
                consumer.Commit(result);
            }
            catch (ConsumeException ex)
            {
                logger.LogError(ex, "Kafka consume error. Topic={Topic} Reason={Reason}", topic, ex.Error.Reason);
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
            catch (JsonException ex)
            {
                logger.LogError(ex, "Invalid Kafka message. Topic={Topic}", topic);
                if (result is not null)
                    consumer.Commit(result); // Avoid blocking the partition; a later phase can add a dead-letter topic.
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                logger.LogError(ex, "ProductCreated processing failed. Topic={Topic}; offset will not be committed", topic);
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }

        consumer.Close();
    }
}
