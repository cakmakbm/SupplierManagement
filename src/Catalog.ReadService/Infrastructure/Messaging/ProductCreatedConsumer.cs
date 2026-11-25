using Confluent.Kafka;

namespace Catalog.ReadService.Infrastructure.Messaging;

public sealed class ProductCreatedConsumer(
    ProductCreatedMessageHandler handler,
    IConfiguration configuration,
    ILogger<ProductCreatedConsumer> logger) : BackgroundService
{
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(2);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Consume() blocks; yield first so host startup is not held up.
        await Task.Yield();

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

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(stoppingToken);
                    var outcome = await handler.HandleAsync(consumer, result, stoppingToken);
                    if (outcome == MessageOutcome.Retry)
                        await Task.Delay(RetryDelay, stoppingToken);
                }
                catch (ConsumeException ex)
                {
                    logger.LogError(ex, "Kafka consume error. Topic={Topic} Reason={Reason}", topic, ex.Error.Reason);
                    await Task.Delay(RetryDelay, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
        finally
        {
            consumer.Close();
        }
    }
}
