using System.Text.Json;
using Confluent.Kafka;
using Shared.Contracts;
using Supplier.ProductService.Application.Abstractions;

namespace Supplier.ProductService.Infrastructure.Messaging;

public sealed class KafkaProductEventPublisher : IProductEventPublisher, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaProductEventPublisher> _logger;
    private readonly string _topic;

    public KafkaProductEventPublisher(IConfiguration configuration, ILogger<KafkaProductEventPublisher> logger)
    {
        _logger = logger;
        _topic = configuration["Kafka:Topic"] ?? "product-created";
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServers"],
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();
    }

    public async Task PublishAsync(ProductCreated message, CancellationToken cancellationToken)
    {
        var result = await _producer.ProduceAsync(_topic, new Message<string, string>
        {
            Key = message.SupplierId.ToString(),
            Value = JsonSerializer.Serialize(message)
        }, cancellationToken);

        _logger.LogInformation("ProductCreated published. EventId={EventId} ProductId={ProductId} Topic={Topic} Partition={Partition} Offset={Offset}",
            message.EventId, message.ProductId, _topic, result.Partition.Value, result.Offset.Value);
    }

    public void Dispose() => _producer.Dispose();
}
