using System.Text.Json;
using Catalog.ReadService.Infrastructure.Messaging;
using Catalog.ReadService.Infrastructure.Persistence;
using Confluent.Kafka;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Shared.Contracts;

namespace Catalog.ReadService.Tests;

public sealed class ProductCreatedMessageHandlerTests : IDisposable
{
    private readonly SqliteConnection _connection = new("DataSource=:memory:");
    private readonly FailingSaveInterceptor _interceptor = new();
    private readonly ServiceProvider _services;
    private readonly ProductCreatedMessageHandler _handler;
    private readonly IConsumer<string, string> _consumer = Substitute.For<IConsumer<string, string>>();

    public ProductCreatedMessageHandlerTests()
    {
        _connection.Open();
        _services = new ServiceCollection()
            .AddDbContext<CatalogDbContext>(o => o.UseSqlite(_connection).AddInterceptors(_interceptor))
            .BuildServiceProvider();
        using (var scope = _services.CreateScope())
            scope.ServiceProvider.GetRequiredService<CatalogDbContext>().Database.EnsureCreated();

        _handler = new ProductCreatedMessageHandler(
            _services.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<ProductCreatedMessageHandler>.Instance);
    }

    public void Dispose()
    {
        _services.Dispose();
        _connection.Dispose();
    }

    private static ProductCreated Event(Guid? eventId = null, Guid? productId = null) =>
        new(eventId ?? Guid.NewGuid(), productId ?? Guid.NewGuid(), Guid.NewGuid(),
            "Industrial Drill", "DRILL-001", 2499.90m, 25, DateTimeOffset.UtcNow);

    private static ConsumeResult<string, string> Result(string value, long offset = 7) => new()
    {
        TopicPartitionOffset = new TopicPartitionOffset("product-created", new Partition(0), new Offset(offset)),
        Message = new Message<string, string> { Key = "supplier", Value = value }
    };

    private static ConsumeResult<string, string> Result(ProductCreated message, long offset = 7) =>
        Result(JsonSerializer.Serialize(message), offset);

    private async Task<(int Products, int ProcessedEvents)> CountsAsync()
    {
        using var scope = _services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<CatalogDbContext>();
        return (await db.Products.CountAsync(), await db.ProcessedEvents.CountAsync());
    }

    [Fact]
    public async Task NewEvent_IsAppliedAndItsOffsetCommitted()
    {
        var result = Result(Event());

        var outcome = await _handler.HandleAsync(_consumer, result, CancellationToken.None);

        Assert.Equal(MessageOutcome.Applied, outcome);
        Assert.Equal((1, 1), await CountsAsync());
        _consumer.Received(1).Commit(result);
        _consumer.DidNotReceive().Seek(Arg.Any<TopicPartitionOffset>());
    }

    [Fact]
    public async Task SameEventDeliveredTwice_CreatesOneProductAndCommitsBoth()
    {
        var message = Event();
        var first = Result(message, offset: 7);
        var redelivered = Result(message, offset: 8);

        await _handler.HandleAsync(_consumer, first, CancellationToken.None);
        var outcome = await _handler.HandleAsync(_consumer, redelivered, CancellationToken.None);

        Assert.Equal(MessageOutcome.Duplicate, outcome);
        Assert.Equal((1, 1), await CountsAsync());
        _consumer.Received(1).Commit(first);
        _consumer.Received(1).Commit(redelivered);
    }

    [Fact]
    public async Task DifferentEventsForSameProduct_DoNotDuplicateTheProduct()
    {
        var productId = Guid.NewGuid();

        await _handler.HandleAsync(_consumer, Result(Event(productId: productId), 7), CancellationToken.None);
        await _handler.HandleAsync(_consumer, Result(Event(productId: productId), 8), CancellationToken.None);

        Assert.Equal((1, 2), await CountsAsync());
    }

    [Fact]
    public async Task DatabaseFailure_RewindsToTheSameMessageAndDoesNotCommit()
    {
        var result = Result(Event(), offset: 42);
        _interceptor.FailNext = true;

        var outcome = await _handler.HandleAsync(_consumer, result, CancellationToken.None);

        Assert.Equal(MessageOutcome.Retry, outcome);
        // Without this seek, the next Consume() returns offset 43; committing it would skip 42 for good.
        _consumer.Received(1).Seek(Arg.Is<TopicPartitionOffset>(tpo =>
            tpo.Topic == "product-created" && tpo.Partition.Value == 0 && tpo.Offset.Value == 42));
        _consumer.DidNotReceive().Commit(Arg.Any<ConsumeResult<string, string>>());
        _consumer.DidNotReceive().StoreOffset(Arg.Any<ConsumeResult<string, string>>());
        Assert.Equal((0, 0), await CountsAsync());
    }

    [Fact]
    public async Task DatabaseFailure_ThenRetry_AppliesTheMessageOnce()
    {
        var result = Result(Event(), offset: 42);
        _interceptor.FailNext = true;

        var firstTry = await _handler.HandleAsync(_consumer, result, CancellationToken.None);
        var retry = await _handler.HandleAsync(_consumer, result, CancellationToken.None);

        Assert.Equal(MessageOutcome.Retry, firstTry);
        Assert.Equal(MessageOutcome.Applied, retry);
        Assert.Equal((1, 1), await CountsAsync());
        _consumer.Received(1).Commit(result);
    }

    [Fact]
    public async Task InvalidJson_IsSkippedAndCommittedSoThePartitionIsNotBlocked()
    {
        var result = Result("{ not json");

        var outcome = await _handler.HandleAsync(_consumer, result, CancellationToken.None);

        Assert.Equal(MessageOutcome.Skipped, outcome);
        _consumer.Received(1).Commit(result);
        _consumer.DidNotReceive().Seek(Arg.Any<TopicPartitionOffset>());
        Assert.Equal((0, 0), await CountsAsync());
    }

    private sealed class FailingSaveInterceptor : SaveChangesInterceptor
    {
        public bool FailNext { get; set; }

        public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
            DbContextEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
        {
            if (FailNext)
            {
                FailNext = false;
                throw new DbUpdateException("Simulated database failure.");
            }
            return base.SavingChangesAsync(eventData, result, cancellationToken);
        }
    }
}
