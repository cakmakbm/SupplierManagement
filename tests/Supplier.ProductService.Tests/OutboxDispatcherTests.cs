using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Shared.Contracts;
using Supplier.ProductService.Application.Abstractions;
using Supplier.ProductService.Application.Products;
using Supplier.ProductService.Infrastructure.Outbox;

namespace Supplier.ProductService.Tests;

public sealed class OutboxDispatcherTests : IDisposable
{
    private readonly TestDatabase _database = new();
    private readonly IProductEventPublisher _publisher = Substitute.For<IProductEventPublisher>();

    public void Dispose() => _database.Dispose();

    private async Task<Guid> CreateProductAsync(string sku)
    {
        await using var db = _database.CreateContext();
        var response = await new CreateProductHandler(db).HandleAsync(
            new CreateProductRequest { SupplierId = Guid.NewGuid(), Name = "Product " + sku, SKU = sku, Price = 10m, Stock = 1 },
            CancellationToken.None);
        return response.Id;
    }

    private async Task<int> DispatchAsync()
    {
        await using var db = _database.CreateContext();
        return await new OutboxDispatcher(db, _publisher, NullLogger<OutboxDispatcher>.Instance)
            .DispatchPendingAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Dispatch_PublishesPendingMessagesInOrderAndMarksThemProcessed()
    {
        var first = await CreateProductAsync("A-1");
        var second = await CreateProductAsync("A-2");
        var publishedIds = new List<Guid>();
        _publisher.PublishAsync(Arg.Do<ProductCreated>(m => publishedIds.Add(m.ProductId)), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var published = await DispatchAsync();

        Assert.Equal(2, published);
        Assert.Equal(new[] { first, second }, publishedIds);
        await using var verify = _database.CreateContext();
        Assert.All(await verify.OutboxMessages.ToListAsync(), m => Assert.NotNull(m.ProcessedAt));
    }

    [Fact]
    public async Task Dispatch_WhenKafkaFails_KeepsMessagePendingAndDoesNotPublishLaterMessages()
    {
        var first = await CreateProductAsync("B-1");
        await CreateProductAsync("B-2");
        _publisher.PublishAsync(Arg.Is<ProductCreated>(m => m.ProductId == first), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("Kafka is unavailable"));

        var published = await DispatchAsync();

        Assert.Equal(0, published);
        await _publisher.Received(1).PublishAsync(Arg.Any<ProductCreated>(), Arg.Any<CancellationToken>());
        await using var verify = _database.CreateContext();
        var messages = await verify.OutboxMessages.OrderBy(m => m.Id).ToListAsync();
        Assert.All(messages, m => Assert.Null(m.ProcessedAt));
        Assert.Equal(1, messages[0].Attempts);
        Assert.Equal("Kafka is unavailable", messages[0].LastError);
    }

    [Fact]
    public async Task Dispatch_AfterKafkaRecovers_PublishesTheSameEventAgain()
    {
        var productId = await CreateProductAsync("C-1");
        var eventIds = new List<Guid>();
        _publisher.PublishAsync(Arg.Any<ProductCreated>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("Kafka is unavailable")), Task.CompletedTask);
        _publisher.When(p => p.PublishAsync(Arg.Any<ProductCreated>(), Arg.Any<CancellationToken>()))
            .Do(call => eventIds.Add(call.Arg<ProductCreated>().EventId));

        Assert.Equal(0, await DispatchAsync());
        Assert.Equal(1, await DispatchAsync());
        Assert.Equal(0, await DispatchAsync());

        // Same EventId on retry, so the consumer can recognise a duplicate.
        Assert.Equal(2, eventIds.Count);
        Assert.Equal(eventIds[0], eventIds[1]);
        await using var verify = _database.CreateContext();
        Assert.NotNull((await verify.OutboxMessages.SingleAsync()).ProcessedAt);
    }
}
