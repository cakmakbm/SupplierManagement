namespace Supplier.ProductService.Infrastructure.Outbox;

/// <summary>Background worker that keeps draining the outbox.</summary>
public sealed class OutboxPublisherService(
    IServiceScopeFactory scopeFactory,
    ILogger<OutboxPublisherService> logger) : BackgroundService
{
    private static readonly TimeSpan IdleDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan ErrorDelay = TimeSpan.FromSeconds(5);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var scope = scopeFactory.CreateAsyncScope();
                var dispatcher = scope.ServiceProvider.GetRequiredService<OutboxDispatcher>();
                var published = await dispatcher.DispatchPendingAsync(stoppingToken);

                // A full batch means more may be waiting; poll again right away.
                if (published < OutboxDispatcher.BatchSize)
                    await Task.Delay(IdleDelay, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // For example the database is not reachable yet.
                logger.LogError(ex, "Outbox dispatch pass failed.");
                await Task.Delay(ErrorDelay, stoppingToken);
            }
        }
    }
}
