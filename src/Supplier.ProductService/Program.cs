using Microsoft.EntityFrameworkCore;
using Serilog;
using Supplier.ProductService.Application.Abstractions;
using Supplier.ProductService.Application.Products;
using Supplier.ProductService.Infrastructure.Messaging;
using Supplier.ProductService.Infrastructure.Outbox;
using Supplier.ProductService.Infrastructure.Persistence;

Log.Logger = new LoggerConfiguration().WriteTo.Console().CreateBootstrapLogger();

try
{
    var builder = WebApplication.CreateBuilder(args);
    builder.Host.UseSerilog((context, services, configuration) => configuration
        .ReadFrom.Configuration(context.Configuration).ReadFrom.Services(services).Enrich.FromLogContext().WriteTo.Console());
    builder.Services.AddControllers();
    builder.Services.AddEndpointsApiExplorer();
    builder.Services.AddSwaggerGen();
    builder.Services.AddDbContext<ProductDbContext>(options =>
        options.UseSqlServer(builder.Configuration.GetConnectionString("ProductDb")));
    builder.Services.AddScoped<CreateProductHandler>();
    builder.Services.AddSingleton<IProductEventPublisher, KafkaProductEventPublisher>();
    builder.Services.AddHostedService<DatabaseInitializer>();
    builder.Services.AddScoped<OutboxDispatcher>();
    builder.Services.AddHostedService<OutboxPublisherService>();
    builder.Services.AddHealthChecks().AddDbContextCheck<ProductDbContext>();

    var app = builder.Build();
    app.UseSerilogRequestLogging();
    app.UseSwagger();
    app.UseSwaggerUI();
    app.MapControllers();
    app.MapHealthChecks("/health");
    app.Run();
}
catch (Exception ex) when (ex is not HostAbortedException)
{
    Log.Fatal(ex, "ProductService terminated unexpectedly");
}
finally { await Log.CloseAndFlushAsync(); }

// Lets WebApplicationFactory<Program> host the API in tests.
public partial class Program;
