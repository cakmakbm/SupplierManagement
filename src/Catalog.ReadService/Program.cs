using Catalog.ReadService.Infrastructure.Messaging;
using Catalog.ReadService.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Serilog;

Log.Logger = new LoggerConfiguration().WriteTo.Console().CreateBootstrapLogger();
try
{
    var builder = WebApplication.CreateBuilder(args);
    builder.Host.UseSerilog((context, services, configuration) => configuration
        .ReadFrom.Configuration(context.Configuration).ReadFrom.Services(services).Enrich.FromLogContext().WriteTo.Console());
    builder.Services.AddControllers();
    builder.Services.AddEndpointsApiExplorer();
    builder.Services.AddSwaggerGen();
    builder.Services.AddDbContext<CatalogDbContext>(options =>
        options.UseSqlServer(builder.Configuration.GetConnectionString("CatalogDb")));
    builder.Services.AddHostedService<DatabaseInitializer>();
    builder.Services.AddSingleton<ProductCreatedMessageHandler>();
    builder.Services.AddHostedService<ProductCreatedConsumer>();
    builder.Services.AddHealthChecks().AddDbContextCheck<CatalogDbContext>();

    var app = builder.Build();
    app.UseSerilogRequestLogging();
    app.UseSwagger();
    app.UseSwaggerUI();
    app.MapControllers();
    app.MapHealthChecks("/health");
    app.Run();
}
catch (Exception ex) when (ex is not HostAbortedException) { Log.Fatal(ex, "Catalog.ReadService terminated unexpectedly"); }
finally { await Log.CloseAndFlushAsync(); }
