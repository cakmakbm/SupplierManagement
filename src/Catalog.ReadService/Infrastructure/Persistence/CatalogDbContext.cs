using Catalog.ReadService.Domain.Entities;
using Microsoft.EntityFrameworkCore;

namespace Catalog.ReadService.Infrastructure.Persistence;

public sealed class CatalogDbContext(DbContextOptions<CatalogDbContext> options) : DbContext(options)
{
    public DbSet<CatalogProduct> Products => Set<CatalogProduct>();
    public DbSet<ProcessedEvent> ProcessedEvents => Set<ProcessedEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var product = modelBuilder.Entity<CatalogProduct>();
        product.ToTable("CatalogProducts");
        product.HasKey(x => x.ProductId);
        product.Property(x => x.Name).HasMaxLength(200).IsRequired();
        product.Property(x => x.SKU).HasMaxLength(64).IsRequired();
        product.Property(x => x.Price).HasPrecision(18, 2);
        modelBuilder.Entity<ProcessedEvent>().ToTable("ProcessedEvents").HasKey(x => x.EventId);
    }
}
