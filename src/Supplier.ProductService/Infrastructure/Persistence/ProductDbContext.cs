using Microsoft.EntityFrameworkCore;
using Supplier.ProductService.Domain.Entities;

namespace Supplier.ProductService.Infrastructure.Persistence;

public sealed class ProductDbContext(DbContextOptions<ProductDbContext> options) : DbContext(options)
{
    public DbSet<Product> Products => Set<Product>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var product = modelBuilder.Entity<Product>();
        product.ToTable("Products");
        product.HasKey(x => x.Id);
        product.Property(x => x.Name).HasMaxLength(200).IsRequired();
        product.Property(x => x.SKU).HasMaxLength(64).IsRequired();
        product.Property(x => x.Price).HasPrecision(18, 2);
        product.HasIndex(x => new { x.SupplierId, x.SKU }).IsUnique();

        var outbox = modelBuilder.Entity<OutboxMessage>();
        outbox.ToTable("OutboxMessages");
        outbox.HasKey(x => x.Id);
        outbox.Property(x => x.Id).ValueGeneratedOnAdd();
        outbox.HasIndex(x => x.EventId).IsUnique();
        outbox.Property(x => x.Type).HasMaxLength(200).IsRequired();
        outbox.Property(x => x.Key).HasMaxLength(200).IsRequired();
        outbox.Property(x => x.Payload).IsRequired();
        outbox.Property(x => x.LastError).HasMaxLength(2000);
        // The dispatcher polls for unsent messages in insertion order.
        outbox.HasIndex(x => new { x.ProcessedAt, x.Id });
    }
}
