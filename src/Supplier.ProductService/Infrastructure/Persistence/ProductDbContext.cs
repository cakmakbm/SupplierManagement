using Microsoft.EntityFrameworkCore;
using Supplier.ProductService.Domain.Entities;

namespace Supplier.ProductService.Infrastructure.Persistence;

public sealed class ProductDbContext(DbContextOptions<ProductDbContext> options) : DbContext(options)
{
    public DbSet<Product> Products => Set<Product>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var product = modelBuilder.Entity<Product>();
        product.ToTable("Products");
        product.HasKey(x => x.Id);
        product.Property(x => x.Name).HasMaxLength(200).IsRequired();
        product.Property(x => x.SKU).HasMaxLength(64).IsRequired();
        product.Property(x => x.Price).HasPrecision(18, 2);
        product.HasIndex(x => new { x.SupplierId, x.SKU }).IsUnique();
    }
}
