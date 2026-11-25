using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Supplier.ProductService.Tests;

/// <summary>Makes the next SaveChanges call fail, to simulate a database error.</summary>
public sealed class FailingSaveInterceptor : SaveChangesInterceptor
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
