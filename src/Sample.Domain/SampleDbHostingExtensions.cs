using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace Sample.Domain;

public static class SampleDbHostingExtensions
{
    /// <param name="interceptors">Added after the sample's own, e.g. the client's <c>WriteRouter</c>.</param>
    public static IServiceCollection AddSampleDb(this IServiceCollection services, string dbPath, Func<IServiceProvider, IInterceptor[]>? interceptors = null) =>
        services.AddDbContextFactory<SampleDbContext>((sp, o) => SampleDbContext.Configure(o, dbPath, interceptors?.Invoke(sp) ?? []));

    /// <summary>Brings the database up to the current model. Call before the store is installed.</summary>
    public static IServiceProvider MigrateSampleDb(this IServiceProvider services)
    {
        using SampleDbContext db = services.GetRequiredService<IDbContextFactory<SampleDbContext>>().CreateDbContext();
        // The sample ships no migrations yet: create the schema straight from the model.
        if (db.Database.GetMigrations().Any())
            db.Database.Migrate();
        else
            db.Database.EnsureCreated();
        return services;
    }
}
