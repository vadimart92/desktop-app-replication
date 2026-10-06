using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Replication.Model;

/// <summary>Names of the service columns the core adds to every replicated table (5.2, 5.4).</summary>
public static class SyncColumns
{
    public const string Version = "SyncVersion";
    public const string Base = "SyncBase";
    public const string Mask = "SyncMask";
    public const string Origin = "SyncOrigin";
    public const string InstanceId = "InstanceId";
    public const string Key = "Id";

    /// <summary><c>InstanceId</c> of the client's own data (5.4).</summary>
    public const string LocalInstance = "local";

    internal static readonly HashSet<string> s_all = [Version, Base, Mask, Origin, InstanceId];

    public static string ArchiveOf(string instanceId) => instanceId + ":archive";

    public static bool IsRemote(string? instanceId) =>
        !string.IsNullOrEmpty(instanceId) && instanceId != LocalInstance && !instanceId.EndsWith(":archive", StringComparison.Ordinal);
}

public sealed record SyncForeignKey(string Column, string ParentTable, bool Cascade);

/// <summary>A replicated table as the core sees it: key, data columns in mask order, FKs.</summary>
public sealed class SyncTable
{
    private readonly Dictionary<string, int> _index;

    internal SyncTable(string name, Type clrType, IReadOnlyList<string> columns, IReadOnlyList<SyncForeignKey> foreignKeys)
    {
        if (columns.Count > 63)
            throw new NotSupportedException($"Table {name} has {columns.Count} data columns; the INTEGER mask supports 63 (design 5.2, open question 13).");
        Name = name;
        ClrType = clrType;
        Columns = columns;
        ForeignKeys = foreignKeys;
        _index = columns.Select((c, i) => (c, i)).ToDictionary(x => x.c, x => x.i, StringComparer.Ordinal);
    }

    public string Name { get; }
    public Type ClrType { get; }

    /// <summary>Data columns without the key and the service columns. Bit i of <c>SyncMask</c> is <c>Columns[i]</c>.</summary>
    public IReadOnlyList<string> Columns { get; }

    public IReadOnlyList<SyncForeignKey> ForeignKeys { get; }

    public long FullMask => (1L << Columns.Count) - 1;

    public int IndexOf(string column) => _index.TryGetValue(column, out int i) ? i : -1;

    public bool HasColumn(string column) => _index.ContainsKey(column);

    public long MaskOf(IEnumerable<string> columns)
    {
        long m = 0;
        foreach (string c in columns)
        {
            int i = IndexOf(c);
            if (i >= 0)
                m |= 1L << i;
        }
        return m;
    }

    public IEnumerable<string> ColumnsOf(long mask)
    {
        for (int i = 0; i < Columns.Count; i++)
        {
            if ((mask & (1L << i)) != 0)
                yield return Columns[i];
        }
    }

    public override string ToString() => Name;
}

/// <summary>The set of replicated tables, built once from the EF model shared by the owner and the client.</summary>
public sealed class SyncModel
{
    private readonly Dictionary<string, SyncTable> _byName;
    private readonly Dictionary<Type, SyncTable> _byType;

    private SyncModel(IReadOnlyList<SyncTable> tables)
    {
        Tables = tables;
        _byName = tables.ToDictionary(t => t.Name, StringComparer.Ordinal);
        _byType = tables.ToDictionary(t => t.ClrType);
    }

    /// <summary>Tables ordered parents first.</summary>
    public IReadOnlyList<SyncTable> Tables { get; }

    public SyncTable this[string name] => _byName[name];

    public bool TryGet(string name, out SyncTable table) => _byName.TryGetValue(name, out table!);

    public SyncTable? ForType(Type clrType) => _byType.GetValueOrDefault(clrType);

    /// <summary>Tables with an FK to <paramref name="parent"/>, with that FK.</summary>
    public IEnumerable<(SyncTable Child, SyncForeignKey Fk)> ChildrenOf(string parent) =>
        from t in Tables
        from fk in t.ForeignKeys
        where fk.ParentTable == parent
        select (t, fk);

    public static SyncModel From(DbContext context) => From(context.Model);

    public static SyncModel From(IModel model)
    {
        var tables = new List<SyncTable>();
        foreach (IEntityType et in model.GetEntityTypes())
        {
            if (et.FindAnnotation(ReplicationModelBuilderExtensions.ReplicatedAnnotation)?.Value is not true)
                continue;
            string tableName = et.GetTableName()!;
            StoreObjectIdentifier store = StoreObjectIdentifier.Table(tableName, et.GetSchema());
            IKey key = et.FindPrimaryKey() ?? throw new InvalidOperationException($"{tableName}: no primary key");
            if (key.Properties.Count != 1 || key.Properties[0].ClrType != typeof(Guid) || key.Properties[0].GetColumnName(store) != SyncColumns.Key)
                throw new NotSupportedException($"{tableName}: a replicated table needs a single Guid key column named Id (design 5.1).");

            List<string> columns = et.GetProperties()
                .Where(p => !p.IsPrimaryKey())
                .Select(p => p.GetColumnName(store)!)
                .Where(c => !SyncColumns.s_all.Contains(c))
                .ToList();

            List<SyncForeignKey> fks = et.GetForeignKeys()
                .Where(fk => fk.Properties.Count == 1)
                .Select(fk => new SyncForeignKey(
                    fk.Properties[0].GetColumnName(store)!,
                    fk.PrincipalEntityType.GetTableName()!,
                    fk.DeleteBehavior == DeleteBehavior.Cascade))
                .ToList();

            tables.Add(new SyncTable(tableName, et.ClrType, columns, fks));
        }

        // Parents first, so FK-ordered work (snapshot copy, archive) has a stable order.
        var ordered = new List<SyncTable>();
        var visiting = new HashSet<string>();
        Dictionary<string, SyncTable> byName = tables.ToDictionary(t => t.Name);

        void Visit(SyncTable t)
        {
            if (ordered.Contains(t) || !visiting.Add(t.Name))
                return;
            foreach (SyncForeignKey fk in t.ForeignKeys)
            {
                if (byName.TryGetValue(fk.ParentTable, out SyncTable? p) && p != t)
                    Visit(p);
            }

            ordered.Add(t);
        }

        foreach (SyncTable t in tables.OrderBy(t => t.Name, StringComparer.Ordinal))
            Visit(t);
        return new SyncModel(ordered);
    }
}
