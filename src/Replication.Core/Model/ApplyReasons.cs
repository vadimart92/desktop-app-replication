namespace Replication.Model;

/// <summary>Reason strings of ActionResult, sent by the owner and read by the client (6.5).</summary>
internal static class ApplyReasons
{
    public const string Deleted = "deleted";
    public const string ParentDeleted = "parent deleted";
    public const string Unique = "unique";
    public const string AlreadyApplied = "already applied";
    public const string Echo = "echo";
    public const string UnknownTable = "unknown table";
    public const string UnknownColumn = "unknown column";
}
