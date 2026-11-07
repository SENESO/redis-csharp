using System.Buffers;
using System.Security.Cryptography;
using System.Text;
using RedisServer.Core;

namespace RedisServer.Protocol;

/// <summary>
/// Cluster mode over a static slot map (--cluster-enabled yes
/// --cluster-slots "start-end@host:port,..."). The map is the whole
/// topology: identical on every node, no gossip, no failover — this
/// node serves the ranges whose port is its own listen port and
/// answers MOVED for the rest. Slot math is real Redis CRC16 with
/// hash tags, so clients and slot numbers agree with a real cluster.
/// </summary>
public sealed partial class CommandProcessor
{
    private ClusterNodeRange? FindSlotOwner(int slot)
    {
        foreach (var range in _clusterMap)
            if (slot >= range.Start && slot <= range.End)
                return range;
        return null;
    }

    /// <summary>Node id for a map endpoint: this node uses its own id; peers derive one deterministically from their address.</summary>
    private string ClusterNodeId(ClusterNodeRange range) =>
        range.Port == _port
            ? _clusterNodeId
            : Convert.ToHexString(SHA1.HashData(Encoding.ASCII.GetBytes($"{range.Host}:{range.Port}"))).ToLowerInvariant();

    private void ClusterCommand(byte[][] args, ClientContext ctx, IBufferWriter<byte> output, List<byte[][]> effects, List<int> effectDbs)
    {
        if (!_clusterEnabled)
            throw new RedisCommandException("ERR This instance has cluster support disabled");
        var sub = NameOf(args[1]);
        switch (sub.ToUpperInvariant())
        {
            case "INFO":
                RequireSubArity(args, 2, "cluster|info");
                ClusterInfo(output);
                break;
            case "SLOTS":
                RequireSubArity(args, 2, "cluster|slots");
                ClusterSlots(output);
                break;
            case "NODES":
                RequireSubArity(args, 2, "cluster|nodes");
                ClusterNodes(output);
                break;
            case "KEYSLOT":
                RequireSubArity(args, 3, "cluster|keyslot");
                RespWriter.WriteInteger(output, ClusterSlot.OfKey(args[2]));
                break;
            case "COUNTKEYSINSLOT":
                RequireSubArity(args, 3, "cluster|countkeysinslot");
                var slotValue = ParseLong(args[2]);
                if (slotValue < 0 || slotValue >= ClusterSlot.SlotCount)
                    throw new RedisCommandException("ERR Slot out of range");
                var count = 0;
                foreach (var key in _store.AllKeysSorted(0))
                    if (ClusterSlot.OfKey(key) == (int)slotValue)
                        count++;
                RespWriter.WriteInteger(output, count);
                break;
            default:
                throw new RedisCommandException(
                    $"ERR unknown subcommand '{RespParser.Describe(args[1])}'. Try CLUSTER HELP.");
        }
    }

    private void ClusterInfo(IBufferWriter<byte> output)
    {
        var covered = new bool[ClusterSlot.SlotCount];
        var endpoints = new HashSet<(string, int)>();
        foreach (var range in _clusterMap)
        {
            endpoints.Add((range.Host, range.Port));
            for (var s = range.Start; s <= range.End; s++)
                covered[s] = true;
        }
        var assigned = covered.Count(c => c);
        var text = string.Join("\r\n",
            "cluster_enabled:1",
            $"cluster_state:{(assigned == ClusterSlot.SlotCount ? "ok" : "fail")}",
            $"cluster_slots_assigned:{assigned}",
            $"cluster_slots_ok:{assigned}",
            "cluster_slots_pfail:0",
            "cluster_slots_fail:0",
            $"cluster_known_nodes:{endpoints.Count}",
            $"cluster_size:{endpoints.Count}",
            "cluster_current_epoch:0",
            "cluster_my_epoch:0",
            "cluster_stats_messages_sent:0",
            "cluster_stats_messages_received:0",
            "total_cluster_links_buffer_limit_exceeded:0") + "\r\n";
        RespWriter.WriteBulkString(output, Encoding.ASCII.GetBytes(text));
    }

    private void ClusterSlots(IBufferWriter<byte> output)
    {
        RespWriter.WriteArrayHeader(output, _clusterMap.Count);
        foreach (var range in _clusterMap)
        {
            RespWriter.WriteArrayHeader(output, 3);
            RespWriter.WriteInteger(output, range.Start);
            RespWriter.WriteInteger(output, range.End);
            // The serving node: host, port, node id, empty metadata array.
            RespWriter.WriteArrayHeader(output, 4);
            RespWriter.WriteBulkString(output, Encoding.ASCII.GetBytes(range.Host));
            RespWriter.WriteInteger(output, range.Port);
            RespWriter.WriteBulkString(output, Encoding.ASCII.GetBytes(ClusterNodeId(range)));
            RespWriter.WriteArrayHeader(output, 0);
        }
    }

    private void ClusterNodes(IBufferWriter<byte> output)
    {
        var lines = new List<string>();
        foreach (var endpoint in _clusterMap.GroupBy(r => (r.Host, r.Port)))
        {
            var sample = endpoint.First();
            var ranges = string.Join(" ", endpoint.Select(r => $"{r.Start}-{r.End}"));
            var flags = sample.Port == _port ? "myself,master" : "master";
            lines.Add($"{ClusterNodeId(sample)} {sample.Host}:{sample.Port}@{sample.Port + 10000} " +
                      $"{flags} - 0 0 0 connected {ranges}");
        }
        RespWriter.WriteBulkString(output, Encoding.ASCII.GetBytes(string.Join("\n", lines) + "\n"));
    }
}
