#pragma once

#include <Coordination/Storage/BlockCache.h>
#include <Coordination/Storage/NodeRefCache.h>
#include <Coordination/Storage/SortedFile.h>
#include <Coordination/Storage/SortedRun.h>
#include <Coordination/Storage/Memtable.h>
#include <Common/SharedMutex.h>

/// White-box unit tests that inspect StorageState internals (befriended inside StorageState).
class KeeperStorage_BackgroundFlushAndMerge_Test;
class KeeperStorage_SnapshotWriter_Test;
class KeeperStorage_WriteThrottling_Test;

namespace Coordination::Storage
{

struct BackgroundWork;

/// Owns and manages all the node storage things: committed and uncommitted state, background
/// flushes and merges.
struct StorageState
{
public:
    /// Protects uncommitted state (`uncommitted`).
    /// Lock order: storage_mutex can be locked while holding uncommitted_mutex, not the other way around.
    std::mutex uncommitted_mutex;

private:
    /// Internal collaborators that operate directly on this storage's committed state.
    friend struct BackgroundWork;
    friend struct SortedRunWriter;
    friend struct SnapshotWriterNodeStream;
    /// White-box unit tests.
    friend class ::KeeperStorage_BackgroundFlushAndMerge_Test;
    friend class ::KeeperStorage_SnapshotWriter_Test;
    friend class ::KeeperStorage_WriteThrottling_Test;

    struct UncommittedMemtable
    {
        MemtablePtr memtable;
        NodeHashMap<NodeRef> nodes;
        /// Zxid of the latest appended node. When commit point reaches this zxid, this memtable can
        /// be deleted. May be overestimated if request with this zxid was rolled back; that's ok.
        int64_t max_zxid = 0;
    };

    DB::KeeperContextPtr keeper_context;
    LoggerPtr log;

    /// If true, don't do file IO and keep SortedFile blocks pinned in memory.
    bool memory_only = true;

    /// In memory-only mode stays null (blocks are owned by SortedFile-s instead).
    std::unique_ptr<BlockCache> block_cache;

    /// Protects committed state (`files`, `{mutable,immutable}_memtables`, `node_cache`, etc).
    DB::SharedMutex * storage_mutex = nullptr;

    /// Files and memtables containing committed nodes, listed in chronological order.
    /// All memtables come after all files.
    /// In this order, each path's sequence of NodeAction-s forms a valid history,
    /// e.g. [Create, Update, Remove, Create], never e.g. [Create, Create] or [Remove, ...].
    ///
    /// E.g. to find a node (without using the `node_cache` hash map), you'd need to search
    /// mutable_memtable, then immutable_memtables in reverse, then sorted_runs in reverse, stopping
    /// when the node (or its NodeAction::Remove tombstone) is found.
    ///
    /// When a merge is in progress, this may contain both a partial merge output (prefix) and
    /// truncated merge inputs (suffixes), with overlapping file_seqno ranges but nonoverlapping
    /// NodePath ranges. The merge is careful to keep all such published intermediate results
    /// consistent, such that the above paragraphs are always true, so readers don't need to think
    /// about merges.
    std::vector<SortedRunPtr> sorted_runs;
    std::vector<MemtablePtr> immutable_memtables;
    MemtablePtr mutable_memtable; // may be nullptr

    /// TODO: On startup, initialize to (maximum max_file_seqno over preexisting files) + 1.
    uint32_t next_file_seqno = 1;

    /// Latest occurrence of each node in files and memtables. Doesn't contain removed nodes.
    NodeRefCache node_cache;

    /// Uncommitted state, as an overlay on top of committed state.
    /// Contains all uncommitted changes and some recently committed changes (i.e. overlaps committed state).
    /// To find a node, search in these UncommittedMemtable-s in reverse, then in committed state
    /// if not found.
    /// Similarly to regular memtables, we create a new one when the latest one gets big enough.
    /// But these memtables are never flushed to files; instead, a memtable is simply deleted when
    /// its max_zxid gets committed. This vector usually has two elements.
    std::vector<UncommittedMemtable> uncommitted;

    std::unique_ptr<BackgroundWork> background;

    /// The bigger the number the more we should slow down writes. 0 means no throttling.
    std::atomic<size_t> write_throttling{};

public:
    explicit StorageState(DB::KeeperContextPtr keeper_context_, DB::SharedMutex * storage_mutex_);
    ~StorageState(); // calls shutdown()

    /// Start and stop background threads.
    void startup();
    void shutdown();

    /// === Operations on committed state. ===
    /// Caller must hold storage_mutex in shared mode (for const methods) or exclusive mode (for non-const).

    /// Node lookup in committed state.
    ///
    /// Returns NodeRef with action == Remove and block == nullptr if the node doesn't exist.
    /// If the node's block was evicted from the block cache, reloads it (through `block_cache`)
    /// and re-points the affected `node_cache` entries at the newly loaded block.
    NodeRef getCommittedNode(const NodePathWithHash & path) const;

    void appendCommittedNode(FullNode & node);

    /// Merged children set of the node, from all files and memtables. Children names are copied
    /// into `out_arena`. The caller may assert that all returned entries have action == Create.
    ChildrenSet2 listCommittedChildren(const NodePathWithHash & path, DB::Arena & out_arena) const;

    /// === Operations on uncommitted state. ===
    /// Caller must hold uncommitted_mutex and must *not* hold storage_mutex (these methods may lock+unlock it).

    /// Node lookup in committed+uncommitted state.
    /// Locks storage_mutex (shared) for committed state lookup if the node is not found in
    /// uncommitted state.
    NodeRef getUncommittedNode(const NodePathWithHash & path);

    void appendUncommittedNode(FullNode & node, int64_t zxid);

    /// Call periodically to remove obsolete UncommittedMemtable-s.
    void cleanupUncommittedState(int64_t committed_zxid);

    /// The caller should ignore entries with action == Remove.
    ChildrenSet2 listUncommittedChildren(const NodePathWithHash & path, DB::Arena & out_arena);

    /// Sleeps for a short time if background work fell behind.
    /// Call before each write, with storage_mutex unlocked.
    void throttleWrite() const;

private:
    /// Call when memtables or sorted runs were added or removed, with storage_mutex held.
    void recalculateWriteThrottling();

    /// TODO: list*Children that outputs NodeRef-s in addition to names, useful for
    ///       filtered/extended List requests and RemoveRecursive requests.

    /// TODO: listRecursive of some kind. It can be much faster than normal tree traversal because
    ///       SortedFile can just do one range scan per depth. But it seems very tricky because
    ///       memtable can only list children of one node at a time, and also uncommitted state may
    ///       remove children (which may've been listed as part of SortedFile scan).
};

}
