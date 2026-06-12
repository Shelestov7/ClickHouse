#include <Coordination/Storage/StorageState.h>

#include <Coordination/Storage/BackgroundWork.h>
#include <Coordination/Storage/Node.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <mutex>
#include <shared_mutex>
#include <thread>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsUInt64 committed_memtable_size;
    extern const CoordinationSettingsUInt64 memtable_block_size;
    extern const CoordinationSettingsUInt64 uncommitted_memtable_size;
    extern const CoordinationSettingsUInt64 unflushed_memtables_soft_limit;
    extern const CoordinationSettingsUInt64 sorted_runs_soft_limit;
    extern const CoordinationSettingsUInt64 write_throttling_min_delay_ms;
    extern const CoordinationSettingsUInt64 write_throttling_max_delay_ms;
    extern const CoordinationSettingsFloat write_throttling_factor;
}

namespace Coordination::Storage
{

StorageState::StorageState(DB::KeeperContextPtr keeper_context_, DB::SharedMutex * storage_mutex_)
    : keeper_context(std::move(keeper_context_)), log(getLogger("KeeperLSMT")), storage_mutex(storage_mutex_)
{
    /// TODO: Init memory_only. Init block_cache if not memory_only.
}

StorageState::~StorageState()
{
    shutdown();
}

void StorageState::startup()
{
    chassert(!background);
    background = std::make_unique<BackgroundWork>(this);
}

void StorageState::shutdown()
{
    if (background)
    {
        background->shutdown();
        background.reset();
    }
}

NodeRef StorageState::getCommittedNode(const NodePathWithHash & path) const
{
    chassert(!storage_mutex->try_lock());

    const auto * lookup = node_cache.map.find(path.hash);
    if (!lookup)
        return NodeRef{}; // the node doesn't exist
    const NodeRefCache::Entry & info = lookup->getMapped();

    {
        std::lock_guard guard(info.block);

        if (BlockPtr block = info.block.get())
            /// Normal fast path: the node is already in memory.
            return NodeRef{.action = NodeAction::Create, .offset = info.node_offset, .block = std::move(block)};
    }

    /// The block was evicted from the block cache. Memtables keep their blocks alive, so the
    /// node's latest update must be in a file (sorted run).
    ///
    /// We can't binary-search the run by file_seqno: while a merge incrementally publishes its
    /// output run alongside the not-yet-consumed suffixes of its input runs, several runs cover
    /// overlapping seqno ranges. So scan runs newest-first; among the runs whose seqno range covers
    /// our seqno, exactly one actually has the path (its cutoff lets the others reject it).
    const uint32_t seqno = info.file_seqno;
    BlockPtr block;
    const SortedRun * found_run = nullptr;
    for (auto it = sorted_runs.rbegin(); it != sorted_runs.rend(); ++it)
    {
        const SortedRun & run = **it;
        if (seqno < run.min_file_seqno || seqno > run.max_file_seqno)
            continue;
        block = run.getBlockCoveringPath(path.path, block_cache.get());
        if (block)
        {
            found_run = &run;
            break;
        }
    }
    if (!block)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "Node's block expired, but no sorted run covering file_seqno {} has its path", seqno);
    const SortedRun & run = *found_run;

    /// Re-point `node_cache` entries at the freshly loaded copy of the block, for all nodes
    /// in it. We may be holding storage_mutex in shared mode, so concurrent readers may be doing
    /// the same; that's fine: we only update existing entries (no map rehash), one entry at a time
    /// under its spinlock.
    NodeRef ref{.action = NodeAction::Create, .offset = 0, .block = block};
    std::string path_buf;
    for (uint32_t offset = block->entries_start; offset < block->size;)
    {
        ref.offset = offset;
        NodeBasicInfo basic;
        NodePath node_path;
        ref.read(basic, &node_path, &path_buf, nullptr, nullptr);

        if (const auto * node_lookup = node_cache.map.find(node_path.calculateHash()))
        {
            const NodeRefCache::Entry & node_info = node_lookup->getMapped();
            /// Don't touch entries whose latest update is in a newer sorted run or memtable.
            if (run.max_file_seqno >= node_info.file_seqno)
            {
                chassert(run.min_file_seqno <= node_info.file_seqno);
                std::lock_guard guard(node_info.block);
                node_info.block.set(block);
                node_info.node_offset = offset;
            }
        }

        offset += basic.serialized_size;
    }

    /// `info` is still valid: the loop above only updated existing `node_cache` entries.
    std::lock_guard guard(info.block);
    BlockPtr loaded = info.block.get();
    if (!loaded)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "Node (file_seqno {}) not found in the expected block of sorted run covering {}-{}",
            seqno, run.min_file_seqno, run.max_file_seqno);
    return NodeRef{.action = NodeAction::Create, .offset = info.node_offset, .block = std::move(loaded)};
}

NodeRef StorageState::getUncommittedNode(const NodePathWithHash & path)
{
    chassert(!uncommitted_mutex.try_lock());

    /// Search uncommitted memtables, newest first. The found NodeRef may be a tombstone
    /// (action == Remove) with a non-null block.
    for (auto it = uncommitted.rbegin(); it != uncommitted.rend(); ++it)
        if (const auto * lookup = it->nodes.find(path.hash))
            return lookup->getMapped();

    std::shared_lock lock(*storage_mutex);
    return getCommittedNode(path);
}

void StorageState::appendCommittedNode(FullNode & node)
{
    chassert(!storage_mutex->try_lock_shared());

    const DB::CoordinationSettings & settings = keeper_context->getCoordinationSettings();

    if (!mutable_memtable ||
        /// (Quirk: this condition will usually pass just after allocating a new block in the memtable.
        ///  So we'll usually finalize the memtable with a nearly empty last block, wasting its capacity.
        ///  That's fine, memtable usually has lots of blocks, this is a tiny waste of memory.)
        mutable_memtable->total_bytes > settings[DB::CoordinationSetting::committed_memtable_size])
    {
        if (mutable_memtable)
        {
            immutable_memtables.push_back(std::move(mutable_memtable));
            recalculateWriteThrottling();
            background->maybeStartFlush();
        }

        mutable_memtable = std::make_shared<Memtable>();
        mutable_memtable->target_block_size = settings[DB::CoordinationSetting::memtable_block_size];
        mutable_memtable->file_seqno = next_file_seqno++;

        LOG_DEBUG(log, "Creating new memtable {}", mutable_memtable->file_seqno);

        /// TODO: Create block_cache (if not memory-only mode) or update its settings if changed.
    }

    NodeRef ref = mutable_memtable->appendNode(node, /*strict=*/ true);

    /// Update `node_cache`. (We hold storage_mutex exclusively, so no concurrent readers;
    /// no need for the per-entry spinlocks.)
    const NodePathHash hash = node.getOrCalculatePathHash();
    if (auto * lookup = node_cache.map.find(hash))
    {
        NodeRefCache::Entry & info = lookup->getMapped();
        /// The node already exists, so its history so far combines to Create.
        /// Combine that with the new action, strictly (e.g. asserts we don't Create it again).
        std::optional<NodeAction> combined = combineActions(NodeAction::Create, node.basic.action, /*strict=*/ true);
        if (!combined)
        {
            /// Create + Remove: `node_cache` doesn't keep removed nodes.
            node_cache.map.erase(hash);
        }
        else
        {
            chassert(*combined == NodeAction::Create); // Create + Update = Create
            info.file_seqno = mutable_memtable->file_seqno;
            info.block.store(ref.block);
            info.node_offset = ref.offset;
        }
    }
    else
    {
        if (node.basic.action != NodeAction::Create)
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR, "Unexpected NodeAction {} for a node that doesn't exist",
                uint32_t(node.basic.action));

        NodeRefCache::Entry & info = node_cache.map[hash];
        info.file_seqno = mutable_memtable->file_seqno;
        info.block.store(ref.block);
        info.node_offset = ref.offset;
    }
}

ChildrenSet2 StorageState::listCommittedChildren(const NodePathWithHash & path, DB::Arena & out_arena) const
{
    chassert(!storage_mutex->try_lock());

    ChildrenSet2 set;

    /// The direct children of `path` are exactly the nodes Q with range_start < Q < range_end (both
    /// bounds exclusive), at depth path.depth + 1, where:
    ///   range_start = path + "/"   (e.g. "/foo/bar/")
    ///   range_end   = range_start with the last char bumped from '/' to '0' ('/'+1)   (e.g. "/foo/bar0")
    /// In the (depth, path string) order this is exactly the depth-(path.depth+1) nodes whose string
    /// starts with the "path + '/'" prefix.
    std::string range_start_str(path.path.str());
    if (!range_start_str.ends_with('/'))
        range_start_str += '/';
    std::string range_end_str = range_start_str;
    ++range_end_str.back(); // '/' (0x2F) -> '0' (0x30)
    const NodePath range_start(range_start_str, path.path.depth + 1);
    const NodePath range_end(range_end_str, path.path.depth + 1);

    /// Oldest to newest, so that strict insertCombine sees each child's history in order.
    for (const SortedRunPtr & run : sorted_runs)
        run->listChildren(range_start, range_end, set, out_arena, block_cache.get());
    for (const MemtablePtr & memtable : immutable_memtables)
        memtable->listChildren(path, /*strict=*/ true, set, out_arena);
    if (mutable_memtable)
        mutable_memtable->listChildren(path, /*strict=*/ true, set, out_arena);

    return set;
}

void StorageState::appendUncommittedNode(FullNode & node, int64_t zxid)
{
    chassert(!uncommitted_mutex.try_lock());

    const DB::CoordinationSettings & settings = keeper_context->getCoordinationSettings();

    if (uncommitted.empty()
        || uncommitted.back().memtable->total_bytes > settings[DB::CoordinationSetting::uncommitted_memtable_size])
    {
        if (!uncommitted.empty())
            LOG_DEBUG(log, "Creating new uncommitted memtable (last memtable max_zxid = {}, current zxid = {})", uncommitted.back().max_zxid, zxid);

        UncommittedMemtable u;
        u.memtable = std::make_shared<Memtable>();
        u.memtable->target_block_size = settings[DB::CoordinationSetting::memtable_block_size];
        uncommitted.push_back(std::move(u));
    }

    UncommittedMemtable & u = uncommitted.back();
    u.max_zxid = std::max(u.max_zxid, zxid);
    /// strict=false: see the comment at Memtable::appendNode.
    NodeRef ref = u.memtable->appendNode(node, /*strict=*/ false);
    /// Loose model: the last record for a path wins, including Remove tombstones.
    u.nodes[node.getOrCalculatePathHash()] = ref;
}

void StorageState::cleanupUncommittedState(int64_t committed_zxid)
{
    chassert(!uncommitted_mutex.try_lock());

    while (!uncommitted.empty() && uncommitted.front().max_zxid <= committed_zxid)
    {
        LOG_DEBUG(log, "Removing obsolete uncommitted memtable with max_zxid = {} (committed_zxid = {})", uncommitted.front().max_zxid, committed_zxid);

        uncommitted.erase(uncommitted.begin());
    }
}

ChildrenSet2 StorageState::listUncommittedChildren(const NodePathWithHash & path, DB::Arena & out_arena)
{
    chassert(!uncommitted_mutex.try_lock());

    ChildrenSet2 set;
    {
        std::shared_lock lock(*storage_mutex);
        set = listCommittedChildren(path, out_arena);
    }

    for (const UncommittedMemtable & u : uncommitted)
        u.memtable->listChildren(path, /*strict=*/ false, set, out_arena);

    return set;
}

void StorageState::throttleWrite() const
{
    size_t amount = write_throttling.load(std::memory_order_relaxed);
    if (amount == 0)
        return;

    const DB::CoordinationSettings & settings = keeper_context->getCoordinationSettings();
    const uint64_t max_delay_ms = settings[DB::CoordinationSetting::write_throttling_max_delay_ms];
    const uint64_t min_delay_ms = settings[DB::CoordinationSetting::write_throttling_min_delay_ms];
    const float factor = settings[DB::CoordinationSetting::write_throttling_factor];

    /// Exponential backoff, clamped to max_delay_ms. If `amount` is very large, pow overflows to
    /// +inf, and min() clamps it back to max_delay_ms, so delay_ms stays finite.
    const double delay_ms = std::min(double(max_delay_ms), double(min_delay_ms) * std::pow(double(factor), double(amount)));
    std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int64_t>(delay_ms)));
}

void StorageState::recalculateWriteThrottling()
{
    chassert(!storage_mutex->try_lock());

    auto excess = [](size_t current, size_t limit) { return current - std::min(current, limit); };

    const DB::CoordinationSettings & settings = keeper_context->getCoordinationSettings();
    size_t flushes_fell_behind = excess(immutable_memtables.size(), settings[DB::CoordinationSetting::unflushed_memtables_soft_limit]);
    size_t merges_fell_behind = excess(sorted_runs.size(), settings[DB::CoordinationSetting::sorted_runs_soft_limit]);

    size_t throttle = flushes_fell_behind + merges_fell_behind;
    size_t prev = write_throttling.exchange(throttle);

    if (throttle != prev)
        LOG_INFO(log, "{} writes, there are {} immutable memtables and {} sorted runs", throttle ? "Throttling" : "Unthrottling", immutable_memtables.size(), sorted_runs.size());
}

}
