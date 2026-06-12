#pragma once

#include <Coordination/Storage/Common.h>
#include <Common/Exception.h>
#include <Common/GroupVarint.h>
#include <IO/VarInt.h>
#include <base/defines.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace Coordination::Storage
{

constexpr uint32_t SERIALIZATION_VERSION_LATEST = 1;

/// Node is split into 4 parts that can be deserialized separately, listed in FullNode.
/// Skipping a part is slightly faster than deserializing it.

struct NodeBasicInfo
{
    NodeAction action = NodeAction::Create;

    /// Size of the whole serialized node. Advance offset by this much to get to the next node.
    uint32_t serialized_size = 0;
    uint32_t data_size = 0;
    uint32_t acl_id = 0;
    uint32_t version = 0;
    uint32_t num_children_and_is_ephemeral = 0;

    uint64_t digest = 0; // 0 if not calculated

    uint32_t getNumChildren() const { return num_children_and_is_ephemeral >> 1; }
    uint32_t isEphemeral() const { return (num_children_and_is_ephemeral & 1) != 0; }

    void setNumChildrenAndIsEphemeral(uint32_t num_children, bool is_ephemeral);

    void invalidateDigest() { digest = 0; }
};

struct NodeStats2
{
    int64_t czxid{0};
    int64_t mzxid{0};
    int64_t pzxid{0};

    int64_t ctime{0};
    int64_t mtime{0};

    int32_t cversion{0};
    int32_t aversion{0};

    int64_t ephemeral_owner_or_seq_num{0};

    int64_t getEphemeralOwner(bool is_ephemeral) const { return is_ephemeral ? ephemeral_owner_or_seq_num : 0; }
    int64_t getSeqNum(bool is_ephemeral) const { return is_ephemeral ? 0 : ephemeral_owner_or_seq_num; }
};

/// All parts of a node grouped in one struct for convenience. Doesn't own path and data.
struct FullNode
{
    NodeBasicInfo basic;
    NodeStats2 stats;
    NodePath path;
    NodePathHash path_hash {}; // 0 if not calculated
    std::string_view data;

    /// Assigns `basic.digest` if it's 0. The digest is compatible with KeeperStorage's
    /// calculateDigest (KEEPER_CURRENT_DIGEST_VERSION).
    uint64_t getOrCalculateDigest();

    /// Assigns `path_hash` if it's 0.
    NodePathHash getOrCalculatePathHash();
};

/// Memory buffer containing a sequence of serialized znodes.
/// Serialization format is described in BlockData::appendNodeNoResize.
/// The same format is used for znode storage in memory and in files.
///
/// This struct is co-located with its data buffer in one allocation.
/// The BlockData+buffer combination is always wrapped in a (custom-refcounted) BlockPtr.
///
/// BlockData supports appending znodes, as long as there's enough space in the buffer.
/// If the buffer needs to grow, a new BlockData is allocated.
/// After a znode is serialized into a BlockData, the corresponding memory subrange is immutable, so
/// any NodeRef can always safely deserialize the znode without any synchronization, even if it
/// points to a BlockData that's still being written to by another thread.
///
/// A serialized znode may represent either a full znode info (NodeAction::Create or Update) or
/// a tombstone (NodeAction::Remove) saying that the znode was removed (by the file or memtable that
/// contains this block).
///
/// Generally the sequence of znodes in blocks is the source of truth, and everything else is index
/// on top of it. E.g. children sets in Memtable, node hash map in StorageState, bloom filters (TODO),
/// file children index (TODO) - these can all be reconstructed from blocks.
struct BlockData
{
    /// The data buffer starts right after the BlockData, i.e. at `this + 1`.
    uint32_t capacity = 0;
    uint32_t size = 0;

    uint32_t serialization_version = 0;
    uint32_t entries_start = 0; // offset in data() where serialized znodes start

    /// Information for group compression of znode fields.
    uint32_t base_depth = 0;
    uint32_t base_path_len = 0;
    uint32_t base_path_offset = 0; // relative to data()
    uint64_t base_zxid = 0;
    uint64_t base_time = 0;

    bool compatible_digest = false;

    static BlockPtr create(size_t capacity_);

    static void reserve(BlockPtr & block, size_t required_capacity)
    {
        if (block->capacity >= required_capacity)
            return;
        BlockPtr new_block = create(std::max(required_capacity, static_cast<size_t>(block->capacity) * 2));
        uint32_t cap = new_block->capacity;
        memcpy(new_block->data(), block->data(), block->size);
        *new_block = *block; // copy all the fields
        new_block->capacity = cap;
        block = std::move(new_block);
    }

    /// The data buffer is in memory immediately after the BlockData struct.
    char * data() { return reinterpret_cast<char *>(this + 1); }
    const char * data() const { return reinterpret_cast<const char *>(this + 1); }

    /// Estimate how many bytes serialized node would take. Never underestimates.
    size_t nodeSerializedSizeUpperBound(const FullNode & node) const;

    /// Initializes the block, taking base values for delta encoding from `node`.
    /// `node` itself is not written.
    static void writeHeader(BlockPtr & block, const FullNode & node);

    /// May reassign `block` if we hit capacity and had to allocate a bigger block.
    /// Automatically calls writeHeader if block is uninitialized.
    static NodeRef appendNode(BlockPtr & block, FullNode & node);

    /// The caller must ensure that there's enough space (nodeSerializedSizeUpperBound) and that
    /// writeHeader was called.
    static NodeRef appendNodeNoResize(BlockPtr block, FullNode & node);

    /// If the node fits in this block's capacity, append it here, and return false.
    /// Otherwise create a new block with capacity at least new_block_capacity, append the node to
    /// it, and return true. `block` may be nullptr, then we always start a new block.
    static bool appendNodeOrStartNewBlock(const BlockPtr & block, FullNode & node, size_t new_block_capacity, BlockPtr & out_new_block, NodeRef & out_node_ref);

    /// Deserializes the block header and assigns fields. Call after writing the block data
    /// (that presumably comes from file) to `data()` and assigning `size` and
    /// `serialization_version` (which is not stored per block, only in file header).
    void readHeader();

    /// Make a copy with capacity = size.
    BlockPtr copyAndShrinkToFit() const;

private:
    /// Constructed only via create()/reserve(), which allocate the object together with its trailing
    /// data buffer. Keep the default constructor private to prevent accidental construction that
    /// would bypass that (and leave data() pointing at unowned memory).
    BlockData() = default;
};

/// Decode/encode a field stored as a zigzag delta relative to a base value. The arithmetic is done
/// on unsigned types so wraparound is well-defined (signed overflow would be UB).
/// (Note: signed and unsigned addition are the same operation. Ditto for subtraction.)
inline uint64_t decodeZigZagDelta64(uint64_t base, uint64_t zigzag_delta)
{
    return base + static_cast<uint64_t>(DB::decodeZigZag(zigzag_delta));
}
inline uint64_t encodeZigZagDelta64(uint64_t base, uint64_t value)
{
    return DB::encodeZigZag(static_cast<int64_t>(value - base));
}
inline uint32_t decodeZigZagDelta32(uint32_t base, uint32_t zigzag_delta)
{
    return base + static_cast<uint32_t>(DB::decodeZigZag32(zigzag_delta));
}
inline uint32_t encodeZigZagDelta32(uint32_t base, uint32_t value)
{
    return DB::encodeZigZag32(static_cast<int32_t>(value - base));
}

inline void NodeRef::read(
    NodeBasicInfo & out_basic, NodePath * out_path, std::string * out_path_buf, NodeStats2 * out_stats,
    std::string_view * out_data) const
{
    /// Serialization format is described in BlockData::appendNodeNoResize.

    const char * p = block->data() + offset;

    uint8_t action_byte = static_cast<uint8_t>(p[0]);
    if (action_byte > static_cast<uint8_t>(NodeAction::Create))
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Invalid NodeAction: {}", action_byte);
    out_basic.action = static_cast<NodeAction>(action_byte);

    const uint32_t varints_size = static_cast<uint8_t>(p[1]);

    const char * varints = p + 2;
    /// `path suffix` immediately follows all the group varints.
    const char * path_suffix = varints + varints_size;

    /// group varint (4 x u32): path_suffix_size, data_size, acl_id, version
    uint32_t path_suffix_size = 0;
    uint32_t data_size = 0;
    DB::GroupVarint4x32::decode(varints, path_suffix_size, data_size, out_basic.acl_id, out_basic.version);

    /// Check against capacity rather than size: capacity is immutable, so this doesn't race with
    /// a thread concurrently appending to the block (reading `size` here would). For blocks read
    /// from files (which may be corrupted) capacity is tight, so the check is just as precise;
    /// for in-memory blocks (well-formed by construction) it's effectively an assert anyway.
    size_t total_size = size_t(2) + varints_size + path_suffix_size + data_size + 8;
    if (total_size > block->capacity - offset)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Node goes out of bounds of its block");

    out_basic.data_size = data_size;
    out_basic.serialized_size = uint32_t(total_size);

    /// group varint (4 x u32): path_prefix_size, path_depth_delta, num_children_and_is_ephemeral, (unused)
    uint32_t path_prefix_size = 0;
    uint32_t path_depth_delta = 0;
    uint32_t unused = 0;
    DB::GroupVarint4x32::decode(varints, path_prefix_size, path_depth_delta, out_basic.num_children_and_is_ephemeral, unused);

    if (out_stats && out_basic.action != NodeAction::Remove)
    {
        /// group varint (4 x u32): cversion, aversion, (unused), (unused)
        uint32_t cversion = 0;
        uint32_t aversion = 0;
        DB::GroupVarint4x32::decode(varints, cversion, aversion, unused, unused);
        out_stats->cversion = static_cast<int32_t>(cversion);
        out_stats->aversion = static_cast<int32_t>(aversion);

        /// group varint (8 x u64): czxid_delta, mzxid_delta, pzxid_delta, ctime_delta, mtime_delta,
        ///                         ephemeral_owner_or_seq_num, (unused), (unused)
        uint64_t czxid_delta = 0;
        uint64_t mzxid_delta = 0;
        uint64_t pzxid_delta = 0;
        uint64_t ctime_delta = 0;
        uint64_t mtime_delta = 0;
        uint64_t ephemeral_owner_or_seq_num = 0;
        uint64_t unused64 = 0;
        DB::GroupVarint8x64::decode(
            varints, czxid_delta, mzxid_delta, pzxid_delta, ctime_delta, mtime_delta, ephemeral_owner_or_seq_num, unused64, unused64);

        out_stats->czxid = static_cast<int64_t>(decodeZigZagDelta64(block->base_zxid, czxid_delta));
        out_stats->mzxid = static_cast<int64_t>(decodeZigZagDelta64(block->base_zxid, mzxid_delta));
        out_stats->pzxid = static_cast<int64_t>(decodeZigZagDelta64(block->base_zxid, pzxid_delta));
        out_stats->ctime = static_cast<int64_t>(decodeZigZagDelta64(block->base_time, ctime_delta));
        out_stats->mtime = static_cast<int64_t>(decodeZigZagDelta64(block->base_time, mtime_delta));
        out_stats->ephemeral_owner_or_seq_num = static_cast<int64_t>(ephemeral_owner_or_seq_num);
    }

    /// We do this range check only after doing the reads, instead of expensively pre-checking
    /// before each varint decode. So when deserializing a corrupted file we may read memory past
    /// end of buffer. This is ok; possible outcomes are:
    ///  * one of the bad reads segfaults,
    ///  * we get here, fail this check, and discard all results of the bad reads.
    /// Both are fine.
    if (varints > path_suffix)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Node has incorrect varints_size");

    if (out_path)
    {
        chassert(out_path_buf);
        /// path = base_path[:path_prefix_size] + path_suffix
        out_path_buf->reserve(path_prefix_size + path_suffix_size);
        out_path_buf->assign(block->data() + block->base_path_offset, path_prefix_size);
        out_path_buf->append(path_suffix, path_suffix_size);
        out_path->depth = decodeZigZagDelta32(block->base_depth, path_depth_delta);
        out_path->len = path_prefix_size + path_suffix_size;
        out_path->ptr = out_path_buf->data();
    }

    if (out_data)
        *out_data = std::string_view(path_suffix + path_suffix_size, data_size);

    memcpy(&out_basic.digest, path_suffix + path_suffix_size + data_size, 8);
    if (!block->compatible_digest)
        out_basic.digest = 0;
}

}
