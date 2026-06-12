#include <Coordination/Storage/Node.h>

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <base/unaligned.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CORRUPTED_DATA;
}

namespace Coordination::Storage
{

using DB::Exception;
namespace ErrorCodes = DB::ErrorCodes;

namespace
{
    /// Block header:
    ///    u32 header_size (includes this field itself)
    ///    u32 base_depth
    ///    u32 base_path_len
    ///    u64 base_zxid
    ///    u64 base_time
    ///    base_path (base_path_len bytes)
    constexpr size_t BLOCK_HEADER_SCALARS_SIZE = sizeof(uint32_t) * 3 + sizeof(uint64_t) * 2;
}

std::optional<NodeAction> combineActions(std::optional<NodeAction> first, NodeAction second, bool strict)
{
    if (!strict || !first.has_value())
        return second;

    if (first == NodeAction::Create && second == NodeAction::Update)
        return NodeAction::Create;
    if (first == NodeAction::Create && second == NodeAction::Remove)
        return std::nullopt;
    if (first == NodeAction::Update && second == NodeAction::Update)
        return NodeAction::Update;
    if (first == NodeAction::Update && second == NodeAction::Remove)
        return NodeAction::Remove;
    if (first == NodeAction::Remove && second == NodeAction::Create)
        return NodeAction::Update;

    throw Exception(
        ErrorCodes::LOGICAL_ERROR, "Can't combine NodeAction {} followed by {}",
        uint32_t(*first), uint32_t(second));
}

NodePathHash NodePath::calculateHash() const
{
    return sipHash128(ptr, len);
}

NodePathWithHash NodePath::withCalculatedHash() const
{
    return NodePathWithHash{.path = *this, .hash = calculateHash()};
}

NodePath NodePath::parentPath() const
{
    chassert(depth != 0);
    return NodePath(Coordination::parentNodePath(str()), depth - 1);
}

std::string_view NodePath::baseName() const
{
    chassert(depth != 0);
    return Coordination::getBaseNodeName(str());
}

void NodeBasicInfo::setNumChildrenAndIsEphemeral(uint32_t num_children, bool is_ephemeral)
{
    /// The low bit stores is_ephemeral, so num_children must fit in the remaining 31 bits.
    if (num_children > (std::numeric_limits<uint32_t>::max() >> 1))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Too many children in a znode: {}", num_children);
    num_children_and_is_ephemeral = (num_children << 1) | uint32_t(is_ephemeral);
}

uint64_t FullNode::getOrCalculateDigest()
{
    if (basic.digest != 0)
        return basic.digest;

    /// Must match calculateDigest in KeeperStorage.cpp (KEEPER_CURRENT_DIGEST_VERSION).
    SipHash hash;

    hash.update(path.str());
    if (!data.empty())
        hash.update(data);

    hash.update(stats.czxid);
    hash.update(stats.mzxid);
    hash.update(stats.ctime);
    hash.update(stats.mtime);
    hash.update(static_cast<int32_t>(basic.version));
    hash.update(stats.cversion);
    hash.update(stats.aversion);
    hash.update(stats.getEphemeralOwner(basic.isEphemeral()));
    hash.update(static_cast<int32_t>(basic.getNumChildren()));
    hash.update(stats.pzxid);

    basic.digest = hash.get64();

    /// 0 means no calculated digest, it's not a valid digest value.
    if (basic.digest == 0)
        basic.digest = 1;

    return basic.digest;
}

NodePathHash FullNode::getOrCalculatePathHash()
{
    if (path_hash == 0)
        path_hash = path.calculateHash();
    return path_hash;
}

void destroyBlockData(BlockData * ptr) noexcept
{
    ptr->~BlockData();
    ::operator delete(static_cast<void *>(ptr));
}

BlockPtr BlockData::create(size_t capacity_)
{
    if (capacity_ > std::numeric_limits<uint32_t>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BlockData capacity {} is too large", capacity_);

    /// Allocate the BlockData together with its trailing data buffer, so that reads have one fewer
    /// pointer to chase. The buffer isn't allowed to grow anyway because NodeRef-s must stay valid.
    /// (The control block is allocated separately so that a lingering BlockWeakPtr keeps only the
    /// small control block alive, not the whole BlockData+buffer.)
    void * raw = ::operator new(sizeof(BlockData) + capacity_);
    BlockData * block = new (raw) BlockData();
    block->capacity = static_cast<uint32_t>(capacity_);

    try
    {
        auto * control = new BlockPtrControlBlock{.ptr = block}; // strong = 1, weak = 1
        return BlockPtr(block, control); // adopt the initial strong ref
    }
    catch (...)
    {
        destroyBlockData(block);
        throw;
    }
}

BlockPtr BlockData::copyAndShrinkToFit() const
{
    BlockPtr new_block = create(size);
    uint32_t cap = new_block->capacity;
    memcpy(new_block->data(), data(), size);
    *new_block = *this; // copy all the fields
    new_block->capacity = cap;
    return new_block;
}

void BlockData::readHeader()
{
    /// serialization_version is stored in the file header, not per block, and assigned by the caller.
    if (serialization_version == 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Keeper block has invalid serialization version 0");

    if (size < BLOCK_HEADER_SCALARS_SIZE)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Keeper block is too small to contain a header");

    const char * p = data();
    uint32_t header_size = 0;
    memcpy(&header_size, p, sizeof(header_size));
    p += sizeof(header_size);
    memcpy(&base_depth, p, sizeof(base_depth));
    p += sizeof(base_depth);
    memcpy(&base_path_len, p, sizeof(base_path_len));
    p += sizeof(base_path_len);
    memcpy(&base_zxid, p, sizeof(base_zxid));
    p += sizeof(base_zxid);
    memcpy(&base_time, p, sizeof(base_time));
    p += sizeof(base_time);

    if (size_t(header_size) < BLOCK_HEADER_SCALARS_SIZE + size_t(base_path_len))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "header_size too small");

    base_path_offset = BLOCK_HEADER_SCALARS_SIZE;
    entries_start = header_size;

    if (entries_start > size)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Keeper block header extends past the block end");
}

void BlockData::writeHeader(BlockPtr & block, const FullNode & node)
{
    chassert(block->size == 0);
    chassert(block->serialization_version <= SERIALIZATION_VERSION_LATEST);
    if (block->serialization_version == 0)
        block->serialization_version = SERIALIZATION_VERSION_LATEST;

    const uint32_t base_path_len = node.path.len;
    const size_t header_size_64 = BLOCK_HEADER_SCALARS_SIZE + base_path_len;
    if (header_size_64 > UINT32_MAX)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block header size doesn't fit in 32 bits");
    uint32_t header_size = uint32_t(header_size_64);
    reserve(block, header_size_64);

    block->base_depth = node.path.depth;
    block->base_zxid = static_cast<uint64_t>(node.stats.mzxid);
    block->base_time = static_cast<uint64_t>(node.stats.mtime);
    block->base_path_len = base_path_len;
    block->base_path_offset = BLOCK_HEADER_SCALARS_SIZE;
    block->entries_start = header_size;

    char * p = block->data();
    memcpy(p, &header_size, sizeof(header_size));
    p += sizeof(header_size);
    memcpy(p, &block->base_depth, sizeof(block->base_depth));
    p += sizeof(block->base_depth);
    memcpy(p, &base_path_len, sizeof(base_path_len));
    p += sizeof(base_path_len);
    memcpy(p, &block->base_zxid, sizeof(block->base_zxid));
    p += sizeof(block->base_zxid);
    memcpy(p, &block->base_time, sizeof(block->base_time));
    p += sizeof(block->base_time);
    memcpy(p, node.path.ptr, base_path_len);

    block->size = block->entries_start;
}

size_t BlockData::nodeSerializedSizeUpperBound(const FullNode & node) const
{
    /// 2 fixed header bytes + the four group varints at their maximum size + 8 digest bytes.
    /// The path suffix is at most the full path; we don't try to predict prefix compression.
    constexpr size_t constant = 2 + 3 * DB::GroupVarint4x32::MAX_SIZE + DB::GroupVarint8x64::MAX_SIZE + 8;
    return constant + node.path.len + node.data.size();
}

NodeRef BlockData::appendNode(BlockPtr & block, FullNode & node)
{
    if (block->size == 0)
        writeHeader(block, node);

    const size_t upper_bound = block->nodeSerializedSizeUpperBound(node);
    reserve(block, static_cast<size_t>(block->size) + upper_bound);

    return appendNodeNoResize(block, node);
}

NodeRef BlockData::appendNodeNoResize(BlockPtr block, FullNode & node)
{
    /// Serialization format:
    ///    1 byte: NodeAction
    ///    1 byte: varints_size; total size of all the next group varints, up to `path suffix`;
    ///            note that the worst-case total size of these varints is 118 bytes < 256 bytes;
    ///            if bigger than actual varints size, reader ignores the extra bytes; this can be
    ///            used by future versions of the format to add fields that can be ignored by
    ///            older readers
    ///    group varint (4 x u32): path_suffix_size, data_size, acl_id, version
    ///    group varint (4 x u32): path_prefix_size, path_depth_delta, num_children_and_is_ephemeral
    ///    group varint (4 x u32): cversion, aversion
    ///    group varint (8 x u64): czxid_delta, mzxid_delta, pzxid_delta, ctime_delta, mtime_delta,
    ///                            ephemeral_owner_or_seq_num
    ///    path_suffix (path_suffix_size bytes)
    ///    data (data_size bytes)
    ///    8 bytes: digest
    ///
    /// Tombstones (NodeAction::Remove) don't store stats, so the last two group varints are
    /// omitted for them. The digest bytes are kept as padding (see Appendix 1 below).
    ///
    /// The *_delta values are delta+zigzag-encoded relative to a corresponding base_* values in
    /// BlockData.
    /// TODO: Try going overboard and conditionally encoding them relative to each other instead,
    ///       e.g. mtime relative to ctime if version == 0, otherwise relative to base_time.
    ///
    /// The znode path is base_path[:path_prefix_size] + path_suffix.
    ///
    /// It is important that the 8-byte digest comes after all group varints. Explained in Appendix 1 below.
    ///
    /// Path depth is stored explicitly instead of being recalculated from path string. There's no
    /// strong reason for this, it just feels less sketchy when sorting key doesn't rely on
    /// nontrivial calculation and its determinism. We're not planning on changing path string
    /// syntax, but who knows.
    ///
    /// Appendix 1: group varint padding
    /// There's an annoying gotcha: group varint decoder reads (and ignores) up to 8 bytes past
    /// the end. Avoiding this makes it much slower. So we must ensure there are at least
    /// 8 readable bytes of after the last group varint.
    /// Normally this kind of problem would be trivially solved by padding the memory buffer; we'd
    /// read a few bytes into the next znode or into unused buffer space, and that would be fine.
    /// But our situation is different: it's possible that the next znode is being concurrently
    /// serialized by another thread (appending to the block). Then reading+ignoring the first bytes
    /// of the next znode would technically be a data race and UB. It would probably trip TSAN.
    /// So the serialized node must contain at least 8 bytes after the last group varint.
    /// Thankfully we have 8-byte digest to store anyway, so we put it after all the varints.
    /// Make sure to not add more group varints after it, and keep serialized digest at least
    /// 8 bytes (or add other padding)!

    chassert(block->entries_start != 0); // writeHeader must have been called
    const size_t upper_bound = block->nodeSerializedSizeUpperBound(node);
    chassert(static_cast<size_t>(block->size) + upper_bound <= block->capacity);

    const uint32_t node_offset = block->size;
    char * const start = block->data() + node_offset;
    char * p = start;

    /// Fixed-size fields.
    *p = char(uint8_t(node.basic.action));
    ++p;
    char * const varints_size_byte = p++; // filled in at the end

    char * const varints_begin = p;

    /// Path prefix shared with the block's base path.
    const char * const base_path_str = block->data() + block->base_path_offset;
    const uint32_t max_prefix = std::min(block->base_path_len, node.path.len);
    uint32_t path_prefix_size = 0;
    while (size_t(path_prefix_size) + 8 <= size_t(max_prefix) &&
           unalignedLoad<uint64_t>(base_path_str + path_prefix_size) == unalignedLoad<uint64_t>(node.path.ptr + path_prefix_size))
        path_prefix_size += 8;
    while (path_prefix_size < max_prefix && base_path_str[path_prefix_size] == node.path.ptr[path_prefix_size])
        ++path_prefix_size;
    const uint32_t path_suffix_size = node.path.len - path_prefix_size;
    const char * const path_suffix_str = node.path.ptr + path_prefix_size;

    const uint32_t data_size = static_cast<uint32_t>(node.data.size());
    const uint32_t path_depth_delta = encodeZigZagDelta32(block->base_depth, node.path.depth);

    DB::GroupVarint4x32::encode(p, path_suffix_size, data_size, node.basic.acl_id, node.basic.version);
    DB::GroupVarint4x32::encode(p, path_prefix_size, path_depth_delta, node.basic.num_children_and_is_ephemeral, 0);

    uint64_t digest = 0x0000deadbeef0000; // for tombstones the digest bytes are just padding
    if (node.basic.action != NodeAction::Remove)
    {
        DB::GroupVarint4x32::encode(
            p, static_cast<uint32_t>(node.stats.cversion), static_cast<uint32_t>(node.stats.aversion), 0, 0);
        DB::GroupVarint8x64::encode(
            p,
            encodeZigZagDelta64(block->base_zxid, static_cast<uint64_t>(node.stats.czxid)),
            encodeZigZagDelta64(block->base_zxid, static_cast<uint64_t>(node.stats.mzxid)),
            encodeZigZagDelta64(block->base_zxid, static_cast<uint64_t>(node.stats.pzxid)),
            encodeZigZagDelta64(block->base_time, static_cast<uint64_t>(node.stats.ctime)),
            encodeZigZagDelta64(block->base_time, static_cast<uint64_t>(node.stats.mtime)),
            static_cast<uint64_t>(node.stats.ephemeral_owner_or_seq_num),
            0,
            0);

        digest = node.getOrCalculateDigest();
    }

    const size_t varints_size = static_cast<size_t>(p - varints_begin);
    chassert(varints_size <= std::numeric_limits<uint8_t>::max());

    memcpy(p, path_suffix_str, path_suffix_size);
    p += path_suffix_size;
    memcpy(p, node.data.data(), data_size);
    p += data_size;
    memcpy(p, &digest, 8);
    p += 8;

    *varints_size_byte = static_cast<char>(static_cast<uint8_t>(varints_size));

    const size_t serialized_size = static_cast<size_t>(p - start);
    chassert(serialized_size <= upper_bound);
    block->size = static_cast<uint32_t>(node_offset + serialized_size);

    return NodeRef{.action = node.basic.action, .offset = node_offset, .block = std::move(block)};
}

bool BlockData::appendNodeOrStartNewBlock(const BlockPtr & block, FullNode & node, size_t new_block_capacity, BlockPtr & out_new_block, NodeRef & out_node_ref)
{
    if (block)
    {
        size_t bytes_required = block->nodeSerializedSizeUpperBound(node);
        if (block->capacity - block->size >= bytes_required)
        {
            out_node_ref = appendNodeNoResize(block, node);
            return false;
        }
    }

    out_new_block = BlockData::create(new_block_capacity);
    out_node_ref = BlockData::appendNode(out_new_block, node);
    return true;
}

void NodeRef::readFull(FullNode & out_node, std::string & out_path_buf) const
{
    read(out_node.basic, &out_node.path, &out_path_buf, &out_node.stats, &out_node.data);
    out_node.path_hash = 0;
}

void NodeRef::readFullWithPath(FullNode & out_node, const NodePath & path, NodePathHash path_hash) const
{
    read(out_node.basic, nullptr, nullptr, &out_node.stats, &out_node.data);
    out_node.path = path;
    out_node.path_hash = path_hash;
}

}
