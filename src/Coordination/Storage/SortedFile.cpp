#include <Coordination/Storage/SortedFile.h>

#include <Coordination/Storage/BlockCache.h>
#include <Coordination/Storage/Memtable.h>
#include <Coordination/Storage/Node.h>
#include <Coordination/Storage/StorageState.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <Common/Exception.h>
#include <base/defines.h>

#include <algorithm>

#include <atomic>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsUInt64 file_block_size;
}

namespace Coordination::Storage
{

BlockPtr SortedFile::getOrLoadBlock(uint32_t block_idx, BlockCache * block_cache) const
{
    chassert(block_idx < blocks.size());
    const BlockInfo & info = blocks[block_idx];

    if (BlockPtr cached = info.data.load())
        return cached;

    chassert(block_cache); // in memory-only mode load() above succeeds because all blocks are pinned

    BlockPtr block = block_cache->getOrSet(
        BlockCacheKey{.file_id = file_id, .block_idx = block_idx},
        [&] { return loadBlock(block_idx); });

    info.data.store(block);
    return block;
}

BlockPtr SortedFile::getBlockCoveringPath(NodePath path, BlockCache * block_cache) const
{
    /// The last block with min_path <= path; if path falls past its max_path (a gap between blocks,
    /// or past the last block) or before the first block, the path is not in this file.
    auto block_it = std::partition_point(
        blocks.begin(), blocks.end(),
        [&](const BlockInfo & block) { return block.min_path.compare(path) <= 0; });
    if (block_it == blocks.begin())
        return {};
    --block_it;
    if (path.compare(block_it->max_path) > 0)
        return {};
    return getOrLoadBlock(static_cast<uint32_t>(block_it - blocks.begin()), block_cache);
}

void SortedFile::listChildren(NodePath range_start, NodePath range_end, ChildrenSet2 & out, DB::Arena & out_arena, BlockCache * block_cache) const
{
    auto block_it = std::partition_point(
        blocks.begin(), blocks.end(),
        [&](const BlockInfo & block) { return block.max_path.compare(range_start) <= 0; });

    std::string path_buf;
    for (; block_it != blocks.end(); ++block_it)
    {
        if (block_it->min_path.compare(range_end) >= 0)
            return;

        const uint32_t block_idx = static_cast<uint32_t>(block_it - blocks.begin());
        BlockPtr block = getOrLoadBlock(block_idx, block_cache);

        NodeRef ref{.action = NodeAction::Create, .offset = 0, .block = block};
        for (uint32_t offset = block->entries_start; offset < block->size;)
        {
            ref.offset = offset;
            NodeBasicInfo basic;
            NodePath node_path;
            ref.read(basic, &node_path, &path_buf, nullptr, nullptr);
            offset += basic.serialized_size;

            if (node_path.compare(range_start) <= 0)
                continue; /// before the range (range_start is exclusive)
            if (node_path.compare(range_end) >= 0)
                return; /// past the range (range_end is exclusive)

            out.insertCombine(node_path.baseName(), basic.action, out_arena, /*strict=*/ true);
        }
    }
}

void SortedFile::removeFromBlockCache(BlockCache * block_cache) const
{
    if (!block_cache)
        /// Memory-only mode: blocks are pinned, not in any cache.
        return;
    for (uint32_t block_idx = 0; block_idx < blocks.size(); ++block_idx)
        block_cache->remove(BlockCacheKey{.file_id = file_id, .block_idx = block_idx});
}

uint32_t SortedFile::generateFileId()
{
    static std::atomic<uint32_t> next_file_id{1};
    return next_file_id.fetch_add(1, std::memory_order_relaxed);
}

BlockPtr SortedFile::loadBlock(uint32_t) const
{
    /// TODO: Come up with file format and implement. Remember to assign block's compatible_digest = (digest_version == KEEPER_CURRENT_DIGEST_VERSION).
    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Reading blocks from Keeper storage files is not implemented yet");
}

}
