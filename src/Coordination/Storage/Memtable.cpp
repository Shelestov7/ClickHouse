#include <Coordination/Storage/Memtable.h>

#include <Coordination/Storage/Node.h>

namespace Coordination::Storage
{

void ChildrenSet2::insertCombine(std::string_view name, NodeAction action, DB::Arena & arena, bool strict)
{
    auto it = set.find(name);
    if (it != set.end())
    {
        std::optional<NodeAction> combined = combineActions(it->action, action, strict);
        if (!combined)
        {
            /// Create + Remove: as if the child never existed.
            set.erase(it);
            return;
        }
        if (*combined == it->action)
            return;
        /// Hash set elements are immutable; erase and reinsert with the new action, reusing the
        /// name that's already in the arena.
        Entry entry = *it;
        entry.action = *combined;
        set.erase(it);
        set.insert(entry);
        return;
    }

    Entry entry;
    entry.ptr = arena.insert(name.data(), name.size());
    entry.len = static_cast<uint32_t>(name.size());
    entry.action = action;
    set.insert(entry);
}

void MemtableChildrenSet::insertCombine(std::string_view name, NodeAction action, DB::Arena & arena, bool strict)
{
    switch (getMode())
    {
        case Mode::Empty:
        {
            ChildrenSet2::Entry entry;
            entry.ptr = arena.insert(name.data(), name.size());
            entry.len = static_cast<uint32_t>(name.size());
            entry.action = action;
            setInlineEntry(entry);
            break;
        }
        case Mode::Inline:
        {
            ChildrenSet2::Entry entry = getInlineEntry();
            if (entry.str() == name)
            {
                std::optional<NodeAction> combined = combineActions(entry.action, action, strict);
                if (!combined)
                {
                    /// Create + Remove: as if the child never existed.
                    mode = Mode::Empty;
                }
                else if (*combined != entry.action)
                {
                    entry.action = *combined;
                    setInlineEntry(entry);
                }
                break;
            }

            auto new_set = std::make_unique<ChildrenSet2>();
            new_set->set.insert(entry); // reuse the name that's already in the arena
            new_set->insertCombine(name, action, arena, strict);
            setSet(new_set.release());
            break;
        }
        case Mode::Set:
            getSet()->insertCombine(name, action, arena, strict);
            break;
    }
}

void MemtableChildrenSet::mergeInto(ChildrenSet2 & out, DB::Arena & arena, bool strict) const
{
    switch (getMode())
    {
        case Mode::Empty:
            break;
        case Mode::Inline:
        {
            ChildrenSet2::Entry entry = getInlineEntry();
            out.insertCombine(entry.str(), entry.action, arena, strict);
            break;
        }
        case Mode::Set:
            for (const ChildrenSet2::Entry & entry : getSet()->set)
                out.insertCombine(entry.str(), entry.action, arena, strict);
            break;
    }
}

NodeRef Memtable::appendNode(FullNode & node, bool strict)
{
    /// Update the parent's children set. (The root has no parent; Update doesn't change children.)
    if (node.basic.action != NodeAction::Update && node.path.depth != 0)
        children[node.path.parentPath().calculateHash()].insertCombine(
            node.path.baseName(), node.basic.action, arena, strict);

    node_count_delta += nodeCountDelta(node.basic.action);

    BlockPtr new_block;
    NodeRef ref;
    if (BlockData::appendNodeOrStartNewBlock(blocks.empty() ? nullptr : blocks.back(), node, target_block_size, new_block, ref))
    {
        total_bytes += new_block->capacity;
        blocks.push_back(std::move(new_block));
    }
    return ref;
}

void Memtable::listChildren(const NodePathWithHash & path, bool strict, ChildrenSet2 & out, DB::Arena & out_arena) const
{
    if (const auto * lookup = children.find(path.hash))
        lookup->getMapped().mergeInto(out, out_arena, strict);
}

MemtablePtr Memtable::takeSnapshot() const
{
    MemtablePtr res = std::make_shared<Memtable>();
    res->file_seqno = file_seqno;
    res->total_bytes = total_bytes;
    res->node_count_delta = node_count_delta;
    res->blocks = blocks;

    /// Last block may still be appended to, make an immutable copy.
    if (!res->blocks.empty())
        res->blocks.back() = res->blocks.back()->copyAndShrinkToFit();

    return res;
}

}
