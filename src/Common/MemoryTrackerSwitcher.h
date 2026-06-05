#pragma once

#include <Common/MemoryTracker.h>

namespace DB
{

struct MemoryTrackerSwitcher
{
    explicit MemoryTrackerSwitcher(MemoryTracker * new_tracker);
    ~MemoryTrackerSwitcher();

private:
    MemoryTracker * prev_memory_tracker_parent = nullptr;
};

}
