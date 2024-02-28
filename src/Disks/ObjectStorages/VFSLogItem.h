#pragma once
#include "Disks/ObjectStorages/StoredObject.h"
#include "boost/container/flat_map.hpp"

namespace Poco
{
class Logger;
}

namespace DB
{
class ReadBuffer;
class WriteBuffer;
using VFSLogItemStorage = boost::container::flat_map<String /* remote_path */, int /*references delta */>;

struct VFSMergeResult
{
    StoredObjects obsolete;
    VFSLogItemStorage invalid;
};

struct VFSLogItem : VFSLogItemStorage
{
    static VFSLogItem parse(std::string_view str);

    String serialize() const;
    // Faster alternative that avoids constructing VFSLogItem object
    static String getSerialised(StoredObjects && link, StoredObjects && unlink);

    void merge(VFSLogItem && other);
    void merge(StoredObjects && link, StoredObjects && unlink);
    VFSMergeResult mergeWithSnapshot(ReadBuffer & snapshot, WriteBuffer & new_snapshot, Poco::Logger * log) &&;
};
}

template <>
struct fmt::formatter<DB::VFSLogItem>
{
    constexpr auto parse(auto & ctx) { return ctx.begin(); }
    constexpr auto format(const DB::VFSLogItem & item, auto & ctx)
    {
        fmt::format_to(ctx.out(), "VFSLogItem(\n");
        for (const auto & [path, links] : item)
            fmt::format_to(ctx.out(), "{} {}\n", path, links);
        return fmt::format_to(ctx.out(), ")");
    }
};
