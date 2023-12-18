#pragma once
#include "Poco/Logger.h"
#include "Common/ZooKeeper/ZooKeeperLock.h"
#include "Core/BackgroundSchedulePool.h"
#include "VFSLogItem.h"
#include "base/types.h"

namespace DB
{
class DiskObjectStorageVFS;

// Despite the name, this thread handles not only garbage collection but also snapshot making and
// uploading it to corresponding object storage
// TODO myrrc we should think about dropping the snapshot for log (at some point we want to remove
// even the latest snapshot if we e.g. clean the bucket)
class ObjectStorageVFSGCThread
{
public:
    ObjectStorageVFSGCThread(DiskObjectStorageVFS & storage_, ContextPtr context);
    ~ObjectStorageVFSGCThread();

    inline void stop() { task->deactivate(); }

private:
    DiskObjectStorageVFS & storage;
    Poco::Logger * const log;
    BackgroundSchedulePool::TaskHolder task;
    zkutil::ZooKeeperLock zookeeper_lock;

    void run();

    // @returns new snapshot remote path
    String updateSnapshotWithLogEntries(size_t start_logpointer, size_t end_logpointer);

    VFSLogItem getBatch(size_t start_logpointer, size_t end_logpointer) const;

    /// @returns Obsolete objects
    StoredObjects mergeSnapshotWithLogBatch(ReadBuffer & snapshot, VFSLogItem && batch, WriteBuffer & new_snapshot);

    void onBatchProcessed(size_t start_logpointer, size_t end_logpointer, const String & snapshot_remote_path);

    String getNode(size_t id) const;
};
}
