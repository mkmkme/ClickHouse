#include "ObjectStorageVFSGCThread.h"
#include "Common/ZooKeeper/ZooKeeperLock.h"
#include "Disks/ObjectStorages/DiskObjectStorageVFS.h"
#include "Interpreters/Context.h"

namespace DB
{
ObjectStorageVFSGCThread::ObjectStorageVFSGCThread(DiskObjectStorageVFS & storage_, ContextPtr context)
    : storage(storage_)
    , log_name("ObjectStorageVFS (GC thread)")
    , log(&Poco::Logger::get(log_name))
    , zookeeper_lock(zkutil::createSimpleZooKeeperLock(storage.zookeeper, VFS_BASE_NODE, "lock", ""))
    , sleep_ms(10'000) // TODO myrrc should pick this from settings
{
    task = context->getSchedulePool().createTask(log_name, [this] { run(); });
}

void ObjectStorageVFSGCThread::run()
{
    if (!zookeeper_lock->tryLock())
    {
        LOG_DEBUG(log, "Failed to acquire GC lock, sleeping");
        task->scheduleAfter(sleep_ms);
        return;
    }

    LOG_DEBUG(log, "Acquired GC lock");
    zkutil::ZooKeeper & zookeeper = *storage.zookeeper;

    VFSSnapshot snapshot;
    const auto [start_str, end_str] = std::ranges::minmax(zookeeper.getChildren(VFS_LOG_BASE_NODE));
    const bool res = tryParse(snapshot.end_logpointer, end_str);
    chassert(res);

    if (start_str != "0")
        populateSnapshot(snapshot, fmt::format("vfs_snapshot_{}", start_str));

    // do some stuff

    zookeeper_lock->unlock();
    task->scheduleAfter(sleep_ms);
}
}
