#include "ObjectStorageVFSGCThread.h"
#include "Common/ZooKeeper/ZooKeeperLock.h"
#include "Compression/CompressedReadBuffer.h"
#include "Compression/CompressedWriteBuffer.h"
#include "DiskObjectStorageVFS.h"
#include "IO/ReadBufferFromEmptyFile.h"
#include "Interpreters/Context.h"

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

ObjectStorageVFSGCThread::ObjectStorageVFSGCThread(DiskObjectStorageVFS & storage_, ContextPtr context)
    : storage(storage_)
    , log(&Poco::Logger::get(fmt::format("VFSGC({})", storage_.getName())))
    , zookeeper_lock(storage.zookeeper, storage.traits.locks_node, "gc_lock")
{
    task = context->getSchedulePool().createTask(
        log->name(),
        [this]
        {
            try
            {
                run();
            }
            catch (...)
            {
                LOG_DEBUG(log, "Task threw an exception, rescheduling");
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
            }
        });

    task->activateAndSchedule();
}

ObjectStorageVFSGCThread::~ObjectStorageVFSGCThread() = default;

void ObjectStorageVFSGCThread::run()
{
    SCOPE_EXIT({
        zookeeper_lock.unlock();
        task->scheduleAfter(storage.gc_thread_sleep_ms);
    });

    if (!zookeeper_lock.tryLock())
    {
        LOG_DEBUG(log, "Failed to acquire lock, sleeping");
        return;
    }

    Strings log_items_batch = storage.zookeeper->getChildren(storage.traits.log_base_node);
    constexpr size_t batch_min_size = 1; // TODO myrrc should be a setting
    // TODO myrrc should (possibly?) be a setting. The batch size must not be too large as we put it in memory
    constexpr size_t batch_max_size = 5000;
    if (log_items_batch.size() < batch_min_size)
        return;

    // TODO myrrc ZK should return children in lexicographical order. If it were true, we could
    // get minmax by (begin(), rbegin()), but it's not the case so we have to traverse all range
    const auto [start_str, end_str] = std::ranges::minmax(log_items_batch);
    const size_t start_logpointer = parseFromString<size_t>(start_str.substr(4)); // log- is a prefix
    const size_t end_logpointer = std::min(parseFromString<size_t>(end_str.substr(4)), start_logpointer + batch_max_size);
    log_items_batch = {};

    LOG_DEBUG(log, "Acquired lock for log range [{};{}]", start_logpointer, end_logpointer);
    updateSnapshotWithLogEntries(start_logpointer, end_logpointer);
    removeBatch(start_logpointer, end_logpointer);
    LOG_DEBUG(log, "Removed lock for log range [{};{}]", start_logpointer, end_logpointer);
}

void ObjectStorageVFSGCThread::updateSnapshotWithLogEntries(size_t start_logpointer, size_t end_logpointer)
{
    const bool should_have_previous_snapshot = start_logpointer > 0;

    const StoredObject old_snapshot = getSnapshotObject(start_logpointer - 1);
    auto old_snapshot_uncompressed_buf = should_have_previous_snapshot
        ? storage.object_storage->readObject(old_snapshot)
        : std::unique_ptr<ReadBufferFromFileBase>(std::make_unique<ReadBufferFromEmptyFile>());
    CompressedReadBuffer old_snapshot_buf{*old_snapshot_uncompressed_buf};

    const StoredObject new_snapshot = getSnapshotObject(end_logpointer);
    auto new_snapshot_uncompressed_buf = storage.object_storage->writeObject(new_snapshot, WriteMode::Rewrite);
    CompressedWriteBuffer new_snapshot_buf{*new_snapshot_uncompressed_buf};

    auto [obsolete, invalid] = getBatch(start_logpointer, end_logpointer).mergeWithSnapshot(old_snapshot_buf, new_snapshot_buf, log);
    if (should_have_previous_snapshot)
        obsolete.emplace_back(old_snapshot);

    if (!invalid.empty())
    {
        String out;
        for (const auto & [path, ref] : invalid)
            fmt::format_to(std::back_inserter(out), "{} {}\n", path, ref);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid objects:\n{}", out);
    }

    new_snapshot_buf.finalize(); // We need to write new snapshot to s3 before removing old one
    new_snapshot_uncompressed_buf->finalize();

    storage.object_storage->removeObjects(obsolete);
}

VFSLogItem ObjectStorageVFSGCThread::getBatch(size_t start_logpointer, size_t end_logpointer) const
{
    const size_t log_batch_length = end_logpointer - start_logpointer + 1;

    Strings nodes(log_batch_length);
    for (size_t i = 0; i < log_batch_length; ++i)
        nodes[i] = getNode(start_logpointer + i);
    auto responses = storage.zookeeper->get(nodes);
    nodes = {};

    VFSLogItem out;
    for (size_t i = 0; i < log_batch_length; ++i)
    {
        auto item = VFSLogItem::parse(responses[i].data);
        LOG_TRACE(log, "Log item {}", item);
        out.merge(std::move(item));
    }
    LOG_TRACE(log, "Merged batch:\n{}", out);

    return out;
}

void ObjectStorageVFSGCThread::removeBatch(size_t start_logpointer, size_t end_logpointer)
{
    LOG_DEBUG(log, "Removing log range [{};{}]", start_logpointer, end_logpointer);

    const size_t log_batch_length = end_logpointer - start_logpointer + 1;
    Coordination::Requests requests(log_batch_length);
    for (size_t i = 0; i < log_batch_length; ++i)
        requests[i] = zkutil::makeRemoveRequest(getNode(start_logpointer + i), -1);
    storage.zookeeper->multi(requests);
}

String ObjectStorageVFSGCThread::getNode(size_t id) const
{
    // Zookeeper's sequential node is 10 digits with padding zeros
    return fmt::format("{}{:010}", storage.traits.log_item, id);
}

StoredObject ObjectStorageVFSGCThread::getSnapshotObject(size_t logpointer) const
{
    /// TODO myrrc this works only for S3ObjectStorage. Must also recheck encrypted disk replication
    return StoredObject{ObjectStorageKey::createAsRelative(storage.object_key_prefix, fmt::format("vfs/_{}", logpointer)).serialize()};
}
}
