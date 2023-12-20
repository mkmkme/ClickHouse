#include <Storages/MergeTree/MutateFromLogEntryTask.h>

#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <cmath>

namespace ProfileEvents
{
    extern const Event DataAfterMutationDiffersFromReplica;
    extern const Event ReplicatedPartMutations;
}

namespace DB
{

ReplicatedMergeMutateTaskBase::PrepareResult MutateFromLogEntryTask::prepare()
{
    const String & source_part_name = entry.source_parts.at(0);
    const auto storage_settings_ptr = storage.getSettings();
    LOG_TRACE(log, "Executing log entry to mutate part {} to {}", source_part_name, entry.new_part_name);

    MergeTreeData::DataPartPtr source_part = storage.getActiveContainingPart(source_part_name);
    if (!source_part)
    {
        LOG_DEBUG(log, "Source part {} for {} is missing; will try to fetch it instead. "
            "Either pool for fetches is starving, see background_fetches_pool_size, or none of active replicas has it",
            source_part_name, entry.new_part_name);
        return PrepareResult{
            .prepared_successfully = false,
            .need_to_check_missing_part_in_fetch = true,
            .part_log_writer = {}
        };
    }

    if (source_part->name != source_part_name)
    {
        LOG_WARNING(log,
            "Part {} is covered by {} but should be mutated to {}. "
            "Possibly the mutation of this part is not needed and will be skipped. "
            "This shouldn't happen often.",
            source_part_name, source_part->name, entry.new_part_name);

        return PrepareResult{
            .prepared_successfully = false,
            .need_to_check_missing_part_in_fetch = true,
            .part_log_writer = {}
        };
    }

    /// TODO - some better heuristic?
    size_t estimated_space = MergeTreeDataMergerMutator::estimateNeededDiskSpace({source_part});

    if (entry.create_time + storage_settings_ptr->prefer_fetch_merged_part_time_threshold.totalSeconds() <= time(nullptr)
        && estimated_space >= storage_settings_ptr->prefer_fetch_merged_part_size_threshold)
    {
        /// If entry is old enough, and have enough size, and some replica has the desired part,
        /// then prefer fetching from replica.
        String replica = storage.findReplicaHavingPart(entry.new_part_name, true);    /// NOTE excessive ZK requests for same data later, may remove.
        if (!replica.empty())
        {
            LOG_DEBUG(log, "Prefer to fetch {} from replica {}", entry.new_part_name, replica);
            return PrepareResult{
                .prepared_successfully = false,
                .need_to_check_missing_part_in_fetch = true,
                .part_log_writer = {}
            };
        }
    }

    /// In some use cases merging can be more expensive than fetching
    /// and it may be better to spread merges tasks across the replicas
    /// instead of doing exactly the same merge cluster-wise

    if (storage.merge_strategy_picker.shouldMergeOnSingleReplica(entry))
    {
        std::optional<String> replica_to_execute_merge = storage.merge_strategy_picker.pickReplicaToExecuteMerge(entry);
        if (replica_to_execute_merge)
        {
            LOG_DEBUG(log,
                "Prefer fetching part {} from replica {} due to execute_merges_on_single_replica_time_threshold",
                entry.new_part_name, replica_to_execute_merge.value());

            return PrepareResult{
                .prepared_successfully = false,
                .need_to_check_missing_part_in_fetch = true,
                .part_log_writer = {}
            };

        }
    }

    new_part_info = MergeTreePartInfo::fromPartName(entry.new_part_name, storage.format_version);
    Strings mutation_ids;
    commands = std::make_shared<MutationCommands>(storage.queue.getMutationCommands(source_part, new_part_info.mutation, mutation_ids));
    LOG_TRACE(log, "Mutating part {} with mutation commands from {} mutations ({}): {}",
              entry.new_part_name, commands->size(), fmt::join(mutation_ids, ", "), commands->toString());

    /// Once we mutate part, we must reserve space on the same disk, because mutations can possibly create hardlinks.
    /// Can throw an exception.
    reserved_space = storage.reserveSpace(estimated_space, source_part->getDataPartStorage());

    table_lock_holder = storage.lockForShare(
            RWLockImpl::NO_QUERY, storage_settings_ptr->lock_acquire_timeout_for_background_operations);
    StorageMetadataPtr metadata_snapshot = storage.getInMemoryMetadataPtr();

    transaction_ptr = std::make_unique<MergeTreeData::Transaction>(storage, NO_TRANSACTION_RAW);

    future_mutated_part = std::make_shared<FutureMergedMutatedPart>();
    future_mutated_part->name = entry.new_part_name;
    future_mutated_part->uuid = entry.new_part_uuid;
    future_mutated_part->parts.push_back(source_part);
    future_mutated_part->part_info = new_part_info;
    future_mutated_part->updatePath(storage, reserved_space.get());
    future_mutated_part->part_format = source_part->getFormat();

    disk = reserved_space->getDisk();
    const bool is_zerocopy = storage_settings_ptr->allow_remote_fs_zero_copy_replication
        && disk->supportZeroCopyReplication();
    const bool is_vfs = disk->isObjectStorageVFS();
    if (is_zerocopy || is_vfs)
    {
        if (storage.findReplicaHavingCoveringPart(entry.new_part_name, true))
        {
            LOG_DEBUG(log, "Mutation of part {} finished by some other replica, will download mutated part",
                entry.new_part_name);
            return PrepareResult{
                .prepared_successfully = false,
                .need_to_check_missing_part_in_fetch = true,
                .part_log_writer = {}
            };
        }

        mitigateReplicaSkew(estimated_space);

        if (is_zerocopy)
            zero_copy_lock = storage.tryCreateZeroCopyExclusiveLock(entry.new_part_name, disk);
        const bool zerocopy_lock_already_acquired = is_zerocopy
            && (!zero_copy_lock || !zero_copy_lock->isLocked());

        // TODO myrrc revisit whether we need table shared id
        const String vfs_lock_path = fs::path(storage.getTableSharedID()) / entry.new_part_name / "mutate";
        const bool vfs_lock_already_acquired = is_vfs && !disk->lock(vfs_lock_path, false);

        if (zerocopy_lock_already_acquired || vfs_lock_already_acquired)
        {
            LOG_DEBUG(
                log,
                "Mutation of part {} started by some other replica, will wait for it and mutated merged part. Number of tries {}",
                entry.new_part_name,
                entry.num_tries);
            storage.watchZeroCopyLock(entry.new_part_name, disk);

            return PrepareResult{
                .prepared_successfully = false,
                .need_to_check_missing_part_in_fetch = false,
                .part_log_writer = {}
            };
        }
        else if (storage.findReplicaHavingCoveringPart(entry.new_part_name, /* active */ false))
        {
            /// Why this if still needed? We can check for part in zookeeper, don't find it and sleep for any amount of time. During this sleep part will be actually committed from other replica
            /// and exclusive zero copy lock will be released. We will take the lock and execute mutation one more time, while it was possible just to download the part from other replica.
            ///
            /// It's also possible just because reads in [Zoo]Keeper are not lineariazable.
            ///
            /// NOTE: In case of mutation and hardlinks it can even lead to extremely rare dataloss (we will produce new part with the same hardlinks, don't fetch the same from other replica), so this check is important.
            ///
            /// In case of DROP_RANGE on fast replica and stale replica we can have some failed select queries in case of zero copy replication.
            if (zero_copy_lock)
                zero_copy_lock->lock->unlock();
            disk->unlock(vfs_lock_path);

            LOG_DEBUG(log, "We took zero copy lock, but mutation of part {} finished by some other replica, will release lock and download mutated part to avoid data duplication", entry.new_part_name);
            return PrepareResult{
                .prepared_successfully = false,
                .need_to_check_missing_part_in_fetch = true,
                .part_log_writer = {}
            };
        }
        else
        {
            LOG_DEBUG(log, "Zero copy lock taken, will mutate part {}", entry.new_part_name);
        }
    }

    task_context = Context::createCopy(storage.getContext());
    task_context->makeQueryContext();
    task_context->setCurrentQueryId(getQueryId());

    merge_mutate_entry = storage.getContext()->getMergeList().insert(
        storage.getStorageID(),
        future_mutated_part,
        task_context);

    stopwatch_ptr = std::make_unique<Stopwatch>();

    mutate_task = storage.merger_mutator.mutatePartToTemporaryPart(
            future_mutated_part, metadata_snapshot, commands, merge_mutate_entry.get(),
            entry.create_time, task_context, NO_TRANSACTION_PTR, reserved_space, table_lock_holder);

    /// Adjust priority
    for (auto & item : future_mutated_part->parts)
        priority.value += item->getBytesOnDisk();

    return {true, true, [this] (const ExecutionStatus & execution_status)
    {
        auto profile_counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(profile_counters.getPartiallyAtomicSnapshot());
        storage.writePartLog(
            PartLogElement::MUTATE_PART, execution_status, stopwatch_ptr->elapsed(),
            entry.new_part_name, new_part, future_mutated_part->parts, merge_mutate_entry.get(), std::move(profile_counters_snapshot));
    }};
}


bool MutateFromLogEntryTask::finalize(ReplicatedMergeMutateTaskBase::PartLogWriter write_part_log)
{
    new_part = mutate_task->getFuture().get();
    auto & data_part_storage = new_part->getDataPartStorage();
    if (data_part_storage.hasActiveTransaction())
        data_part_storage.precommitTransaction();

    storage.renameTempPartAndReplace(new_part, *transaction_ptr);

    try
    {
        storage.checkPartChecksumsAndCommit(*transaction_ptr, new_part, mutate_task->getHardlinkedFiles());
    }
    catch (const Exception & e)
    {
        if (MergeTreeDataPartChecksums::isBadChecksumsErrorCode(e.code()))
        {
            transaction_ptr->rollback();

            ProfileEvents::increment(ProfileEvents::DataAfterMutationDiffersFromReplica);

            LOG_ERROR(log, "{}. Data after mutation is not byte-identical to data on another replicas. "
                           "We will download merged part from replica to force byte-identical result.", getCurrentExceptionMessage(false));

            write_part_log(ExecutionStatus::fromCurrentException("", true));

            if (storage.getSettings()->detach_not_byte_identical_parts)
                storage.forcefullyMovePartToDetachedAndRemoveFromMemory(std::move(new_part), "mutate-not-byte-identical");
            else
                storage.tryRemovePartImmediately(std::move(new_part));

            /// No need to delete the part from ZK because we can be sure that the commit transaction
            /// didn't go through.

            return false;
        }

        throw;
    }

    if (zero_copy_lock)
    {
        LOG_DEBUG(log, "Removing zero-copy lock");
        zero_copy_lock->lock->unlock();
    }
    const String lock_path = fs::path(storage.getTableSharedID()) / entry.new_part_name / "mutate";
    disk->unlock(lock_path);

    /** With `ZSESSIONEXPIRED` or `ZOPERATIONTIMEOUT`, we can inadvertently roll back local changes to the parts.
         * This is not a problem, because in this case the entry will remain in the queue, and we will try again.
         */
    finish_callback = [storage_ptr = &storage]() { storage_ptr->merge_selecting_task->schedule(); };
    ProfileEvents::increment(ProfileEvents::ReplicatedPartMutations);
    write_part_log({});

    return true;
}


}
