#include <Disks/ObjectStorages/DiskObjectStorageTransaction.h>
#include <Disks/ObjectStorages/DiskObjectStorageTransactionOperation.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/IO/WriteBufferWithFinalizeCallback.h>
#include <Interpreters/Context.h>
#include <Common/checkStackSize.h>
#include <ranges>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Disks/WriteMode.h>
#include <base/defines.h>

#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT;
    extern const int LOGICAL_ERROR;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CANNOT_OPEN_FILE;
    extern const int FILE_DOESNT_EXIST;
    extern const int BAD_FILE_TYPE;
    extern const int FILE_ALREADY_EXISTS;
}

DiskObjectStorageTransaction::DiskObjectStorageTransaction(
    IObjectStorage & object_storage_,
    IMetadataStorage & metadata_storage_,
    DiskObjectStorageRemoteMetadataRestoreHelper * metadata_helper_)
    : object_storage(object_storage_)
    , metadata_storage(metadata_storage_)
    , metadata_transaction(metadata_storage.createTransaction())
    , metadata_helper(metadata_helper_)
{}


DiskObjectStorageTransaction::DiskObjectStorageTransaction(
    IObjectStorage & object_storage_,
    IMetadataStorage & metadata_storage_,
    DiskObjectStorageRemoteMetadataRestoreHelper * metadata_helper_,
    MetadataTransactionPtr metadata_transaction_)
    : object_storage(object_storage_)
    , metadata_storage(metadata_storage_)
    , metadata_transaction(metadata_transaction_)
    , metadata_helper(metadata_helper_)
{}

MultipleDisksObjectStorageTransaction::MultipleDisksObjectStorageTransaction(
    IObjectStorage & object_storage_,
    IMetadataStorage & metadata_storage_,
    IObjectStorage& destination_object_storage_,
    IMetadataStorage& destination_metadata_storage_,
    DiskObjectStorageRemoteMetadataRestoreHelper * metadata_helper_)
    : DiskObjectStorageTransaction(object_storage_, metadata_storage_, metadata_helper_, destination_metadata_storage_.createTransaction())
    , destination_object_storage(destination_object_storage_)
    , destination_metadata_storage(destination_metadata_storage_)
{}

namespace
{
/// Operation which affects only metadata. Simplest way to
/// implement via callback.
struct PureMetadataObjectStorageOperation final : public IDiskObjectStorageOperation
{
    std::function<void(MetadataTransactionPtr tx)> on_execute;

    PureMetadataObjectStorageOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        std::function<void(MetadataTransactionPtr tx)> && on_execute_)
        : IDiskObjectStorageOperation(object_storage_, metadata_storage_)
        , on_execute(std::move(on_execute_))
    {}

    void execute(MetadataTransactionPtr transaction) override
    {
        on_execute(transaction);
    }

    void undo() override
    {
    }

    void finalize() override
    {
    }

    std::string getInfoForLog() const override { return fmt::format("PureMetadataObjectStorageOperation"); }
};

struct RemoveObjectStorageOperation final : public IDiskObjectStorageOperation
{
    std::string path;
    bool delete_metadata_only;
    ObjectsToRemove objects_to_remove;
    bool if_exists;
    bool remove_from_cache = false;

    RemoveObjectStorageOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const std::string & path_,
        bool delete_metadata_only_,
        bool if_exists_)
        : IDiskObjectStorageOperation(object_storage_, metadata_storage_)
        , path(path_)
        , delete_metadata_only(delete_metadata_only_)
        , if_exists(if_exists_)
    {}

    std::string getInfoForLog() const override
    {
        return fmt::format("RemoveObjectStorageOperation (path: {}, if exists: {})", path, if_exists);
    }

    void execute(MetadataTransactionPtr tx) override
    {
        if (!metadata_storage.exists(path))
        {
            if (if_exists)
                return;

            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Metadata path '{}' doesn't exist", path);
        }

        if (!metadata_storage.isFile(path))
            throw Exception(ErrorCodes::BAD_FILE_TYPE, "Path '{}' is not a regular file", path);

        try
        {
            auto objects = metadata_storage.getStorageObjects(path);

            auto unlink_outcome = tx->unlinkMetadata(path);

            if (unlink_outcome)
                objects_to_remove = ObjectsToRemove{std::move(objects), std::move(unlink_outcome)};
        }
        catch (const Exception & e)
        {
            /// If it's impossible to read meta - just remove it from FS.
            if (e.code() == ErrorCodes::UNKNOWN_FORMAT
                || e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF
                || e.code() == ErrorCodes::CANNOT_READ_ALL_DATA
                || e.code() == ErrorCodes::CANNOT_OPEN_FILE)
            {
                tx->unlinkFile(path);
            }
            else
                throw;
        }
    }

    void undo() override
    {

    }

    void finalize() override
    {
        /// The client for an object storage may do retries internally
        /// and there could be a situation when a query succeeded, but the response is lost
        /// due to network error or similar. And when it will retry an operation it may receive
        /// a 404 HTTP code. We don't want to threat this code as a real error for deletion process
        /// (e.g. throwing some exceptions) and thus we just use method `removeObjectsIfExists`
        if (!delete_metadata_only && !objects_to_remove.objects.empty()
            && objects_to_remove.unlink_outcome->num_hardlinks == 0)
        {
            object_storage.removeObjectsIfExist(objects_to_remove.objects);
        }
    }
};
};

struct ReplaceFileObjectStorageOperation final : public IDiskObjectStorageOperation
{
    std::string path_from;
    std::string path_to;
    StoredObjects objects_to_remove;

    ReplaceFileObjectStorageOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const std::string & path_from_,
        const std::string & path_to_)
        : IDiskObjectStorageOperation(object_storage_, metadata_storage_)
        , path_from(path_from_)
        , path_to(path_to_)
    {}

    std::string getInfoForLog() const override
    {
        return fmt::format("ReplaceFileObjectStorageOperation (path_from: {}, path_to: {})", path_from, path_to);
    }

    void execute(MetadataTransactionPtr tx) override
    {
        if (metadata_storage.exists(path_to))
        {
            objects_to_remove = metadata_storage.getStorageObjects(path_to);
            tx->replaceFile(path_from, path_to);
        }
        else
            tx->moveFile(path_from, path_to);
    }

    void undo() override
    {

    }

    void finalize() override
    {
        /// Read comment inside RemoveObjectStorageOperation class
        /// TL;DR Don't pay any attention to 404 status code
        if (!objects_to_remove.empty())
            object_storage.removeObjectsIfExist(objects_to_remove);
    }
};

struct WriteFileObjectStorageOperation final : public IDiskObjectStorageOperation
{
    StoredObject object;
    std::function<void(MetadataTransactionPtr)> on_execute;

    WriteFileObjectStorageOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const StoredObject & object_)
        : IDiskObjectStorageOperation(object_storage_, metadata_storage_)
        , object(object_)
    {}

    std::string getInfoForLog() const override
    {
        return fmt::format("WriteFileObjectStorageOperation");
    }

    void setOnExecute(std::function<void(MetadataTransactionPtr)> && on_execute_)
    {
        on_execute = on_execute_;
    }

    void execute(MetadataTransactionPtr tx) override
    {
        if (on_execute)
            on_execute(tx);
    }

    void undo() override
    {
        if (object_storage.exists(object))
            object_storage.removeObject(object);
    }

    void finalize() override
    {
    }
};

void DiskObjectStorageTransaction::createDirectory(const std::string & path)
{
    operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [path](MetadataTransactionPtr tx)
        {
            tx->createDirectory(path);
        }));
}

void DiskObjectStorageTransaction::createDirectories(const std::string & path)
{
    operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [path](MetadataTransactionPtr tx)
        {
            tx->createDirectoryRecursive(path);
        }));
}


void DiskObjectStorageTransaction::moveDirectory(const std::string & from_path, const std::string & to_path)
{
    operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [from_path, to_path](MetadataTransactionPtr tx)
        {
            tx->moveDirectory(from_path, to_path);
        }));
}

void DiskObjectStorageTransaction::moveFile(const String & from_path, const String & to_path)
{
     operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [from_path, to_path, this](MetadataTransactionPtr tx)
        {
            if (metadata_storage.exists(to_path))
                throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "File already exists: {}", to_path);

            if (!metadata_storage.exists(from_path))
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} doesn't exist, cannot move", from_path);

            tx->moveFile(from_path, to_path);
        }));
}

void DiskObjectStorageTransaction::replaceFile(const std::string & from_path, const std::string & to_path)
{
    auto operation = std::make_unique<ReplaceFileObjectStorageOperation>(object_storage, metadata_storage, from_path, to_path);
    operations_to_execute.emplace_back(std::move(operation));
}

void DiskObjectStorageTransaction::clearDirectory(const std::string & path)
{
    for (auto it = metadata_storage.iterateDirectory(path); it->isValid(); it->next())
    {
        if (metadata_storage.isFile(it->path()))
            removeFile(it->path());
    }
}

void DiskObjectStorageTransaction::removeFile(const std::string & path)
{
    removeSharedFile(path, false);
}

void DiskObjectStorageTransaction::removeSharedFile(const std::string & path, bool keep_shared_data)
{
    auto operation = std::make_unique<RemoveObjectStorageOperation>(object_storage, metadata_storage, path, keep_shared_data, false);
    operations_to_execute.emplace_back(std::move(operation));
}

void DiskObjectStorageTransaction::removeSharedRecursive(
    const std::string & path, bool keep_all_shared_data, const NameSet & file_names_remove_metadata_only)
{
    auto operation = std::make_unique<RemoveRecursiveObjectStorageOperation>(
        object_storage, metadata_storage, path, keep_all_shared_data, file_names_remove_metadata_only);
    operations_to_execute.emplace_back(std::move(operation));
}

void DiskObjectStorageTransaction::removeSharedFileIfExists(const std::string & path, bool keep_shared_data)
{
    auto operation = std::make_unique<RemoveObjectStorageOperation>(object_storage, metadata_storage, path, keep_shared_data, true);
    operations_to_execute.emplace_back(std::move(operation));
}

void DiskObjectStorageTransaction::removeDirectory(const std::string & path)
{
    operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [path](MetadataTransactionPtr tx)
        {
            tx->removeDirectory(path);
        }));
}


void DiskObjectStorageTransaction::removeRecursive(const std::string & path)
{
    removeSharedRecursive(path, false, {});
}

void DiskObjectStorageTransaction::removeFileIfExists(const std::string & path)
{
    removeSharedFileIfExists(path, false);
}


void DiskObjectStorageTransaction::removeSharedFiles(
    const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only)
{
    auto operation = std::make_unique<RemoveManyObjectStorageOperation>(object_storage, metadata_storage, files, keep_all_batch_data, file_names_remove_metadata_only);
    operations_to_execute.emplace_back(std::move(operation));
}

namespace
{

String revisionToString(UInt64 revision)
{
    return std::bitset<64>(revision).to_string();
}

}

std::unique_ptr<WriteBufferFromFileBase> DiskObjectStorageTransaction::writeFile( /// NOLINT
    const String & path,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings,
    bool autocommit)
{
    StoredObject blob;
    return writeFileOps(path, buf_size, mode, settings, autocommit, blob);
}

std::unique_ptr<WriteBufferFromFileBase> DiskObjectStorageTransaction::writeFileOps( /// NOLINT
    const std::string & path,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings,
    bool autocommit,
    StoredObject& object)
{
    auto object_key = object_storage.generateObjectKeyForPath(path);
    std::optional<ObjectAttributes> object_attributes;

    if (metadata_helper)
    {
        if (!object_key.hasPrefix())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "metadata helper is not supported with absolute paths");

        auto revision = metadata_helper->revision_counter + 1;
        metadata_helper->revision_counter++;
        object_attributes = {
            {"path", path}
        };

        object_key = ObjectStorageKey::createAsRelative(
            object_key.getPrefix(),
            "r" + revisionToString(revision) + "-file-" + object_key.getSuffix());
    }

    /// seems ok
    object = StoredObject(object_key.serialize(), path);
    std::function<void(size_t count)> create_metadata_callback;

    if (autocommit)
    {
        create_metadata_callback = [tx = shared_from_this(), mode, path, key_ = std::move(object_key)](size_t count)
        {
            if (mode == WriteMode::Rewrite)
            {
                /// Otherwise we will produce lost blobs which nobody points to
                /// WriteOnce storages are not affected by the issue
                if (!tx->object_storage.isWriteOnce() && tx->metadata_storage.exists(path))
                    tx->object_storage.removeObjectsIfExist(tx->metadata_storage.getStorageObjects(path));

                tx->metadata_transaction->createMetadataFile(path, key_, count);
            }
            else
                tx->metadata_transaction->addBlobToMetadata(path, key_, count);

            // Inheriting files e.g. DiskObjectStorageVFSTransaction can add something to
            // operations_to_execute, so we can't just commit metadata
            tx->commit();
        };
    }
    else
    {
        auto write_operation = std::make_unique<WriteFileObjectStorageOperation>(object_storage, metadata_storage, object);

        create_metadata_callback = [object_storage_tx = shared_from_this(), write_op = write_operation.get(), mode, path, key_ = std::move(object_key)](size_t count)
        {
            /// This callback called in WriteBuffer finalize method -- only there we actually know
            /// how many bytes were written. We don't control when this finalize method will be called
            /// so here we just modify operation itself, but don't execute anything (and don't modify metadata transaction).
            /// Otherwise it's possible to get reorder of operations, like:
            /// tx->createDirectory(xxx) -- will add metadata operation in execute
            /// buf1 = tx->writeFile(xxx/yyy.bin)
            /// buf2 = tx->writeFile(xxx/zzz.bin)
            /// ...
            /// buf1->finalize() // shouldn't do anything with metadata operations, just memoize what to do
            /// tx->commit()
            write_op->setOnExecute([object_storage_tx, mode, path, key_, count](MetadataTransactionPtr tx)
            {
                if (mode == WriteMode::Rewrite)
                {
                    /// Otherwise we will produce lost blobs which nobody points to
                    /// WriteOnce storages are not affected by the issue
                    if (!object_storage_tx->object_storage.isWriteOnce() && object_storage_tx->metadata_storage.exists(path))
                    {
                        object_storage_tx->object_storage.removeObjectsIfExist(
                            object_storage_tx->metadata_storage.getStorageObjects(path));
                    }

                    tx->createMetadataFile(path, key_, count);
                }
                else
                    tx->addBlobToMetadata(path, key_, count);
            });
        };

        operations_to_execute.emplace_back(std::move(write_operation));
    }


    auto impl = object_storage.writeObject(
        object,
        /// We always use mode Rewrite because we simulate append using metadata and different files
        WriteMode::Rewrite,
        object_attributes,
        buf_size,
        settings);

    return std::make_unique<WriteBufferWithFinalizeCallback>(
        std::move(impl), std::move(create_metadata_callback), object.remote_path);
}

void DiskObjectStorageTransaction::writeFileUsingBlobWritingFunction(
    const String & path, WriteMode mode, WriteBlobFunction && write_blob_function)
{
    StoredObject object;
    writeFileUsingBlobWritingFunctionOps(path, mode, std::move(write_blob_function), object);
}

void DiskObjectStorageTransaction::writeFileUsingBlobWritingFunctionOps(
    const String & path, WriteMode mode, WriteBlobFunction && write_blob_function, StoredObject& object)
{
    /// This function is a simplified and adapted version of DiskObjectStorageTransaction::writeFile().
    auto object_key = object_storage.generateObjectKeyForPath(path);
    std::optional<ObjectAttributes> object_attributes;

    if (metadata_helper)
    {
        if (!object_key.hasPrefix())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "metadata helper is not supported with abs paths");

        auto revision = metadata_helper->revision_counter + 1;
        metadata_helper->revision_counter++;
        object_attributes = {
            {"path", path}
        };

        object_key = ObjectStorageKey::createAsRelative(
            object_key.getPrefix(),
            "r" + revisionToString(revision) + "-file-" + object_key.getSuffix());
    }

    /// seems ok
    object = StoredObject(object_key.serialize(), path);
    auto write_operation = std::make_unique<WriteFileObjectStorageOperation>(object_storage, metadata_storage, object);

    operations_to_execute.emplace_back(std::move(write_operation));

    /// See DiskObjectStorage::getBlobPath().
    Strings blob_path;
    blob_path.reserve(2);
    blob_path.emplace_back(object.remote_path);
    String objects_namespace = object_storage.getObjectsNamespace();
    if (!objects_namespace.empty())
        blob_path.emplace_back(objects_namespace);

    /// We always use mode Rewrite because we simulate append using metadata and different files
    size_t object_size = std::move(write_blob_function)(blob_path, WriteMode::Rewrite, object_attributes);

    /// Create metadata (see create_metadata_callback in DiskObjectStorageTransaction::writeFile()).
    if (mode == WriteMode::Rewrite)
    {
        if (!object_storage.isWriteOnce() && metadata_storage.exists(path))
            object_storage.removeObjectsIfExist(metadata_storage.getStorageObjects(path));

        metadata_transaction->createMetadataFile(path, std::move(object_key), object_size);
    }
    else
        metadata_transaction->addBlobToMetadata(path, std::move(object_key), object_size);
}


void DiskObjectStorageTransaction::createHardLink(const std::string & src_path, const std::string & dst_path)
{
    operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [src_path, dst_path](MetadataTransactionPtr tx)
        {
            tx->createHardLink(src_path, dst_path);
        }));
}

void DiskObjectStorageTransaction::setReadOnly(const std::string & path)
{
    operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [path](MetadataTransactionPtr tx)
        {
            tx->setReadOnly(path);
        }));
}

void DiskObjectStorageTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [path, timestamp](MetadataTransactionPtr tx)
        {
            tx->setLastModified(path, timestamp);
        }));
}

void DiskObjectStorageTransaction::chmod(const String & path, mode_t mode)
{
    operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [path, mode](MetadataTransactionPtr tx)
        {
            tx->chmod(path, mode);
        }));
}

void DiskObjectStorageTransaction::createFile(const std::string & path)
{
    operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [path](MetadataTransactionPtr tx)
        {
            tx->createEmptyMetadataFile(path);
        }));
}

void DiskObjectStorageTransaction::copyFile(const std::string & from_file_path, const std::string & to_file_path, const ReadSettings & read_settings, const WriteSettings & write_settings)
{
    operations_to_execute.emplace_back(
        std::make_unique<CopyFileObjectStorageOperation>(object_storage, metadata_storage, object_storage, read_settings, write_settings, from_file_path, to_file_path));
}

void MultipleDisksObjectStorageTransaction::copyFile(const std::string & from_file_path, const std::string & to_file_path, const ReadSettings & read_settings, const WriteSettings & write_settings)
{
    operations_to_execute.emplace_back(
        std::make_unique<CopyFileObjectStorageOperation>(object_storage, metadata_storage, destination_object_storage, read_settings, write_settings, from_file_path, to_file_path));
}

void DiskObjectStorageTransaction::commit()
{
    for (size_t i = 0; i < operations_to_execute.size(); ++i)
    {
        try
        {
            operations_to_execute[i]->execute(metadata_transaction);
        }
        catch (...)
        {
            tryLogCurrentException(
                &Poco::Logger::get("DiskObjectStorageTransaction"),
                fmt::format("An error occurred while executing transaction's operation #{} ({})", i, operations_to_execute[i]->getInfoForLog()));

            for (int64_t j = i; j >= 0; --j)
            {
                try
                {
                    operations_to_execute[j]->undo();
                }
                catch (...)
                {
                    tryLogCurrentException(
                        &Poco::Logger::get("DiskObjectStorageTransaction"),
                        fmt::format("An error occurred while undoing transaction's operation #{}", i));

                    throw;
                }
            }
            throw;
        }
    }

    try
    {
        metadata_transaction->commit();
    }
    catch (...)
    {
        for (const auto & operation : operations_to_execute | std::views::reverse)
            operation->undo();

        throw;
    }

    for (const auto & operation : operations_to_execute)
        operation->finalize();
}

void DiskObjectStorageTransaction::undo()
{
    for (const auto & operation : operations_to_execute | std::views::reverse)
        operation->undo();
}

}
