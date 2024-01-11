#include "config.h"

#include <Disks/DiskFactory.h>

#if USE_AZURE_BLOB_STORAGE

#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>

#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageAuth.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorageVFS.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Interpreters/Context.h>

namespace DB
{

void registerDiskAzureBlobStorage(DiskFactory & factory,
    bool global_skip_access_check,
    bool allow_vfs, // TODO: these should be converted to flags
    bool allow_vfs_gc)
{
    auto creator = [global_skip_access_check, allow_vfs, allow_vfs_gc](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & /*map*/) -> DiskPtr
    {
        (void)allow_vfs;
        (void)allow_vfs_gc;

        auto [metadata_path, metadata_disk] = prepareForLocalMetadata(name, config, config_prefix, context);

        ObjectStoragePtr azure_object_storage = std::make_unique<AzureObjectStorage>(
            name,
            getAzureBlobContainerClient(config, config_prefix),
            getAzureBlobStorageSettings(config, config_prefix, context));

        String key_prefix;
        auto metadata_storage = std::make_shared<MetadataStorageFromDisk>(metadata_disk, key_prefix);

#ifndef CLICKHOUSE_KEEPER_STANDALONE_BUILD
        constexpr auto key = "merge_tree.allow_object_storage_vfs";
        const bool enable_disk_vfs = allow_vfs && config.getBool(key, false);
        if (enable_disk_vfs)
        {
            auto disk = std::make_shared<DiskObjectStorageVFS>(
                name,
                key_prefix,
                "DiskAzureBlobStorageVFS",
                std::move(metadata_storage),
                std::move(azure_object_storage),
                config,
                config_prefix,
                allow_vfs_gc);

            disk->startup(context, global_skip_access_check);
            return disk;
        }
#endif

        std::shared_ptr<IDisk> azure_blob_storage_disk = std::make_shared<DiskObjectStorage>(
            name,
            /* no namespaces */ key_prefix,
            "DiskAzureBlobStorage",
            std::move(metadata_storage),
            std::move(azure_object_storage),
            config,
            config_prefix
        );

        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
        azure_blob_storage_disk->startup(context, skip_access_check);

        return azure_blob_storage_disk;
    };

    factory.registerDiskType("azure_blob_storage", creator);
}

}

#else

namespace DB
{

void registerDiskAzureBlobStorage(DiskFactory &,
    bool /* global_skip_access_check */,
    bool /* allow_vfs */,
    bool /* allow_vfs_gc */) {}

}

#endif
