#include "DiskVFSTransactionGroupGuard.h"
#include "Common/logger_useful.h"

namespace DB
{

DiskVFSTransactionGroupGuard::DiskVFSTransactionGroupGuard(DiskPtr disk_)
: log(getLogger("DiskVFSTransactionGroupGuard"))
, disk(disk_)
{
    if (disk && !disk->isObjectStorageVFS())
    {
        LOG_TRACE(log, "Disk {} is not object storage, skipping transaction group", disk->getName());
        return;
    }
    LOG_TRACE(log, "Starting transaction group");
}

DiskVFSTransactionGroupGuard::~DiskVFSTransactionGroupGuard()
{
    LOG_TRACE(log, "Finishing transaction group");
    if (disk && disk->isObjectStorageVFS())
    {
        // disk->commitTransactionGroup();
    }
}


}
