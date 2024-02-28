#pragma once

#include <Common/Logger.h>
#include "Disks/IDisk.h"
#include "Disks/ObjectStorages/VFSLogItem.h"

namespace DB
{

class DiskVFSTransactionGroupGuard final
{
public:
    explicit DiskVFSTransactionGroupGuard(DiskPtr disk_);
    ~DiskVFSTransactionGroupGuard();

private:
    LoggerPtr log;
    DiskPtr disk;
    VFSLogItem log_item;
};

}
