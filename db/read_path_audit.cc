//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/read_path_audit.h"

#ifdef ROCKSDB_READ_PATH_AUDIT

namespace ROCKSDB_NAMESPACE {

thread_local ReadPathAuditStats g_read_path_audit_stats;
std::atomic<bool> g_read_path_audit_enabled{false};

}  // namespace ROCKSDB_NAMESPACE

#endif
