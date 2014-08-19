/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <hbase/types.h>

#include "hbase_status.h"

namespace hbase {

const Status Status::Success(0);
const Status Status::EInvalid(EINVAL);
const Status Status::ENoEntry(ENOENT);
const Status Status::EAgain(EAGAIN);
const Status Status::ENoBufs(ENOBUFS);
const Status Status::EBusy(EBUSY);
const Status Status::ENoMem(ENOMEM);
const Status Status::ENoSys(ENOSYS);
const Status Status::ERange(ERANGE);

const Status Status::HBaseInternalError(HBASE_INTERNAL_ERR);
const Status Status::HBaseTableDisabled(HBASE_TABLE_DISABLED);

} /* namespace hbase */
