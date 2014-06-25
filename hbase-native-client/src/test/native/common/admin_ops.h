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
#ifndef HBASE_TESTS_ADMIN_OPS_H_
#define HBASE_TESTS_ADMIN_OPS_H_

#include <hbase/types.h>

#include "byte_buffer.h"

namespace hbase {
namespace test {

int32_t
ensureTable(
    hb_connection_t connection,
    bool createNew,
    const char* table_name,
    bytebuffer families[], size_t numFamilies);

} /* namespace test */
} /* namespace hbase */

#endif /* HBASE_TESTS_ADMIN_OPS_H_ */
