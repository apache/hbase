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
#ifndef HBASE_TESTS_COMMON_UTILS_H_
#define HBASE_TESTS_COMMON_UTILS_H_

#include <stdint.h>

#include <hbase/hbase.h>

#include "test_types.h"

namespace hbase {
namespace test {

void
flush_client_and_wait(hb_client_t client);

void
disconnect_client_and_wait(hb_client_t client);

cell_data_t*
new_cell_data();

} /* namespace test */
} /* namespace hbase */

#endif /* HBASE_TESTS_COMMON_UTILS_H_ */
