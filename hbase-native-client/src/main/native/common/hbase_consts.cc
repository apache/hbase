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
#include "hbase_consts.h"

namespace hbase {

const std::string HConstants::EMPTY_STRING;

const char *HConstants::ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";

const char *HConstants::ZK_ROOT_NODE = "zookeeper.znode.parent";

const char *HConstants::ZK_ENSEMBLE = "hbase.zookeeper.ensemble";

const char *HConstants::ZK_QUORUM = "hbase.zookeeper.quorum";

const char *HConstants::ZK_DEFAULT_PORT = "2181";

} /* namespace hbase */
