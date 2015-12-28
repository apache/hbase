/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "core/client.h"

#include <folly/Logging.h>
#include <folly/Random.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "if/ZooKeeper.pb.h"

using namespace folly;
using namespace hbase::pb;

int main(int argc, char *argv[]) {
  MetaRegionServer mrs;
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  FB_LOG_EVERY_MS(INFO, 10000) << "Hello";
  for (long i = 0; i < 10000000; i++) {
    FB_LOG_EVERY_MS(INFO, 1) << Random::rand32();
  }
  return 0;
}
