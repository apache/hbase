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
#pragma once

namespace google {
namespace protobuf {
class Message;
}
}
namespace folly {
class IOBuf;
}

namespace hbase {

/** @brief A class to convert data from ZooKeeper to other formats.
 *
 * This class will convert data to and from Zookeeper into protobuf objects.
 *
 */
class ZkDeserializer {
public:
  /**
   * Merge the data from a buffer into a given message.
   *
   * @param buf Naked pointer to iobuf containing data read from zookeeper.
   * @param out Naked pointer into which the data will be merged. The message
   * should be the correct type.
   * @return returns true if the parsing was successful.
   */
  bool Parse(folly::IOBuf *buf, google::protobuf::Message *out);
};
} // namespace hbase
