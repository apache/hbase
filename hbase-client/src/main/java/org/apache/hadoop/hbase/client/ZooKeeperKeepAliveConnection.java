/**
 *
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
 */

package org.apache.hadoop.hbase.client;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * We inherit the current ZooKeeperWatcher implementation to change the semantic
 *  of the close: the new close won't immediately close the connection but
 *  will have a keep alive. See {@link HConnection}.
 * This allows to make it available with a consistent interface. The whole
 *  ZooKeeperWatcher use in HConnection will be then changed to remove the
 *   watcher part.
 *
 * This class is intended to be used internally by HBase classes; but not by
 * final user code. Hence it's package protected.
 */
class ZooKeeperKeepAliveConnection extends ZooKeeperWatcher{
  ZooKeeperKeepAliveConnection(
    Configuration conf, String descriptor,
    ConnectionImplementation conn) throws IOException {
    super(conf, descriptor, conn);
  }

  @Override
  public void close() {
    if (this.abortable != null) {
      ((ConnectionImplementation)abortable).releaseZooKeeperWatcher(this);
    }
  }

  void internalClose(){
    super.close();
  }
}
