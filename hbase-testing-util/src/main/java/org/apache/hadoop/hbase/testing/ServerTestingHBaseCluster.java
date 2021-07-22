/**
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

package org.apache.hadoop.hbase.testing;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This interface is intended for uncommon testing use cases which need to access internal state
 * on an HBase minicluster. This may be necessary when testing code which runs within an HBase
 * server process, such as coprocessors or replication endpoints. It provides access to the
 * underlying {@link HBaseTestingUtil}, which its parent {@link TestingHBaseCluster} interface
 * intentionally does not. External use cases that just need to test their code using the HBase
 * client API should use {@link TestingHBaseCluster} instead.
 */
@InterfaceAudience.LimitedPrivate(
  {HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.REPLICATION,
    HBaseInterfaceAudience.PHOENIX})
public interface ServerTestingHBaseCluster extends TestingHBaseCluster {
  /**
   *
   * @return the utility running the minicluster
   */
  HBaseTestingUtil getUtil();

  /**
   * Create a {@link ServerTestingHBaseCluster}. You need to call {@link #start()} of the returned
   * {@link ServerTestingHBaseCluster} to actually start the mini testing cluster.
   */
  static ServerTestingHBaseCluster create(TestingHBaseClusterOption option) {
    return new TestingHBaseClusterImpl(option);
  }
}
