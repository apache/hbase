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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.ipc.RpcScheduler;

/**
 * A factory class that constructs an {@link org.apache.hadoop.hbase.ipc.RpcScheduler} for
 * a region server.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public interface RpcSchedulerFactory {

  /**
   * Constructs a {@link org.apache.hadoop.hbase.ipc.RpcScheduler}.
   *
   * Please note that this method is called in constructor of {@link HRegionServer}, so some
   * fields may not be ready for access. The reason that {@code HRegionServer} is passed as
   * parameter here is that an RPC scheduler may need to access data structure inside
   * {@code HRegionServer} (see example in {@link SimpleRpcSchedulerFactory}).
   */
  RpcScheduler create(Configuration conf, RegionServerServices server);
}
