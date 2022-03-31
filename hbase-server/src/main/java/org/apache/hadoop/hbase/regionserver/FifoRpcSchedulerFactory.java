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
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Factory to use when you want to use the {@link FifoRpcScheduler}
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FifoRpcSchedulerFactory implements RpcSchedulerFactory {
  @Override
  public RpcScheduler create(Configuration conf, PriorityFunction priority, Abortable server) {
    int handlerCount = conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
      HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT);
    return new FifoRpcScheduler(conf, handlerCount);
  }

  @Deprecated
  @Override
  public RpcScheduler create(Configuration conf, PriorityFunction priority) {
    return create(conf, priority, null);
  }
}
