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

package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * RPC Executor that uses different queues for reads and writes for meta.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MetaRWQueueRpcExecutor extends RWQueueRpcExecutor {
  public static final String META_CALL_QUEUE_READ_SHARE_CONF_KEY =
      "hbase.ipc.server.metacallqueue.read.ratio";
  public static final String META_CALL_QUEUE_SCAN_SHARE_CONF_KEY =
      "hbase.ipc.server.metacallqueue.scan.ratio";
  public static final float DEFAULT_META_CALL_QUEUE_READ_SHARE = 0.9f;

  public MetaRWQueueRpcExecutor(final String name, final int handlerCount, final int maxQueueLength,
      final PriorityFunction priority, final Configuration conf, final Abortable abortable) {
    super(name, handlerCount, maxQueueLength, priority, conf, abortable);
  }

  @Override
  protected float getReadShare(final Configuration conf) {
    return conf.getFloat(META_CALL_QUEUE_READ_SHARE_CONF_KEY, DEFAULT_META_CALL_QUEUE_READ_SHARE);
  }

  @Override
  protected float getScanShare(final Configuration conf) {
    return conf.getFloat(META_CALL_QUEUE_SCAN_SHARE_CONF_KEY, 0);
  }
}
