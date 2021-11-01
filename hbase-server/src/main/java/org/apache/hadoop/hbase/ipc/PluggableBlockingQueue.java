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

import java.util.concurrent.BlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Abstract class template for defining a pluggable blocking queue implementation to be used
 * by the 'pluggable' call queue type in the RpcExecutor.
 *
 * The intention is that the constructor shape helps re-inforce the expected parameters needed
 * to match up to how the RpcExecutor will instantiate instances of the queue.
 *
 * If the implementation class implements the
 * {@link org.apache.hadoop.hbase.conf.ConfigurationObserver} interface, it will also be wired
 * into configuration changes.
 *
 * Instantiation requires a constructor with {@code
 *     final int maxQueueLength,
 *     final PriorityFunction priority,
 *     final Configuration conf)}
 *  as the arguments.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class PluggableBlockingQueue implements BlockingQueue<CallRunner> {
  protected final int maxQueueLength;
  protected final PriorityFunction priority;
  protected final Configuration conf;

  public PluggableBlockingQueue(final int maxQueueLength,
        final PriorityFunction priority, final Configuration conf) {
    this.maxQueueLength = maxQueueLength;
    this.priority = priority;
    this.conf = conf;
  }
}
