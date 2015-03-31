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

package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Request a flush.
 */
@InterfaceAudience.Private
public interface FlushRequester {
  /**
   * Tell the listener the cache needs to be flushed.
   *
   * @param region the Region requesting the cache flush
   * @param forceFlushAllStores whether we want to flush all stores. e.g., when request from log
   *          rolling.
   */
  void requestFlush(Region region, boolean forceFlushAllStores);

  /**
   * Tell the listener the cache needs to be flushed after a delay
   *
   * @param region the Region requesting the cache flush
   * @param delay after how much time should the flush happen
   * @param forceFlushAllStores whether we want to flush all stores. e.g., when request from log
   *          rolling.
   */
  void requestDelayedFlush(Region region, long delay, boolean forceFlushAllStores);

  /**
   * Register a FlushRequestListener
   *
   * @param listener
   */
  void registerFlushRequestListener(final FlushRequestListener listener);

  /**
   * Unregister the given FlushRequestListener
   *
   * @param listener
   * @return true when passed listener is unregistered successfully.
   */
  public boolean unregisterFlushRequestListener(final FlushRequestListener listener);

  /**
   * Sets the global memstore limit to a new size.
   *
   * @param globalMemStoreSize
   */
  public void setGlobalMemstoreLimit(long globalMemStoreSize);
}
