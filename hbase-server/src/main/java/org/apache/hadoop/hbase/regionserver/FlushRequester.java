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

import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Request a flush.
 */
@InterfaceAudience.Private
public interface FlushRequester {
  /**
   * Tell the listener the cache needs to be flushed.
   *
   * @param region the Region requesting the cache flush
   * @return true if our region is added into the queue, false otherwise
   */
  boolean requestFlush(HRegion region, FlushLifeCycleTracker tracker);

  /**
   * Tell the listener the cache needs to be flushed.
   *
   * @param region the Region requesting the cache flush
   * @param families stores of region to flush, if null then use flush policy
   * @return true if our region is added into the queue, false otherwise
   */
  boolean requestFlush(HRegion region, List<byte[]> families,
    FlushLifeCycleTracker tracker);

  /**
   * Tell the listener the cache needs to be flushed after a delay
   *
   * @param region the Region requesting the cache flush
   * @param delay after how much time should the flush happen
   * @return true if our region is added into the queue, false otherwise
   */
  boolean requestDelayedFlush(HRegion region, long delay);

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
  public void setGlobalMemStoreLimit(long globalMemStoreSize);
}
