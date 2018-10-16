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
package org.apache.hadoop.hbase.io.hfile;

import org.apache.hbase.thirdparty.com.google.common.collect.MinMaxPriorityQueue;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.io.HeapSize;

/**
 * A memory-bound queue that will grow until an element brings
 * total size &gt;= maxSize.  From then on, only entries that are sorted larger
 * than the smallest current entry will be inserted/replaced.
 *
 * <p>Use this when you want to find the largest elements (according to their
 * ordering, not their heap size) that consume as close to the specified
 * maxSize as possible.  Default behavior is to grow just above rather than
 * just below specified max.
 *
 * <p>Object used in this queue must implement {@link HeapSize} as well as
 * {@link Comparable}.
 */
@InterfaceAudience.Private
public class LruCachedBlockQueue implements HeapSize {

  private MinMaxPriorityQueue<LruCachedBlock> queue;

  private long heapSize;
  private long maxSize;

  /**
   * @param maxSize the target size of elements in the queue
   * @param blockSize expected average size of blocks
   */
  public LruCachedBlockQueue(long maxSize, long blockSize) {
    int initialSize = (int)(maxSize / blockSize);
    if(initialSize == 0) initialSize++;
    queue = MinMaxPriorityQueue.expectedSize(initialSize).create();
    heapSize = 0;
    this.maxSize = maxSize;
  }

  /**
   * Attempt to add the specified cached block to this queue.
   *
   * <p>If the queue is smaller than the max size, or if the specified element
   * is ordered before the smallest element in the queue, the element will be
   * added to the queue.  Otherwise, there is no side effect of this call.
   * @param cb block to try to add to the queue
   */
  public void add(LruCachedBlock cb) {
    if(heapSize < maxSize) {
      queue.add(cb);
      heapSize += cb.heapSize();
    } else {
      LruCachedBlock head = queue.peek();
      if(cb.compareTo(head) > 0) {
        heapSize += cb.heapSize();
        heapSize -= head.heapSize();
        if(heapSize > maxSize) {
          queue.poll();
        } else {
          heapSize += head.heapSize();
        }
        queue.add(cb);
      }
    }
  }

  /**
   * @return The next element in this queue, or {@code null} if the queue is
   * empty.
   */
  public LruCachedBlock poll() {
    return queue.poll();
  }

  /**
   * @return The last element in this queue, or {@code null} if the queue is
   * empty.
   */
  public LruCachedBlock pollLast() {
    return queue.pollLast();
  }

  /**
   * Total size of all elements in this queue.
   * @return size of all elements currently in queue, in bytes
   */
  @Override
  public long heapSize() {
    return heapSize;
  }
}
