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
package org.apache.hadoop.hbase.master.procedure;

import org.apache.hadoop.hbase.util.AvlUtil.AvlIterableList;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class FairQueue<T extends Comparable<T>> {

  private Queue<T> queueHead = null;
  private int size = 0;

  public boolean hasRunnables() {
    return size > 0;
  }

  public void add(Queue<T> queue) {
    // For normal priority queue, just append it to the tail
    if (queueHead == null || queue.getPriority() == 1) {
      queueHead = AvlIterableList.append(queueHead, queue);
      size++;
      return;
    }
    // Find the one which priority is less than us
    // For now only TableQueue and ServerQueue has priority. For TableQueue there are only a small
    // number of tables which have higher priority, and for ServerQueue there is only one server
    // which could carry meta which leads to a higher priority, so this will not be an expensive
    // operation.
    Queue<T> base = queueHead;
    do {
      if (base.getPriority() < queue.getPriority()) {
        queueHead = AvlIterableList.prepend(queueHead, base, queue);
        size++;
        return;
      }
      base = AvlIterableList.readNext(base);
    } while (base != queueHead);
    // no one is lower than us, append to the tail
    queueHead = AvlIterableList.append(queueHead, queue);
    size++;
  }

  public void remove(Queue<T> queue) {
    queueHead = AvlIterableList.remove(queueHead, queue);
    size--;
  }

  public Queue<T> poll() {
    if (queueHead == null) {
      return null;
    }
    Queue<T> q = queueHead;
    do {
      if (q.isAvailable()) {
        if (q.getPriority() == 1) {
          // for the normal priority queue, remove it and append it to the tail
          queueHead = AvlIterableList.remove(queueHead, q);
          queueHead = AvlIterableList.append(queueHead, q);
        }
        return q;
      }
      q = AvlIterableList.readNext(q);
    } while (q != queueHead);
    return null;
  }
}
