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

package org.apache.hadoop.hbase.procedure2;

import java.util.Map;

import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * This class is a container of queues that allows to select a queue
 * in a round robin fashion, considering priority of the queue.
 *
 * the quantum is just how many poll() will return the same object.
 * e.g. if quantum is 1 and you have A and B as object you'll get: A B A B
 * e.g. if quantum is 2 and you have A and B as object you'll get: A A B B A A B B
 * then the object priority is just a priority * quantum
 *
 * Example:
 *  - three queues (A, B, C) with priorities (1, 1, 2)
 *  - The first poll() will return A
 *  - The second poll() will return B
 *  - The third and forth poll() will return C
 *  - and so on again and again.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureFairRunQueues<TKey, TQueue extends ProcedureFairRunQueues.FairObject> {
  private ConcurrentSkipListMap<TKey, TQueue> objMap =
    new ConcurrentSkipListMap<TKey, TQueue>();

  private final ReentrantLock lock = new ReentrantLock();
  private final int quantum;

  private Map.Entry<TKey, TQueue> current = null;
  private int currentQuantum = 0;

  public interface FairObject {
    boolean isAvailable();
    int getPriority();
  }

  /**
   * @param quantum how many poll() will return the same object.
   */
  public ProcedureFairRunQueues(final int quantum) {
    this.quantum = quantum;
  }

  public TQueue get(final TKey key) {
    return objMap.get(key);
  }

  public TQueue add(final TKey key, final TQueue queue) {
    TQueue oldq = objMap.putIfAbsent(key, queue);
    return oldq != null ? oldq : queue;
  }

  public TQueue remove(final TKey key) {
    TQueue queue = objMap.get(key);
    if (queue != null) {
      lock.lock();
      try {
        queue = objMap.remove(key);
        if (current != null && queue == current.getValue()) {
          currentQuantum = 0;
          current = null;
        }
      } finally {
        lock.unlock();
      }
    }
    return queue;
  }

  public void clear() {
    lock.lock();
    try {
      current = null;
      objMap.clear();
    } finally {
      lock.unlock();
    }
  }

  /**
   * @return the next available item if present
   */
  public TQueue poll() {
    lock.lock();
    try {
      TQueue queue;
      if (currentQuantum == 0) {
        if (nextObject() == null) {
          // nothing here
          return null;
        }

        queue = current.getValue();
        currentQuantum = calculateQuantum(queue) - 1;
      } else {
        currentQuantum--;
        queue = current.getValue();
      }

      if (!queue.isAvailable()) {
        Map.Entry<TKey, TQueue> last = current;
        // Try the next one
        do {
          if (nextObject() == null)
            return null;
        } while (current.getValue() != last.getValue() && !current.getValue().isAvailable());

        queue = current.getValue();
        currentQuantum = calculateQuantum(queue) - 1;
      }

      return queue;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append('{');
    for (Map.Entry<TKey, TQueue> entry: objMap.entrySet()) {
      builder.append(entry.getKey());
      builder.append(':');
      builder.append(entry.getValue());
    }
    builder.append('}');
    return builder.toString();
  }

  private Map.Entry<TKey, TQueue> nextObject() {
    Map.Entry<TKey, TQueue> next = null;

    // If we have already a key, try the next one
    if (current != null) {
      next = objMap.higherEntry(current.getKey());
    }

    // if there is no higher key, go back to the first
    current = (next != null) ? next : objMap.firstEntry();
    return current;
  }

  private int calculateQuantum(final TQueue fairObject) {
    // TODO
    return Math.max(1, fairObject.getPriority() * quantum);
  }
}
