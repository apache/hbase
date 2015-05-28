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
package org.apache.hadoop.hbase.util;

import java.util.AbstractQueue;
import java.util.Iterator;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A bounded non-thread safe implementation of {@link java.util.Queue}.
 */
@InterfaceAudience.Private
public class BoundedArrayQueue<E> extends AbstractQueue<E> {

  private Object[] items;
  private int takeIndex, putIndex;
  private int count;

  public BoundedArrayQueue(int maxElements) {
    items =  new Object[maxElements];
  }

  @Override
  public int size() {
    return count;
  }

  /**
   * Not implemented and will throw {@link UnsupportedOperationException}
   */
  @Override
  public Iterator<E> iterator() {
    // We don't need this. Leaving it as not implemented.
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean offer(E e) {
    if (count == items.length) return false;
    items[putIndex] = e;
    if (++putIndex == items.length) putIndex = 0;
    count++;
    return true;
  }

  @Override
  public E poll() {
    return (count == 0) ? null : dequeue();
  }

  @SuppressWarnings("unchecked")
  private E dequeue() {
    E x = (E) items[takeIndex];
    items[takeIndex] = null;
    if (++takeIndex == items.length) takeIndex = 0;
    count--;
    return x;
  }

  @SuppressWarnings("unchecked")
  @Override
  public E peek() {
    return (E) items[takeIndex];
  }
}
