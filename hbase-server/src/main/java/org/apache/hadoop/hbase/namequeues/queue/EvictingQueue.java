/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.namequeues.queue;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
public final class EvictingQueue<E> implements Serializable {

  private final Queue<E> delegate;
  final int maxSize;
  private static final long serialVersionUID = 0L;

  private EvictingQueue(int maxSize) {
    Preconditions.checkArgument(maxSize >= 0, "maxSize (%s) must >= 0", maxSize);
    this.delegate = new ArrayBlockingQueue<>(maxSize);
    this.maxSize = maxSize;
  }

  public static <E> EvictingQueue<E> create(int maxSize) {
    return new EvictingQueue<>(maxSize);
  }

  public int remainingCapacity() {
    return this.maxSize - this.delegate.size();
  }

  protected Queue<E> delegate() {
    return this.delegate;
  }

  public boolean offer(E e) {
    return this.add(e);
  }

  public boolean add(E e) {
    Preconditions.checkNotNull(e);
    if (this.maxSize == 0) {
      return true;
    } else {
      if (this.delegate().size() == this.maxSize) {
        this.delegate.remove();
      }
      this.delegate.add(e);
      return true;
    }
  }

  public <T> T[] toArray(T[] array) {
    return this.delegate().toArray(array);
  }

  public void clear() {
    this.delegate().clear();
  }

  public boolean isEmpty() {
    return this.delegate().isEmpty();
  }

  public E poll() {
    return this.delegate().poll();
  }
}
