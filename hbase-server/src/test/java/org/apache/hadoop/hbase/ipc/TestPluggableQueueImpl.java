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

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.util.BoundedPriorityBlockingQueue;

/**
 * Implementation of the PluggableBlockingQueue abstract class.
 *
 * Used to verify that the pluggable call queue type for the RpcExecutor can load correctly
 * via the FQCN reflection semantics.
 */
public class TestPluggableQueueImpl extends PluggableBlockingQueue implements
  ConfigurationObserver {

  private final BoundedPriorityBlockingQueue<CallRunner> inner;
  private static boolean configurationRecentlyChanged = false;

  public TestPluggableQueueImpl(int maxQueueLength, PriorityFunction priority, Configuration conf) {
    super(maxQueueLength, priority, conf);
    Comparator<CallRunner> comparator = Comparator.comparingInt(r -> r.getRpcCall().getPriority());
    inner = new BoundedPriorityBlockingQueue<>(maxQueueLength, comparator);
    configurationRecentlyChanged = false;
  }

  @Override public boolean add(CallRunner callRunner) {
    return inner.add(callRunner);
  }

  @Override public boolean offer(CallRunner callRunner) {
    return inner.offer(callRunner);
  }

  @Override public CallRunner remove() {
    return inner.remove();
  }

  @Override public CallRunner poll() {
    return inner.poll();
  }

  @Override public CallRunner element() {
    return inner.element();
  }

  @Override public CallRunner peek() {
    return inner.peek();
  }

  @Override public void put(CallRunner callRunner) throws InterruptedException {
    inner.put(callRunner);
  }

  @Override public boolean offer(CallRunner callRunner, long timeout, TimeUnit unit)
    throws InterruptedException {
    return inner.offer(callRunner, timeout, unit);
  }

  @Override public CallRunner take() throws InterruptedException {
    return inner.take();
  }

  @Override public CallRunner poll(long timeout, TimeUnit unit) throws InterruptedException {
    return inner.poll(timeout, unit);
  }

  @Override public int remainingCapacity() {
    return inner.remainingCapacity();
  }

  @Override public boolean remove(Object o) {
    return inner.remove(o);
  }

  @Override public boolean containsAll(Collection<?> c) {
    return inner.containsAll(c);
  }

  @Override public boolean addAll(Collection<? extends CallRunner> c) {
    return inner.addAll(c);
  }

  @Override public boolean removeAll(Collection<?> c) {
    return inner.removeAll(c);
  }

  @Override public boolean retainAll(Collection<?> c) {
    return inner.retainAll(c);
  }

  @Override public void clear() {
    inner.clear();
  }

  @Override public int size() {
    return inner.size();
  }

  @Override public boolean isEmpty() {
    return inner.isEmpty();
  }

  @Override public boolean contains(Object o) {
    return inner.contains(o);
  }

  @Override public Iterator<CallRunner> iterator() {
    return inner.iterator();
  }

  @Override public Object[] toArray() {
    return inner.toArray();
  }

  @Override public <T> T[] toArray(T[] a) {
    return inner.toArray(a);
  }

  @Override public int drainTo(Collection<? super CallRunner> c) {
    return inner.drainTo(c);
  }

  @Override public int drainTo(Collection<? super CallRunner> c, int maxElements) {
    return inner.drainTo(c, maxElements);
  }

  public static boolean hasObservedARecentConfigurationChange() {
    return configurationRecentlyChanged;
  }

  @Override public void onConfigurationChange(Configuration conf) {
    configurationRecentlyChanged = true;
  }
}
