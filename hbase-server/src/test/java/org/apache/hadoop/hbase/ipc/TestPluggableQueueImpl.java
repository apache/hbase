package org.apache.hadoop.hbase.ipc;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.BoundedPriorityBlockingQueue;

/**
 * Implementation of the PluggableBlockingQueue abstract class.
 *
 * Used to verify that the pluggable call queue type for the RpcExecutor can load correctly
 * via the FQCN reflection semantics.
 */
public class TestPluggableQueueImpl extends PluggableBlockingQueue {

  private final BoundedPriorityBlockingQueue<CallRunner> inner;

  public TestPluggableQueueImpl(int maxQueueLength, PriorityFunction priority, Configuration conf) {
    super(maxQueueLength, priority, conf);
    Comparator<CallRunner> comparator = Comparator.comparingInt(runner -> runner.getRpcCall().getPriority());
    inner = new BoundedPriorityBlockingQueue<>(maxQueueLength, comparator);
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
}
