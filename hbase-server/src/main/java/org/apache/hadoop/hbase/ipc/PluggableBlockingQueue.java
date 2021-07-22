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
