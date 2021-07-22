package org.apache.hadoop.hbase.ipc;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Internal runtime error type to indicate the RpcExecutor failed to execute a `pluggable`
 * call queue type. Either the FQCN for the class was missing in Configuration, not found on the
 * classpath, or is not a subtype of {@code BlockingQueue<CallRunner>}
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class PluggableRpcQueueNotFound extends RuntimeException {
  public PluggableRpcQueueNotFound(String message) {
    super(message);
  }
}
