package org.apache.hadoop.hbase.consensus.fsm;

import java.util.concurrent.TimeUnit;

public class Util {
  public static boolean awaitTermination(
      final FiniteStateMachineService service,
      final long shutdownTimeout,
      final long shutdownNowTimeout,
      TimeUnit unit) {
    boolean isTerminated = false;
    try {
      isTerminated = service.awaitTermination(shutdownTimeout, unit);
      if (!isTerminated) {
        service.shutdownNow();
        isTerminated = service.awaitTermination(shutdownNowTimeout, unit);
      }
    } catch (InterruptedException e) {
      // Interrupted while waiting for termination. Something is bad, so
      // interrupt the current event.
      service.shutdownNow();
      // Honor the interrupt
      Thread.currentThread().interrupt();
    }
    return isTerminated;
  }
}
