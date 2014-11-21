package org.apache.hadoop.hbase.consensus.fsm;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.serial.SerialExecutorService.SerialExecutionStream;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of the FiniteStateMachineService where we have multiple
 * state machines being handled by a single thread. Used along with a
 * SerialExecutionStream object, which handles the multiplexing of the
 * events of multiple state machines.
 */
public class ConstitutentFSMService implements FiniteStateMachineService {
  private static final Log LOG = LogFactory.getLog(ConstitutentFSMService.class);
  private static final int MAX_EVENTS = 10;
  private static final long EVENT_PROCESSING_LATENCY_OUTLIER_THRESHOLD = 1000;
  private static final long MAX_TIME_TO_SPEND_IN_ASYNC_MS = 2000;
  FiniteStateMachine finiteStateMachine;
  SerialExecutionStream executionStream;

  private final AtomicInteger countPending = new AtomicInteger();

  // For all practical purposes, when this flag is set to true, we assume the
  // FSM to be shutdown / terminated. The multiplexer will lazily remove this
  // FSM from its queue.
  volatile boolean shutdownRequested = false;

  public ConstitutentFSMService(FiniteStateMachine fsm,
                                SerialExecutionStream executionStream) {
    this.finiteStateMachine = fsm;
    this.executionStream = executionStream;
  }

  protected void handleEvent(Event e) {
    finiteStateMachine.handleEvent(e);
  }

  @Override
  public boolean isShutdown() {
    return shutdownRequested;
  }

  @Override
  public boolean isTerminated() {
    return shutdownRequested;
  }

  @Override
  public String getName() {
    return finiteStateMachine.getName();
  }

  @Override
  public State getCurrentState() {
    return finiteStateMachine.getCurrentState();
  }

  @Override
  public boolean offer(final Event e) {
    if (shutdownRequested) {
      return false;
    }
    if (countPending.incrementAndGet() > MAX_EVENTS) {
      LOG.warn(String.format("%s has %d pending events in queue. Current" +
          " event:%s and current state:%s",
        finiteStateMachine.getName(), countPending.get(), e,
        finiteStateMachine.getCurrentState()));
    }

    Callable<ListenableFuture<Void>> command = new Callable<ListenableFuture<Void>>() {
      @Override
      public ListenableFuture<Void> call() {
        final long start = EnvironmentEdgeManager.currentTime();
        countPending.decrementAndGet();

        final String currentState = finiteStateMachine.getCurrentState().toString();
        handleEvent(e);
        ListenableFuture<?> future = finiteStateMachine.getAsyncCompletion();
        final long delta = EnvironmentEdgeManager.currentTime() - start;

        if (delta > EVENT_PROCESSING_LATENCY_OUTLIER_THRESHOLD) {
          LOG.warn(String.format("%s took %d ms from %s to %s.",
            finiteStateMachine.getName(), delta, currentState,
            finiteStateMachine.getCurrentState()));
        }
        if (future == null) {
          completeAsyncState();
          return null;
        } else {
          final long timestamp = System.currentTimeMillis();
          final SettableFuture<Void> readyForComplete = SettableFuture.create();
          Futures.addCallback(future, new FutureCallback<Object>() {
            @Override
            public void onFailure(Throwable t) {
              long taken = System.currentTimeMillis() - timestamp;
              if (taken > MAX_TIME_TO_SPEND_IN_ASYNC_MS) {
                LOG.error("action took too long to fail " + taken + ", event we handled: " + e
                    + ", current state: " + currentState);
              }
              if (!completeAsyncState()) {
                LOG.fatal("async state couldn't be completed after future completion");
              }
              LOG.error("Future failure ", t);
              readyForComplete.setException(t);
            }
            @Override
            public void onSuccess(Object result) {
              long taken = System.currentTimeMillis() - timestamp;
              if (taken > MAX_TIME_TO_SPEND_IN_ASYNC_MS) {
                LOG.error("action took too long to succeed " + taken + ", event we handled: " + e
                    + ", current state: " + currentState);
              }

              if (!completeAsyncState()) {
                LOG.fatal("async state couldn't be completed after future completion");
              }
              readyForComplete.set(null);
            }
          });
          return readyForComplete;
        }
      }

      @Override
      public String toString() {
        return "Command for event " + e;
      }
    };
    executionStream.execute(command);
    return true;
  }

  @Override
  public void shutdown() {
    // Setting the shutdownRequest = true will let the multiplexer lazily
    // reap the FSM when the event queue has drained.
    shutdownRequested = true;
  }

  @Override
  public void shutdownNow() {
    // There is no executor to call shutdownNow() on.
    shutdown();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    // We cannot actually terminate the executor, since other FSMs might be
    // using it.
    return true;
  }

  @Override
  public int getNumPendingEvents() {
    return countPending.get();
  }

  protected boolean completeAsyncState() {
    if (finiteStateMachine.isInAnIncompleteAsyncState()) {
      return false;
    }

    if (!shutdownRequested && finiteStateMachine.isInACompletedAsyncState()) {
      finiteStateMachine.finishAsyncStateCompletionHandling();
    }
    return true;
  }

}
