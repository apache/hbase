package org.apache.hadoop.hbase.consensus.fsm;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.util.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single threaded implementation of the FiniteStateMachineService
 */
public class FiniteStateMachineServiceImpl
        implements FiniteStateMachineService {
  protected static Logger LOG = LoggerFactory.getLogger(
          FiniteStateMachineServiceImpl.class);

  private FiniteStateMachine fsm;
  private ExecutorService executor;
  protected LinkedBlockingDeque<Event> eventQueue;
  protected volatile boolean shutdownRequested = false;
  private static final int SECONDS_TOWAIT_FOR_ASYNC_TO_COMPLETE = 1;

  public FiniteStateMachineServiceImpl(final FiniteStateMachine fsm) {
    this.fsm = fsm;
    this.eventQueue = new LinkedBlockingDeque<>();
    this.executor = Executors.newSingleThreadExecutor(
            new DaemonThreadFactory(String.format("%s-", fsm.getName())));
    start();
  }

  @Override
  public boolean isShutdown() {
    return shutdownRequested && executor.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return shutdownRequested && executor.isTerminated();
  }

  @Override
  public String getName() {
    return fsm.getName();
  }

  @Override
  public State getCurrentState() {
    return fsm.getCurrentState();
  }

  protected void start() {
    if (getCurrentState() != null) {
      executor.submit(new Runnable() {
        @Override
        public void run() {
          while (!(shutdownRequested && eventQueue.isEmpty())) {
            try {
              // Check if we are in an incomplete async state. If yes,
              // we will busy wait in this 'inner loop'.
              completeAsyncState();

              Event e = eventQueue.poll(200, TimeUnit.MILLISECONDS);
              if (e != null) {
                // At this place, we are guaranteed that the state, if async,
                // is complete, and any conditional transitions, if applicable,
                // have been taken. We can safely handle this event.
                fsm.handleEvent(e);
              }
            } catch (InterruptedException ex) {
              // This is most likely caused by a call to shutdownNow on the
              // executor, meaning shutdown took too long. If this is the case
              // shutdownRequested will be false on the next cycle and this task
              // should properly end.
              Thread.currentThread().interrupt();
              continue;
            } catch (AssertionError ex) {
              LOG.error("Assertion error ", ex);
              // Terminate the process
              System.exit(-1);
            } catch (Throwable ex) {
              LOG.error("Unexpected exception: ", ex);
            }
          }
        }
      });
    }
  }

  private void completeAsyncState() {
    while (!shutdownRequested) {
      if (fsm.waitForAsyncToComplete(
          SECONDS_TOWAIT_FOR_ASYNC_TO_COMPLETE, TimeUnit.SECONDS)) {
        break;
      }
    }
    fsm.finishAsyncStateCompletionHandling();
  }

  @Override
  public boolean offer(final Event e) {
    if (!shutdownRequested) {
      return eventQueue.offer(e);
    }
    return false;
  }

  @Override
  public void shutdown() {
    shutdownRequested = true;
    executor.shutdown();
  }

  @Override
  public void shutdownNow() {
    shutdownRequested = true;
    executor.shutdownNow();
  }

  @Override
  public boolean awaitTermination(final long timeout, TimeUnit unit)
          throws InterruptedException {
    return executor.awaitTermination(timeout, unit);
  }

  @Override
  public int getNumPendingEvents() {
    return eventQueue.size();
  }
}
