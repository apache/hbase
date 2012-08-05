/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.util;

import java.io.InterruptedIOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.StopStatus;

/**
 * Suppose we are responding events and have the option to process them in real time or to defer
 * (buffer) them. Also suppose that we have an initial period when we have to buffer all the
 * events, after which we have to process all the buffered events and switch to real-time
 * processing, while events are still arriving. This class provides support for that use case. We
 * are assuming that any number of threads can add elements to the queue, but only one thread can
 * drain the queue and make the switch to real-time processing, and there are no other ways to take
 * elements out of the queue.
 *
 * @param <T> queue element type
 */
public class DrainableQueue<T> {
  private static final Log LOG = LogFactory.getLog(DrainableQueue.class);

  private final BlockingQueue<T> queue = new LinkedBlockingQueue<T>();
  private final String name;

  /** True if the queue has been completely drained. */
  private boolean drained = false;

  /** Making sure that only one thread can drain the queue at a time */
  private Object drainLock = new Object();

  /** We will stop draining the queue if this stop status is set to true */
  private StopStatus drainStop;
  
  public DrainableQueue(String name) {
    this.name = name;
  }

  /**
   * Enqueue an event if we are still in the "deferred processing" mode for this queue. Even if
   * we have already started draining the queue, we still enqueue events if the draining process
   * has not completed.
   */
  public synchronized boolean enqueue(T event) throws InterruptedIOException {
    if (!drained) {
      try {
        queue.put(event);
      } catch (InterruptedException ex) {
        String msg = "Could not add event to queue " + name;
        LOG.error(msg, ex);
        throw new InterruptedIOException(msg + ": " + ex.getMessage());
      }
      // Enqueued the event.
      return true;
    }
    // Event not accepted, tell the caller to process it in real time.
    return false;
  }

  /**
   * Find the top element in the queue if it is present. Used while draining the queue. If there
   * are no elements left in the queue, the "drained" status is set. Does not remove the element
   * from the queue.
   *
   * @return the head of the queue or null if the queue has been drained.
   */
  private T peek() throws InterruptedException {
    synchronized (this) {
      if (queue.isEmpty()) {
        drained = true;
        return null;
      }
    }
    // We are assuming that no elements could be taken out of the queue between the isEmpty check
    // above and here, because the only thread that can do that this thread, which is draining
    // the queue.
    return queue.peek();
  }

  public void drain(ParamCallable<T> processor) {
    synchronized (drainLock) {
      if (drainStop != null && drainStop.isStopped()) {
        LOG.error("Stopping draining event queue " + name + " because we are shutting down");
        return;
      }
      T event;
      try {
        while ((event = peek()) != null) {
          processor.call(event);
          // Assuming that this will always succeed as we are the only thread allowed to take
          // elements out of the queue, and we verified above that queue was not empty.
          // We cannot switch to real-time event processing before we finish processing the event
          // that came out of the queue because new events may arrive while the event is still
          // being processed, resulting in event re-ordering.
          queue.remove();
        }
      } catch (InterruptedException ex) {
        LOG.error("Interrupted while draining " + name);
        Thread.currentThread().interrupt();
      }
    }
  }

  public void stopDrainIfStopped(StopStatus drainStop) {
    this.drainStop = drainStop;
  }

}
