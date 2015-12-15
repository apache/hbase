/**
 *
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
package org.apache.hadoop.hbase.executor;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Server;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

/**
 * Abstract base class for all HBase event handlers. Subclasses should
 * implement the {@link #process()} and {@link #prepare()} methods.  Subclasses
 * should also do all necessary checks up in their prepare() if possible -- check
 * table exists, is disabled, etc. -- so they fail fast rather than later when process
 * is running.  Do it this way because process be invoked directly but event
 * handlers are also
 * run in an executor context -- i.e. asynchronously -- and in this case,
 * exceptions thrown at process time will not be seen by the invoker, not till
 * we implement a call-back mechanism so the client can pick them up later.
 * <p>
 * Event handlers have an {@link EventType}.
 * {@link EventType} is a list of ALL handler event types.  We need to keep
 * a full list in one place -- and as enums is a good shorthand for an
 * implemenations -- because event handlers can be passed to executors when
 * they are to be run asynchronously. The
 * hbase executor, see ExecutorService, has a switch for passing
 * event type to executor.
 * <p>
 * Event listeners can be installed and will be called pre- and post- process if
 * this EventHandler is run in a Thread (its a Runnable so if its {@link #run()}
 * method gets called).  Implement
 * {@link EventHandlerListener}s, and registering using
 * {@link #setListener(EventHandlerListener)}.
 * @see ExecutorService
 */
@InterfaceAudience.Private
public abstract class EventHandler implements Runnable, Comparable<Runnable> {
  private static final Log LOG = LogFactory.getLog(EventHandler.class);

  // type of event this object represents
  protected EventType eventType;

  protected Server server;

  // sequence id generator for default FIFO ordering of events
  protected static final AtomicLong seqids = new AtomicLong(0);

  // sequence id for this event
  private final long seqid;

  // Listener to call pre- and post- processing.  May be null.
  private EventHandlerListener listener;

  // Time to wait for events to happen, should be kept short
  protected int waitingTimeForEvents;

  private final Span parent;

  /**
   * This interface provides pre- and post-process hooks for events.
   */
  public interface EventHandlerListener {
    /**
     * Called before any event is processed
     * @param event The event handler whose process method is about to be called.
     */
    void beforeProcess(EventHandler event);
    /**
     * Called after any event is processed
     * @param event The event handler whose process method is about to be called.
     */
    void afterProcess(EventHandler event);
  }

  /**
   * Default base class constructor.
   */
  public EventHandler(Server server, EventType eventType) {
    this.parent = Trace.currentSpan();
    this.server = server;
    this.eventType = eventType;
    seqid = seqids.incrementAndGet();
    if (server != null) {
      this.waitingTimeForEvents = server.getConfiguration().
          getInt("hbase.master.event.waiting.time", 1000);
    }
  }

  /**
   * Event handlers should do all the necessary checks in this method (rather than
   * in the constructor, or in process()) so that the caller, which is mostly executed
   * in the ipc context can fail fast. Process is executed async from the client ipc,
   * so this method gives a quick chance to do some basic checks.
   * Should be called after constructing the EventHandler, and before process().
   * @return the instance of this class
   * @throws Exception when something goes wrong
   */
  public EventHandler prepare() throws Exception {
    return this;
  }

  @Override
  public void run() {
    TraceScope chunk = Trace.startSpan(this.getClass().getSimpleName(), parent);
    try {
      if (getListener() != null) getListener().beforeProcess(this);
      process();
      if (getListener() != null) getListener().afterProcess(this);
    } catch(Throwable t) {
      handleException(t);
    } finally {
      chunk.close();
    }
  }

  /**
   * This method is the main processing loop to be implemented by the various
   * subclasses.
   * @throws IOException
   */
  public abstract void process() throws IOException;

  /**
   * Return the event type
   * @return The event type.
   */
  public EventType getEventType() {
    return this.eventType;
  }

  /**
   * Get the priority level for this handler instance.  This uses natural
   * ordering so lower numbers are higher priority.
   * <p>
   * Lowest priority is Integer.MAX_VALUE.  Highest priority is 0.
   * <p>
   * Subclasses should override this method to allow prioritizing handlers.
   * <p>
   * Handlers with the same priority are handled in FIFO order.
   * <p>
   * @return Integer.MAX_VALUE by default, override to set higher priorities
   */
  public int getPriority() {
    return Integer.MAX_VALUE;
  }

  /**
   * @return This events' sequence id.
   */
  public long getSeqid() {
    return this.seqid;
  }

  /**
   * Default prioritized runnable comparator which implements a FIFO ordering.
   * <p>
   * Subclasses should not override this.  Instead, if they want to implement
   * priority beyond FIFO, they should override {@link #getPriority()}.
   */
  @Override
  public int compareTo(Runnable o) {
    EventHandler eh = (EventHandler)o;
    if(getPriority() != eh.getPriority()) {
      return (getPriority() < eh.getPriority()) ? -1 : 1;
    }
    return (this.seqid < eh.seqid) ? -1 : 1;
  }

  /**
   * @return Current listener or null if none set.
   */
  public synchronized EventHandlerListener getListener() {
    return listener;
  }

  /**
   * @param listener Listener to call pre- and post- {@link #process()}.
   */
  public synchronized void setListener(EventHandlerListener listener) {
    this.listener = listener;
  }

  @Override
  public String toString() {
    return "Event #" + getSeqid() +
      " of type " + eventType +
      " (" + getInformativeName() + ")";
  }

  /**
   * Event implementations should override thie class to provide an
   * informative name about what event they are handling. For example,
   * event-specific information such as which region or server is
   * being processed should be included if possible.
   */
  public String getInformativeName() {
    return this.getClass().toString();
  }

  /**
   * Event exception handler, may be overridden
   * @param t Throwable object
   */
  protected void handleException(Throwable t) {
    String msg = "Caught throwable while processing event " + eventType;
    LOG.error(msg, t);
    if (server != null) {
      server.abort(msg, t);
    }
  }
}
