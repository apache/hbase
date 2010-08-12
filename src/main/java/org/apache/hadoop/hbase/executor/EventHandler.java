/**
 * Copyright 2010 The Apache Software Foundation
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.executor.HBaseExecutorService.HBaseExecutorServiceType;


/**
 * Abstract base class for all HBase event handlers. Subclasses should
 * implement the process() method where the actual handling of the event
 * happens.
 * <p>
 * HBaseEventType is a list of ALL events (which also corresponds to messages -
 * either internal to one component or between components). The event type
 * names specify the component from which the event originated, and the
 * component which is supposed to handle it.
 * <p>
 * Listeners can listen to all the events by implementing the interface
 * HBaseEventHandlerListener, and by registering themselves as a listener. They
 * will be called back before and after the process of every event.
 */
public abstract class EventHandler implements Runnable, Comparable<Runnable> {
  private static final Log LOG = LogFactory.getLog(EventHandler.class);
  // type of event this object represents
  protected EventType eventType;
  // server controller
  protected Server server;
  // listeners that are called before and after an event is processed
  protected static List<EventHandlerListener> eventHandlerListeners =
    Collections.synchronizedList(new ArrayList<EventHandlerListener>());
  // sequence id generator for default FIFO ordering of events
  protected static AtomicLong seqids = new AtomicLong(0);
  // sequence id for this event
  protected long seqid;

  /**
   * This interface provides hooks to listen to various events received by the
   * queue. A class implementing this can listen to the updates by calling
   * registerListener and stop receiving updates by calling unregisterListener
   */
  public interface EventHandlerListener {
    /**
     * Called before any event is processed
     */
    public void beforeProcess(EventHandler event);
    /**
     * Called after any event is processed
     */
    public void afterProcess(EventHandler event);
  }

  /**
   * These are a list of HBase events that can be handled by the various
   * HBaseExecutorService's. All the events are serialized as byte values.
   */
  public enum EventType {
    // Messages originating from RS (NOTE: there is NO direct communication from
    // RS to Master). These are a result of RS updates into ZK.
    RS2ZK_REGION_CLOSING      (1),   // RS is in process of closing a region
    RS2ZK_REGION_CLOSED       (2),   // RS has finished closing a region
    RS2ZK_REGION_OPENING      (3),   // RS is in process of opening a region
    RS2ZK_REGION_OPENED       (4),   // RS has finished opening a region

    // Messages originating from Master to RS
    M2RS_OPEN_REGION          (20),  // Master asking RS to open a region
    M2RS_OPEN_ROOT            (21),  // Master asking RS to open root
    M2RS_OPEN_META            (22),  // Master asking RS to open meta
    M2RS_CLOSE_REGION         (23),  // Master asking RS to close a region
    M2RS_CLOSE_ROOT           (24),  // Master asking RS to close root
    M2RS_CLOSE_META           (25),  // Master asking RS to close meta

    // Messages originating from Client to Master
    C2M_DELETE_TABLE          (40),   // Client asking Master to delete a table
    C2M_DISABLE_TABLE         (41),   // Client asking Master to disable a table
    C2M_ENABLE_TABLE          (42),   // Client asking Master to enable a table
    C2M_MODIFY_TABLE          (43),   // Client asking Master to modify a table
    C2M_ADD_FAMILY            (44),   // Client asking Master to add family to table
    C2M_DELETE_FAMILY         (45),   // Client asking Master to delete family of table
    C2M_MODIFY_FAMILY         (46),   // Client asking Master to modify family of table

    // Updates from master to ZK. This is done by the master and there is
    // nothing to process by either Master or RS
    M2ZK_REGION_OFFLINE       (50),  // Master adds this region as offline in ZK

    // Master controlled events to be executed on the master
    M_SERVER_SHUTDOWN         (70);  // Master is processing shutdown of a RS

    /**
     * Returns the executor service type (the thread pool instance) for this
     * event type.  Every type must be handled here.  Multiple types map to
     * Called by the HMaster. Returns a name of the executor service given an
     * event type. Every event type has an entry - if the event should not be
     * handled just add the NONE executor.
     * @return name of the executor service
     */
    public HBaseExecutorServiceType getExecutorServiceType() {
      switch(this) {

        // Master executor services

        case RS2ZK_REGION_CLOSED:
          return HBaseExecutorServiceType.MASTER_CLOSE_REGION;

        case RS2ZK_REGION_OPENED:
          return HBaseExecutorServiceType.MASTER_OPEN_REGION;

        case M_SERVER_SHUTDOWN:
          return HBaseExecutorServiceType.MASTER_SERVER_OPERATIONS;

        case C2M_DELETE_TABLE:
        case C2M_DISABLE_TABLE:
        case C2M_ENABLE_TABLE:
        case C2M_MODIFY_TABLE:
          return HBaseExecutorServiceType.MASTER_TABLE_OPERATIONS;

        // RegionServer executor services

        case M2RS_OPEN_REGION:
          return HBaseExecutorServiceType.RS_OPEN_REGION;

        case M2RS_OPEN_ROOT:
          return HBaseExecutorServiceType.RS_OPEN_ROOT;

        case M2RS_OPEN_META:
          return HBaseExecutorServiceType.RS_OPEN_META;

        case M2RS_CLOSE_REGION:
          return HBaseExecutorServiceType.RS_CLOSE_REGION;

        case M2RS_CLOSE_ROOT:
          return HBaseExecutorServiceType.RS_CLOSE_ROOT;

        case M2RS_CLOSE_META:
          return HBaseExecutorServiceType.RS_CLOSE_META;

        default:
          throw new RuntimeException("Unhandled event type " + this.name());
      }
    }

    /**
     * Start the executor service that handles the passed in event type. The
     * server that starts these event executor services wants to handle these
     * event types.
     */
    public void startExecutorService(String serverName, int maxThreads) {
      getExecutorServiceType().startExecutorService(serverName, maxThreads);
    }

    EventType(int value) {}

    @Override
    public String toString() {
      switch(this) {
        case RS2ZK_REGION_CLOSED:   return "CLOSED";
        case RS2ZK_REGION_CLOSING:  return "CLOSING";
        case RS2ZK_REGION_OPENED:   return "OPENED";
        case RS2ZK_REGION_OPENING:  return "OPENING";
        case M2ZK_REGION_OFFLINE:   return "OFFLINE";
        default:                    return this.name();
      }
    }
  }

  /**
   * Default base class constructor.
   */
  public EventHandler(Server server, EventType eventType) {
    this.server = server;
    this.eventType = eventType;
    seqid = seqids.incrementAndGet();
  }

  /**
   * This is a wrapper around process, used to update listeners before and after
   * events are processed.
   */
  public void run() {
    // fire all beforeProcess listeners
    for(EventHandlerListener listener : eventHandlerListeners) {
      listener.beforeProcess(this);
    }

    // call the main process function
    try {
      process();
    } catch(Throwable t) {
      LOG.error("Caught throwable while processing event " + eventType, t);
    }

    // fire all afterProcess listeners
    for(EventHandlerListener listener : eventHandlerListeners) {
      LOG.debug("Firing " + listener.getClass().getName() +
                ".afterProcess event listener for event " + eventType);
      listener.afterProcess(this);
    }
  }

  /**
   * This method is the main processing loop to be implemented by the various
   * subclasses.
   */
  public abstract void process();

  /**
   * Subscribe to updates before and after processing events
   */
  public static void registerListener(EventHandlerListener listener) {
    eventHandlerListeners.add(listener);
  }

  /**
   * Stop receiving updates before and after processing events
   */
  public static void unregisterListener(EventHandlerListener listener) {
    eventHandlerListeners.remove(listener);
  }

  /**
   * Return the name for this event type.
   * @return
   */
  public HBaseExecutorServiceType getEventHandlerName() {
    return eventType.getExecutorServiceType();
  }

  /**
   * Return the event type
   * @return
   */
  public EventType getEventType() {
    return eventType;
  }

  /**
   * Submits this event object to the correct executor service.
   */
  public void submit() {
    HBaseExecutorServiceType serviceType = getEventHandlerName();
    if(serviceType == null) {
      throw new RuntimeException("Event " + eventType + " not handled on " +
          "this server " + server.getServerName());
    }
    serviceType.getExecutor(server.getServerName()).submit(this);
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
   * Executes this event object in the caller's thread. This is a synchronous
   * way of executing the event.
   */
  public void execute() {
    this.run();
  }
}
