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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This is a generic HBase executor service. This component abstract a
 * threadpool, a queue to which jobs can be submitted and a Runnable that
 * handles the object that is added to the queue.
 *
 * In order to create a new HBExecutorService, you need to do:
 *   HBExecutorService.startExecutorService("myService");
 *
 * In order to use the service created above, you need to override the
 * HBEventHandler class and create an event type that submits to this service.
 *
 */
public class HBaseExecutorService {
  private static final Log LOG = LogFactory.getLog(HBaseExecutorService.class);
  // default number of threads in the pool
  private int corePoolSize = 1;
  // how long to retain excess threads
  private long keepAliveTimeInMillis = 1000;
  // the thread pool executor that services the requests
  ThreadPoolExecutor threadPoolExecutor;
  // work queue to use - unbounded queue
  BlockingQueue<Runnable> workQueue = new PriorityBlockingQueue<Runnable>();
  // name for this executor service
  String name;
  // hold the all the executors created in a map addressable by their names
  static Map<String, HBaseExecutorService> executorServicesMap =
    Collections.synchronizedMap(new HashMap<String, HBaseExecutorService>());

  /**
   * The following is a list of names for the various executor services in both
   * the master and the region server.
   */
  public enum HBaseExecutorServiceType {

    // Master executor services
    MASTER_CLOSE_REGION        (1),
    MASTER_OPEN_REGION         (2),
    MASTER_SERVER_OPERATIONS   (3),
    MASTER_TABLE_OPERATIONS    (4),
    MASTER_RS_SHUTDOWN         (5),

    // RegionServer executor services
    RS_OPEN_REGION             (20),
    RS_OPEN_ROOT               (21),
    RS_OPEN_META               (22),
    RS_CLOSE_REGION            (23),
    RS_CLOSE_ROOT              (24),
    RS_CLOSE_META              (25);

    HBaseExecutorServiceType(int value) {}

    public void startExecutorService(String serverName, int maxThreads) {
      String name = getExecutorName(serverName);
      if(HBaseExecutorService.isExecutorServiceRunning(name)) {
        LOG.debug("Executor service " + toString() + " already running on " + serverName);
        return;
      }
      LOG.debug("Starting executor service [" + name + "]");
      HBaseExecutorService.startExecutorService(name, maxThreads);
    }

    public HBaseExecutorService getExecutor(String serverName) {
      return HBaseExecutorService.getExecutorService(getExecutorName(serverName));
    }

    public String getExecutorName(String serverName) {
      return (this.toString() + "-" + serverName);
    }
  }

  /**
   * Start an executor service with a given name. If there was a service already
   * started with the same name, this throws a RuntimeException.
   * @param name Name of the service to start.
   */
  public static void startExecutorService(String name, int maxThreads) {
    if(executorServicesMap.get(name) != null) {
      throw new RuntimeException("An executor service with the name " + name + " is already running!");
    }
    HBaseExecutorService hbes = new HBaseExecutorService(name, maxThreads);
    executorServicesMap.put(name, hbes);
    LOG.debug("Starting executor service: " + name);
  }

  public static boolean isExecutorServiceRunning(String name) {
    return (executorServicesMap.containsKey(name));
  }

  /**
   * This method is an accessor for all the HBExecutorServices running so far
   * addressable by name. If there is no such service, then it returns null.
   */
  public static HBaseExecutorService getExecutorService(String name) {
    HBaseExecutorService executor = executorServicesMap.get(name);
    if(executor == null) {
      LOG.debug("Executor service [" + name + "] not found in " +
        executorServicesMap);
    }
    return executor;
  }

  public static void shutdown() {
    for(Entry<String, HBaseExecutorService> entry : executorServicesMap.entrySet()) {
      entry.getValue().threadPoolExecutor.shutdown();
    }
    executorServicesMap.clear();
  }

  protected HBaseExecutorService(String name, int maxThreads) {
    this.name = name;
    // create the thread pool executor
    threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maxThreads,
        keepAliveTimeInMillis, TimeUnit.MILLISECONDS, workQueue);
    // name the threads for this threadpool
    threadPoolExecutor.setThreadFactory(new NamedThreadFactory(name));
  }

  /**
   * Submit the event to the queue for handling.
   * @param event
   */
  public void submit(Runnable event) {
    threadPoolExecutor.execute(event);
  }
}
