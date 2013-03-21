/*
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

/**
 * An Injection handler intended to introduce configurable delays
 * for events. <b>Not for production use.</b>
 */
public class DelayInducingInjectionHandler extends InjectionHandler {

  private static final Log LOG =
    LogFactory.getLog(DelayInducingInjectionHandler.class);

  private final ConcurrentMap<InjectionEvent, Long> eventToDelayTimeMs =
    new ConcurrentHashMap<InjectionEvent, Long>();

  private final ConcurrentMap<InjectionEvent, CountDownLatch> eventsToWaitFor =
    new ConcurrentHashMap<InjectionEvent, CountDownLatch>();

  public DelayInducingInjectionHandler() {
    LOG.warn("DelayInducingInjectionHandler initialized. Do not use this in" +
      " production");
  }

  public void setEventDelay(InjectionEvent event, long delayTimeMs) {
    LOG.warn("Setting delay of " + delayTimeMs + " ms");
    eventToDelayTimeMs.put(event, delayTimeMs);
  }

  public void awaitEvent(InjectionEvent event)
  throws InterruptedException {
    if (!eventToDelayTimeMs.containsKey(event)) {
      throw new IllegalArgumentException("No delay set for " + event + "!");
    }
    if (!eventsToWaitFor.containsKey(event)) {
      eventsToWaitFor.putIfAbsent(event, new CountDownLatch(1));
    }
    eventsToWaitFor.get(event).await();
  }

  @Override
  protected void _processEvent(InjectionEvent event, Object... args) {
    notifyAndSleep(event);
  }

  @Override
  protected void _processEventIO(InjectionEvent event, Object... args)
    throws IOException {
    notifyAndSleep(event);
  }

  private void notifyAndSleep(InjectionEvent event) {
    if (eventToDelayTimeMs.containsKey(event)) {
      if (!eventsToWaitFor.containsKey(event)) {
        eventsToWaitFor.putIfAbsent(event, new CountDownLatch(1));
      }
      eventsToWaitFor.get(event).countDown();
      long delayTimeMs = eventToDelayTimeMs.get(event);
      LOG.warn("Sleeping " + delayTimeMs + " ms for " + event);
      try {
        Thread.sleep(delayTimeMs);
      } catch (InterruptedException e) {
        LOG.warn("Sleep interrupted for " + event, e);
        Thread.currentThread().interrupt();
      }
    }
  }
}
