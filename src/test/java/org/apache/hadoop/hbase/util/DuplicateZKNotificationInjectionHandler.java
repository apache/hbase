/**
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventType;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.ZKUnassignedWatcher;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * Duplicates the ZKNotifications for Region opened events,
 * with a given probability.
 */
public class DuplicateZKNotificationInjectionHandler extends InjectionHandler {

  private static final Log LOG =
    LogFactory.getLog(DuplicateZKNotificationInjectionHandler.class);
  private Collection<InjectionEvent> events;

  private double probability;
  private Random rand;
  private long duplicatedEventCnt;

  public double getProbability() {
    return probability;
  }

  public void setProbability(double d) {
    this.probability = d;
  }

  public DuplicateZKNotificationInjectionHandler() {
    this(23434234);
  }

  public DuplicateZKNotificationInjectionHandler(long seed) {
    events = new ArrayList<InjectionEvent>();
    LOG.info("Using DuplicateZKNotificationInjectionHandler  with seed " + seed);
    rand = new Random(seed);
    duplicatedEventCnt = 0;
  }

  @Override
  protected void _processEvent(InjectionEvent event, Object... args) {
    if (events.contains(event)) {
      if (rand.nextDouble() < probability) {
        // let us duplicate the processing
        duplicatedEventCnt++;
        LOG.info("Duplicating event " + event + " for " + args);
        ZKUnassignedWatcher zk = (ZKUnassignedWatcher) args[0];
        EventType eventType = (EventType) args[1];
        String path = (String) args[2];
        byte [] data = (byte[]) args[3];
        try {
          zk.handleRegionStateInZK(eventType, path, data, true);
        } catch (IOException e) {
          LOG.error("Caught exception handling ZK Event", e);
          e.printStackTrace();
        }
      } else {
        LOG.debug("Not duplicating event " + event + " for " + args);
      }
    } else {
        LOG.warn("Unexpected event " + event + " for " + args);
    }
  }

  public long getDuplicatedEventCnt() {
    return duplicatedEventCnt;
  }

  public void duplicateEvent(InjectionEvent event) {
    events.add(event);
  }

}
