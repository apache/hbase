/**
 * Copyright 2013 The Apache Software Foundation
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

package org.apache.hadoop.hbase.conf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Maintains a set of all the classes which are would like to get notified
 * when the Configuration is reloaded from disk.
 */
public class ConfigurationManager {
  public static final Log LOG = LogFactory.getLog(ConfigurationManager.class);

  // The set of Configuration Observers. These classes would like to get
  // notified when the configuration is reloaded from disk. This is a set
  // constructed from a WeakHashMap, whose entries would be removed if the
  // observer classes go out of scope.
  private Set<ConfigurationObserver> configurationObservers =
    Collections.newSetFromMap(new WeakHashMap<ConfigurationObserver,
                                              Boolean>());

  /**
   * Register an observer class
   * @param observer
   */
  public void registerObserver(ConfigurationObserver observer) {
    synchronized (configurationObservers) {
      configurationObservers.add(observer);
    }
  }

  /**
   * Deregister an observer class
   * @param observer
   */
  public void deregisterObserver(ConfigurationObserver observer) {
    synchronized (configurationObservers) {
      configurationObservers.remove(observer);
    }
  }

  /**
   * The conf object has been repopulated from disk, and we have to notify
   * all the observers that are expressed interest to do that.
   */
  public void notifyAllObservers(Configuration conf) {
    synchronized (configurationObservers) {
      for (ConfigurationObserver observer : configurationObservers) {
        try {
          observer.notifyOnChange(conf);
        } catch (NullPointerException e) {
          // Though not likely, but a null pointer exception might happen
          // if the GC happens after this iteration started and before
          // we call the notifyOnChange() method. A
          // ConcurrentModificationException will not happen since GC doesn't
          // change the structure of the map.
          LOG.error("Encountered a NPE while notifying observers.");
        } catch (Throwable t) {
          LOG.error("Encountered a throwable while notifying observers: " +
                    t.getMessage());
        }
      }
    }
  }

  /**
   * Return the number of observers.
   * @return
   */
  public int getNumObservers() {
    return configurationObservers.size();
  }
}
