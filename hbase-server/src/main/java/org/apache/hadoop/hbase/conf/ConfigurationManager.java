/**
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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Maintains the set of all the classes which would like to get notified
 * when the Configuration is reloaded from the disk in the Online Configuration
 * Change mechanism, which lets you update certain configuration properties
 * on-the-fly, without having to restart the cluster.
 *
 * If a class has configuration properties which you would like to be able to
 * change on-the-fly, do the following:
 * 1. Implement the {@link ConfigurationObserver} interface. This would require
 *    you to implement the
 *    {@link ConfigurationObserver#onConfigurationChange(Configuration)}
 *    method.  This is a callback that is used to notify your class' instance
 *    that the configuration has changed. In this method, you need to check
 *    if the new values for the properties that are of interest to your class
 *    are different from the cached values. If yes, update them.
 *
 *    However, be careful with this. Certain properties might be trivially
 *    mutable online, but others might not. Two properties might be trivially
 *    mutable by themselves, but not when changed together. For example, if a
 *    method uses properties "a" and "b" to make some decision, and is running
 *    in parallel when the notifyOnChange() method updates "a", but hasn't
 *    yet updated "b", it might make a decision on the basis of a new value of
 *    "a", and an old value of "b". This might introduce subtle bugs. This
 *    needs to be dealt on a case-by-case basis, and this class does not provide
 *    any protection from such cases.
 *
 * 2. Register the appropriate instance of the class with the
 *    {@link ConfigurationManager} instance, using the
 *    {@link ConfigurationManager#registerObserver(ConfigurationObserver)}
 *    method. For the RS side of things, the ConfigurationManager is a static
 *    member of the {@link org.apache.hadoop.hbase.regionserver.HRegionServer}
 *    class. Be careful not to do this in the constructor, as you might cause
 *    the 'this' reference to escape. Use a factory method, or an initialize()
 *    method which is called after the construction of the object.
 *
 * 3. Deregister the instance using the
 *    {@link ConfigurationManager#deregisterObserver(ConfigurationObserver)}
 *    method when it is going out of scope. In case you are not able to do that
 *    for any reason, it is still okay, since entries for dead observers are
 *    automatically collected during GC. But nonetheless, it is still a good
 *    practice to deregister your observer, whenever possible.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
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
      if (observer instanceof PropagatingConfigurationObserver) {
        ((PropagatingConfigurationObserver) observer).registerChildren(this);
      }
    }
  }

  /**
   * Deregister an observer class
   * @param observer
   */
  public void deregisterObserver(ConfigurationObserver observer) {
    synchronized (configurationObservers) {
      configurationObservers.remove(observer);
      if (observer instanceof PropagatingConfigurationObserver) {
        ((PropagatingConfigurationObserver) observer).deregisterChildren(this);
      }
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
          if (observer != null) {
            observer.onConfigurationChange(conf);
          }
        } catch (Throwable t) {
          LOG.error("Encountered a throwable while notifying observers: " + " of type : " +
              observer.getClass().getCanonicalName() + "(" + observer + ")", t);
        }
      }
    }
  }

  /**
   * @return the number of observers. 
   */
  public int getNumObservers() {
    synchronized (configurationObservers) {
      return configurationObservers.size();
    }
  }
}
