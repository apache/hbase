/*
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
package org.apache.hadoop.hbase.master;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Protection against zombie master. Started once Master accepts active responsibility and starts
 * taking over responsibilities. Allows a finite time window before giving up ownership.
 */
@InterfaceAudience.Private
class MasterInitializationMonitor extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(MasterInitializationMonitor.class);

  /** The amount of time in milliseconds to sleep before checking initialization status. */
  public static final String TIMEOUT_KEY = "hbase.master.initializationmonitor.timeout";
  public static final long TIMEOUT_DEFAULT = TimeUnit.MILLISECONDS.convert(15, TimeUnit.MINUTES);

  /**
   * When timeout expired and initialization has not complete, call {@link System#exit(int)} when
   * true, do nothing otherwise.
   */
  public static final String HALT_KEY = "hbase.master.initializationmonitor.haltontimeout";
  public static final boolean HALT_DEFAULT = false;

  private final HMaster master;
  private final long timeout;
  private final boolean haltOnTimeout;

  /** Creates a Thread that monitors the {@link #isInitialized()} state. */
  MasterInitializationMonitor(HMaster master) {
    super("MasterInitializationMonitor");
    this.master = master;
    this.timeout = master.getConfiguration().getLong(TIMEOUT_KEY, TIMEOUT_DEFAULT);
    this.haltOnTimeout = master.getConfiguration().getBoolean(HALT_KEY, HALT_DEFAULT);
    this.setDaemon(true);
  }

  @Override
  public void run() {
    try {
      while (!master.isStopped() && master.isActiveMaster()) {
        Thread.sleep(timeout);
        if (master.isInitialized()) {
          LOG.debug("Initialization completed within allotted tolerance. Monitor exiting.");
        } else {
          LOG.error("Master failed to complete initialization after " + timeout + "ms. Please" +
            " consider submitting a bug report including a thread dump of this process.");
          if (haltOnTimeout) {
            LOG.error("Zombie Master exiting. Thread dump to stdout");
            Threads.printThreadInfo(System.out, "Zombie HMaster");
            System.exit(-1);
          }
        }
      }
    } catch (InterruptedException ie) {
      LOG.trace("InitMonitor thread interrupted. Existing.");
    }
  }
}