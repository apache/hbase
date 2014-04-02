/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;

import java.io.IOException;
import org.apache.hadoop.hbase.util.HasThread;

import com.google.common.base.Preconditions;

/**
 * Leases
 *
 * There are several server classes in HBase that need to track external clients
 * that occasionally send heartbeats.
 *
 * <p>
 * These external clients hold resources in the server class. Those resources
 * need to be released if the external client fails to send a heartbeat after
 * some interval of time passes.
 *
 * <p>
 * The Leases class is a general reusable class for this kind of pattern. An
 * instance of the Leases class will create a thread to do its dirty work. You
 * should close() the instance if you want to clean up the thread properly.
 *
 * <p>
 * NOTE: This class extends HasThread rather than Chore because the sleep time
 * can be interrupted when there is something to do, rather than the Chore sleep
 * time which is invariant.
 */
public class Leases extends HasThread {
  private static final Log LOG = LogFactory.getLog(Leases.class.getName());
  private final int leasePeriod;
  protected final ConcurrentHashMap<String, LeaseListener> leaseMap =
      new ConcurrentHashMap<String, LeaseListener>();
  private volatile boolean stopRequested = false;
  private final long threadWakeFrequencyMS;

  /**
   * Creates a lease monitor
   *
   * @param leasePeriod
   *          - length of time (milliseconds) that the lease is valid
   * @param threadWakeFrequencyMS
   *          - how often the lease should be checked (milliseconds)
   */
  public Leases(final int leasePeriod, long threadWakeFrequencyMS) {
    this.leasePeriod = leasePeriod;
    this.threadWakeFrequencyMS = threadWakeFrequencyMS;
  }

  /**
   * @see java.lang.Thread#run()
   */
  @Override
  public void run() {
    HashSet<String> expiredSet = new HashSet<String>();
    while (!stopRequested) {
      try {
        expiredSet.clear();
        long now = System.currentTimeMillis();
        for (Map.Entry<String, LeaseListener> entry : leaseMap.entrySet()) {
          Long startTS = entry.getValue().getLeaseStartTS();
          if ((now - startTS) >= leasePeriod) {
            expiredSet.add(entry.getKey());
            entry.getValue().leaseExpired();
          }
        }
        leaseMap.keySet().removeAll(expiredSet);
        Thread.sleep(threadWakeFrequencyMS);
      } catch (InterruptedException e) {
        LOG.error(e.getMessage(), e);
        continue;
      } catch (ConcurrentModificationException e) {
        LOG.error(e.getMessage(), e);
        assert false; // This should fail in unit tests.
        continue;
      } catch (Throwable e) {
        LOG.fatal("Unexpected exception killed leases thread", e);
        break;
      }
    }
    close();
  }

  /**
   * Shuts down this lease instance when all outstanding leases expire. Like
   * {@link #close()} but rather than violently ending all leases,
   * waits first on extant leases to finish.
   * Use this method if the lease holders could loose data, leak locks, etc.
   * Presumes client has shutdown allocation of new leases.
   */
  public void closeAfterLeasesExpire() {
    this.stopRequested = true;
  }

  /**
   * Shut down this Leases instance. All pending leases will be destroyed,
   * without any cancellation calls.
   */
  public void close() {
    LOG.info(Thread.currentThread().getName() + " closing leases");
    this.stopRequested = true;
    leaseMap.clear();
    LOG.info(Thread.currentThread().getName() + " closed leases");
  }

  /**
   * Obtain a lease.
   *
   * @param leaseName name of the lease
   * @param listener listener that will process lease expirations
   *
   * @return returns  the existing lease listener associated with the key,
   *                  null if this is a new key.
   * @throws LeaseStillHeldException
   */
  public void createLease(String leaseName, final LeaseListener listener)
    throws LeaseStillHeldException {
    if (stopRequested) {
      return;
    }
    if (leaseMap.put(leaseName, listener) != null) {
      throw new LeaseStillHeldException(leaseName);
    }
  }

  /**
   * Thrown if we are asked create a lease but lease on passed name already
   * exists.
   */
  @SuppressWarnings("serial")
  public static class LeaseStillHeldException extends IOException {
    private final String leaseName;

    /**
     * @param name
     */
    public LeaseStillHeldException(final String name) {
      this.leaseName = name;
    }

    /** @return name of lease */
    public String getName() {
      return this.leaseName;
    }
  }

  /**
   * Renew a lease.
   *
   * @param leaseName
   *          name of lease
   * @throws LeaseException
   */
  public void renewLease(final String leaseName) throws LeaseException {
    LeaseListener listener;
    if ((listener = leaseMap.get(leaseName)) == null) {
      throw new LeaseException("lease '" + leaseName + "' does not exist");
    }
    listener.setLeaseStartTS(System.currentTimeMillis());
  }

  /**
   * Client explicitly cancels a lease.
   *
   * @param leaseName
   *          name of lease
   * @throws LeaseException
   */
  public void cancelLease(final String leaseName) throws LeaseException {
    if (leaseMap.remove(leaseName) == null) {
      throw new LeaseException("lease '" + leaseName + "' does not exist");
    }
  }
}
