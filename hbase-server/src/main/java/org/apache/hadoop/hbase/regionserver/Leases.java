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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HasThread;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import java.io.IOException;

/**
 * Leases
 *
 * There are several server classes in HBase that need to track external
 * clients that occasionally send heartbeats.
 *
 * <p>These external clients hold resources in the server class.
 * Those resources need to be released if the external client fails to send a
 * heartbeat after some interval of time passes.
 *
 * <p>The Leases class is a general reusable class for this kind of pattern.
 * An instance of the Leases class will create a thread to do its dirty work.
 * You should close() the instance if you want to clean up the thread properly.
 *
 * <p>
 * NOTE: This class extends Thread rather than Chore because the sleep time
 * can be interrupted when there is something to do, rather than the Chore
 * sleep time which is invariant.
 */
@InterfaceAudience.Private
public class Leases extends HasThread {
  private static final Log LOG = LogFactory.getLog(Leases.class.getName());
  public static final int MIN_WAIT_TIME = 100;
  private final Map<String, Lease> leases = new ConcurrentHashMap<String, Lease>();

  protected final int leaseCheckFrequency;
  protected volatile boolean stopRequested = false;

  /**
   * Creates a lease monitor
   * 
   * @param leaseCheckFrequency - how often the lease should be checked
   *          (milliseconds)
   */
  public Leases(final int leaseCheckFrequency) {
    this.leaseCheckFrequency = leaseCheckFrequency;
    setDaemon(true);
  }

  /**
   * @see Thread#run()
   */
  @Override
  public void run() {
    long toWait = leaseCheckFrequency;
    Lease nextLease = null;
    long nextLeaseDelay = Long.MAX_VALUE;

    while (!stopRequested || (stopRequested && !leases.isEmpty()) ) {

      try {
        if (nextLease != null) {
          toWait = nextLease.getDelay(TimeUnit.MILLISECONDS);
        }

        toWait = Math.min(leaseCheckFrequency, toWait);
        toWait = Math.max(MIN_WAIT_TIME, toWait);

        Thread.sleep(toWait);
      } catch (InterruptedException e) {
        continue;
      } catch (ConcurrentModificationException e) {
        continue;
      } catch (Throwable e) {
        LOG.fatal("Unexpected exception killed leases thread", e);
        break;
      }

      nextLease = null;
      nextLeaseDelay = Long.MAX_VALUE;
      for (Iterator<Map.Entry<String, Lease>> it = leases.entrySet().iterator(); it.hasNext();) {
        Map.Entry<String, Lease> entry = it.next();
        Lease lease = entry.getValue();
        long thisLeaseDelay = lease.getDelay(TimeUnit.MILLISECONDS);
        if ( thisLeaseDelay > 0) {
          if (nextLease == null || thisLeaseDelay < nextLeaseDelay) {
            nextLease = lease;
            nextLeaseDelay = thisLeaseDelay;
          }
        } else {
          // A lease expired.  Run the expired code before removing from map
          // since its presence in map is used to see if lease exists still.
          if (lease.getListener() == null) {
            LOG.error("lease listener is null for lease " + lease.getLeaseName());
          } else {
            lease.getListener().leaseExpired();
          }
          it.remove();
        }
      }
    }
    close();
  }

  /**
   * Shuts down this lease instance when all outstanding leases expire.
   * Like {@link #close()} but rather than violently end all leases, waits
   * first on extant leases to finish.  Use this method if the lease holders
   * could lose data, leak locks, etc.  Presumes client has shutdown
   * allocation of new leases.
   */
  public void closeAfterLeasesExpire() {
    this.stopRequested = true;
  }

  /**
   * Shut down this Leases instance.  All pending leases will be destroyed,
   * without any cancellation calls.
   */
  public void close() {
    LOG.info(Thread.currentThread().getName() + " closing leases");
    this.stopRequested = true;
    leases.clear();
    LOG.info(Thread.currentThread().getName() + " closed leases");
  }

  /**
   * Create a lease and insert it to the map of leases.
   *
   * @param leaseName name of the lease
   * @param leaseTimeoutPeriod length of the lease in milliseconds
   * @param listener listener that will process lease expirations
   * @throws LeaseStillHeldException
   */
  public void createLease(String leaseName, int leaseTimeoutPeriod, final LeaseListener listener)
      throws LeaseStillHeldException {
    addLease(new Lease(leaseName, leaseTimeoutPeriod, listener));
  }

  /**
   * Inserts lease.  Resets expiration before insertion.
   * @param lease
   * @throws LeaseStillHeldException
   */
  public void addLease(final Lease lease) throws LeaseStillHeldException {
    if (this.stopRequested) {
      return;
    }
    if (leases.containsKey(lease.getLeaseName())) {
      throw new LeaseStillHeldException(lease.getLeaseName());
    }
    lease.resetExpirationTime();
    leases.put(lease.getLeaseName(), lease);
  }

  /**
   * Renew a lease
   *
   * @param leaseName name of lease
   * @throws LeaseException
   */
  public void renewLease(final String leaseName) throws LeaseException {
    if (this.stopRequested) {
      return;
    }
    Lease lease = leases.get(leaseName);

    if (lease == null ) {
      throw new LeaseException("lease '" + leaseName +
          "' does not exist or has already expired");
    }
    lease.resetExpirationTime();
  }

  /**
   * Client explicitly cancels a lease.
   * @param leaseName name of lease
   * @throws org.apache.hadoop.hbase.regionserver.LeaseException
   */
  public void cancelLease(final String leaseName) throws LeaseException {
    removeLease(leaseName);
  }

  /**
   * Remove named lease.
   * Lease is removed from the map of leases.
   * Lease can be reinserted using {@link #addLease(Lease)}
   *
   * @param leaseName name of lease
   * @throws org.apache.hadoop.hbase.regionserver.LeaseException
   * @return Removed lease
   */
  Lease removeLease(final String leaseName) throws LeaseException {
    Lease lease = leases.remove(leaseName);
    if (lease == null) {
      throw new LeaseException("lease '" + leaseName + "' does not exist");
    }
    return lease;
  }

  /**
   * Thrown if we are asked to create a lease but lease on passed name already
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

  /** This class tracks a single Lease. */
  static class Lease implements Delayed {
    private final String leaseName;
    private final LeaseListener listener;
    private int leaseTimeoutPeriod;
    private long expirationTime;

    Lease(final String leaseName, int leaseTimeoutPeriod, LeaseListener listener) {
      this.leaseName = leaseName;
      this.listener = listener;
      this.leaseTimeoutPeriod = leaseTimeoutPeriod;
      this.expirationTime = 0;
    }

    /** @return the lease name */
    public String getLeaseName() {
      return leaseName;
    }

    /** @return listener */
    public LeaseListener getListener() {
      return this.listener;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      return this.hashCode() == obj.hashCode();
    }

    @Override
    public int hashCode() {
      return this.leaseName.hashCode();
    }

    public long getDelay(TimeUnit unit) {
      return unit.convert(this.expirationTime - EnvironmentEdgeManager.currentTime(),
          TimeUnit.MILLISECONDS);
    }

    public int compareTo(Delayed o) {
      long delta = this.getDelay(TimeUnit.MILLISECONDS) -
        o.getDelay(TimeUnit.MILLISECONDS);

      return this.equals(o) ? 0 : (delta > 0 ? 1 : -1);
    }

    /**
     * Resets the expiration time of the lease.
     */
    public void resetExpirationTime() {
      this.expirationTime = EnvironmentEdgeManager.currentTime() + this.leaseTimeoutPeriod;
    }
  }
}
