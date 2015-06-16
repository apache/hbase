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

package org.apache.hadoop.hbase.quotas;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

import com.google.common.annotations.VisibleForTesting;

/**
 * Simple rate limiter.
 *
 * Usage Example:
 *    // At this point you have a unlimited resource limiter
 *   RateLimiter limiter = new AverageIntervalRateLimiter();
 *                         or new FixedIntervalRateLimiter();
 *   limiter.set(10, TimeUnit.SECONDS);       // set 10 resources/sec
 *
 *   while (true) {
 *     // call canExecute before performing resource consuming operation
 *     bool canExecute = limiter.canExecute();
 *     // If there are no available resources, wait until one is available
 *     if (!canExecute) Thread.sleep(limiter.waitInterval());
 *     // ...execute the work and consume the resource...
 *     limiter.consume();
 *   }
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class RateLimiter {
  public static final String QUOTA_RATE_LIMITER_CONF_KEY = "hbase.quota.rate.limiter";
  private long tunit = 1000;           // Timeunit factor for translating to ms.
  private long limit = Long.MAX_VALUE; // The max value available resource units can be refilled to.
  private long avail = Long.MAX_VALUE; // Currently available resource units

  /**
   * Refill the available units w.r.t the elapsed time.
   * @param limit Maximum available resource units that can be refilled to.
   * @return how many resource units may be refilled ?
   */
  abstract long refill(long limit);

  /**
   * Time in milliseconds to wait for before requesting to consume 'amount' resource.
   * @param limit Maximum available resource units that can be refilled to.
   * @param available Currently available resource units
   * @param amount Resources for which time interval to calculate for
   * @return estimate of the ms required to wait before being able to provide 'amount' resources.
   */
  abstract long getWaitInterval(long limit, long available, long amount);


  /**
   * Set the RateLimiter max available resources and refill period.
   * @param limit The max value available resource units can be refilled to.
   * @param timeUnit Timeunit factor for translating to ms.
   */
  public void set(final long limit, final TimeUnit timeUnit) {
    switch (timeUnit) {
    case MILLISECONDS:
      tunit = 1;
      break;
    case SECONDS:
      tunit = 1000;
      break;
    case MINUTES:
      tunit = 60 * 1000;
      break;
    case HOURS:
      tunit = 60 * 60 * 1000;
      break;
    case DAYS:
      tunit = 24 * 60 * 60 * 1000;
      break;
    default:
      throw new RuntimeException("Unsupported " + timeUnit.name() + " TimeUnit.");
    }
    this.limit = limit;
    this.avail = limit;
  }

  public String toString() {
    String rateLimiter = this.getClass().getSimpleName();
    if (limit == Long.MAX_VALUE) {
      return rateLimiter + "(Bypass)";
    }
    return rateLimiter + "(avail=" + avail + " limit=" + limit + " tunit=" + tunit + ")";
  }

  /**
   * Sets the current instance of RateLimiter to a new values.
   *
   * if current limit is smaller than the new limit, bump up the available resources.
   * Otherwise allow clients to use up the previously available resources.
   */
  public synchronized void update(final RateLimiter other) {
    this.tunit = other.tunit;
    if (this.limit < other.limit) {
      this.avail += (other.limit - this.limit);
    }
    this.limit = other.limit;
  }

  public synchronized boolean isBypass() {
    return limit == Long.MAX_VALUE;
  }

  public synchronized long getLimit() {
    return limit;
  }

  public synchronized long getAvailable() {
    return avail;
  }

  protected long getTimeUnitInMillis() {
    return tunit;
  }

  /**
   * Is there at least one resource available to allow execution?
   * @return true if there is at least one resource available, otherwise false
   */
  public boolean canExecute() {
    return canExecute(1);
  }

  /**
   * Are there enough available resources to allow execution?
   * @param amount the number of required resources
   * @return true if there are enough available resources, otherwise false
   */
  public synchronized boolean canExecute(final long amount) {
    long refillAmount = refill(limit);
    if (refillAmount == 0 && avail < amount) {
      return false;
    }
    // check for positive overflow
    if (avail <= Long.MAX_VALUE - refillAmount) {
      avail = Math.max(0, Math.min(avail + refillAmount, limit));
    } else {
      avail = Math.max(0, limit);
    }
    if (avail >= amount) {
      return true;
    }
    return false;
  }

  /**
   * consume one available unit.
   */
  public void consume() {
    consume(1);
  }

  /**
   * consume amount available units.
   * @param amount the number of units to consume
   */
  public synchronized void consume(final long amount) {
    this.avail -= amount;
    if (this.avail < 0) {
      this.avail = 0;
    }
  }

  /**
   * @return estimate of the ms required to wait before being able to provide 1 resource.
   */
  public long waitInterval() {
    return waitInterval(1);
  }

  /**
   * @return estimate of the ms required to wait before being able to provide "amount" resources.
   */
  public synchronized long waitInterval(final long amount) {
    // TODO Handle over quota?
    return (amount <= avail) ? 0 : getWaitInterval(limit, avail, amount);
  }

  // This method is for strictly testing purpose only
  @VisibleForTesting
  public void setNextRefillTime(long nextRefillTime) {
    this.setNextRefillTime(nextRefillTime);
  }

  public long getNextRefillTime() {
    return this.getNextRefillTime();
  }
}
