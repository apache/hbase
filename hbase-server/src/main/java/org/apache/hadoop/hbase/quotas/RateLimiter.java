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

/**
 * Simple rate limiter.
 *
 * Usage Example:
 *   RateLimiter limiter = new RateLimiter(); // At this point you have a unlimited resource limiter
 *   limiter.set(10, TimeUnit.SECONDS);       // set 10 resources/sec
 *
 *   long lastTs = 0;             // You need to keep track of the last update timestamp
 *   while (true) {
 *     long now = System.currentTimeMillis();
 *
 *     // call canExecute before performing resource consuming operation
 *     bool canExecute = limiter.canExecute(now, lastTs);
 *     // If there are no available resources, wait until one is available
 *     if (!canExecute) Thread.sleep(limiter.waitInterval());
 *     // ...execute the work and consume the resource...
 *     limiter.consume();
 *   }
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RateLimiter {
  private long tunit = 1000;           // Timeunit factor for translating to ms.
  private long limit = Long.MAX_VALUE; // The max value available resource units can be refilled to.
  private long avail = Long.MAX_VALUE; // Currently available resource units

  public RateLimiter() {
  }

  /**
   * Set the RateLimiter max available resources and refill period.
   * @param limit The max value available resource units can be refilled to.
   * @param timeUnit Timeunit factor for translating to ms.
   */
  public void set(final long limit, final TimeUnit timeUnit) {
    switch (timeUnit) {
      case NANOSECONDS:
        throw new RuntimeException("Unsupported NANOSECONDS TimeUnit");
      case MICROSECONDS:
        throw new RuntimeException("Unsupported MICROSECONDS TimeUnit");
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
    }
    this.limit = limit;
    this.avail = limit;
  }

  public String toString() {
    if (limit == Long.MAX_VALUE) {
      return "RateLimiter(Bypass)";
    }
    return "RateLimiter(avail=" + avail + " limit=" + limit + " tunit=" + tunit + ")";
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

  /**
   * given the time interval, is there at least one resource available to allow execution?
   * @param now the current timestamp
   * @param lastTs the timestamp of the last update
   * @return true if there is at least one resource available, otherwise false
   */
  public boolean canExecute(final long now, final long lastTs) {
    return canExecute(now, lastTs, 1);
  }

  /**
   * given the time interval, are there enough available resources to allow execution?
   * @param now the current timestamp
   * @param lastTs the timestamp of the last update
   * @param amount the number of required resources
   * @return true if there are enough available resources, otherwise false
   */
  public synchronized boolean canExecute(final long now, final long lastTs, final long amount) {
    return avail >= amount ? true : refill(now, lastTs) >= amount;
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
    return (amount <= avail) ? 0 : ((amount * tunit) / limit) - ((avail * tunit) / limit);
  }

  /**
   * given the specified time interval, refill the avilable units to the proportionate
   * to elapsed time or to the prespecified limit.
   */
  private long refill(final long now, final long lastTs) {
    long delta = (limit * (now - lastTs)) / tunit;
    if (delta > 0) {
      avail = Math.min(limit, avail + delta);
    }
    return avail;
  }
}
