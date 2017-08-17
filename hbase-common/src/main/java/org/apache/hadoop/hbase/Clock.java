/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import java.util.concurrent.TimeUnit;

/**
 * A clock is an implementation of an algorithm to get timestamps corresponding to one of the
 * {@link TimestampType}s for the current time. Different clock implementations can have
 * different semantics associated with them. Every such clock should be able to map its
 * representation of time to one of the {link TimestampType}s.
 * HBase has traditionally been using the {@link java.lang.System#currentTimeMillis()} to
 * timestamp events in HBase. {@link java.lang.System#currentTimeMillis()} does not give any
 * guarantees about monotonicity of time. We will keep this implementation of clock in place for
 * backward compatibility and call it SYSTEM clock.
 * It is easy to provide monotonically non decreasing time semantics by keeping track of the last
 * timestamp given by the clock and updating it on receipt of external message. This
 * implementation of clock is called SYSTEM_MONOTONIC.
 * SYSTEM Clock and SYSTEM_MONOTONIC clock as described above, both being physical clocks, they
 * cannot track causality. Hybrid Logical Clocks(HLC), as described in
 * <a href="http://www.cse.buffalo.edu/tech-reports/2014-04.pdf">HLC Paper</a>, helps tracking
 * causality using a
 * <a href="http://research.microsoft.com/en-us/um/people/lamport/pubs/time-clocks.pdf">Logical
 * Clock</a> but always keeps the logical time close to the wall time or physical time. It kind
 * of has the advantages of both the worlds. One such advantage being getting consistent
 * snapshots in physical time as described in the paper. Hybrid Logical Clock has an additional
 * advantage that it is always monotonically increasing.
 * Note: It is assumed that any physical clock implementation has millisecond resolution else the
 * {@link TimestampType} implementation has to changed to accommodate it. It is decided after
 * careful discussion to go with millisecond resolution in the HLC design document attached in the
 * issue <a href="https://issues.apache.org/jira/browse/HBASE-14070">HBASE-14070 </a>.
 */

@InterfaceAudience.Private
public interface Clock {
  long DEFAULT_MAX_CLOCK_SKEW_IN_MS = 30000;

  /**
   * This is a method to get the current time.
   *
   * @return Timestamp of current time in 64 bit representation corresponding to the particular
   * clock
   */
  long now() throws RuntimeException;

  /**
   * This is a method to update the current time with the passed timestamp.
   * @param timestamp
   * @return Timestamp of current time in 64 bit representation corresponding to the particular
   * clock
   */
  long update(long timestamp) throws RuntimeException;

  /**
   * @return true if the clock implementation gives monotonically non decreasing timestamps else
   * false.
   */
  boolean isMonotonic();

  /**
   * @return {@link org.apache.hadoop.hbase.TimestampType}
   */
  TimestampType getTimestampType();

  /**
   * @return {@link org.apache.hadoop.hbase.ClockType}
   */
  ClockType getClockType();

  /**
   * @return {@link TimeUnit} of the physical time used by the clock.
   */
  TimeUnit getTimeUnit();

  //////////////////////////////////////////////////////////////////
  // Utility functions
  //////////////////////////////////////////////////////////////////

  // Only for testing.
  @VisibleForTesting
  static Clock getDummyClockOfGivenClockType(ClockType clockType) {
    if (clockType == ClockType.HYBRID_LOGICAL) {
      return new HybridLogicalClock();
    } else if (clockType == ClockType.SYSTEM_MONOTONIC) {
      return new SystemMonotonicClock();
    } else {
      return new SystemClock();
    }
  }
}