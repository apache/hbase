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

import java.io.IOException;
import java.time.Instant;
import org.apache.hadoop.hbase.ClockOutOfSyncException;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores the active time of the last active master, so that a new active master can take an active
 * time greater than that of every earlier one even if its clock is behind theirs.
 * <p/>
 * Remote procedures send the active time of the master that started them, and a master rejects a
 * report carrying an active time greater than its own, as it comes from a procedure started by a
 * newer master. See {@code MasterRpcServices#throwOnOldMaster}.
 * <p/>
 * As a result the active time can be ahead of the wall clock of the current master: if an earlier
 * active master's clock was ahead, later active times follow the stored value until the clock
 * catches up with it.
 */
@InterfaceAudience.Private
public class MasterActiveTimeStore extends MasterStateStore {

  private static final Logger LOG = LoggerFactory.getLogger(MasterActiveTimeStore.class);

  public static final String STATE_NAME = "master_active_time";

  public MasterActiveTimeStore(MasterRegion masterRegion) throws IOException, KeeperException {
    super(masterRegion, STATE_NAME, null, null);
  }

  /**
   * Returns an active time for a master becoming active: the given time if it is greater than the
   * last stored active time, or else the last stored active time plus one. The returned time is
   * stored before returning.
   * <p/>
   * If the last stored active time is ahead of the given time by more than {@code maxClockSkewMs},
   * the clocks of the masters are too far apart to continue, and this throws
   * {@link ClockOutOfSyncException} without storing anything.
   * <p/>
   * Must only be called by the active master, after the master local region has been opened, so
   * that an earlier active master can no longer update the stored time.
   * @param now            the current time of this master
   * @param maxClockSkewMs how far the last stored active time may be ahead of {@code now}
   * @return the active time of this master
   * @throws ClockOutOfSyncException if the last stored active time is too far ahead of {@code now}
   */
  public long advance(long now, long maxClockSkewMs) throws IOException {
    byte[] state = getState();
    long activeTime = now;
    if (state != null && state.length != Bytes.SIZEOF_LONG) {
      LOG.warn("Ignoring stored master active time of unexpected length {}", state.length);
    } else if (state != null) {
      long last = Bytes.toLong(state);
      if (last - now > maxClockSkewMs) {
        throw new ClockOutOfSyncException("The last active master became active at " + last + " ("
          + Instant.ofEpochMilli(last) + "), " + (last - now) + "ms after the current time " + now
          + " (" + Instant.ofEpochMilli(now) + ") of this master, which is more than the max"
          + " allowed clock skew of " + maxClockSkewMs + "ms (" + ServerManager.MAX_CLOCK_SKEW_MS
          + "); check the clocks of the masters");
      }
      if (now <= last) {
        LOG.warn(
          "The last active master became active at {} ({}), which is not before the current time"
            + " {} ({}) of this master, possibly because of clock skew between the masters;"
            + " using {} as active time",
          last, Instant.ofEpochMilli(last), now, Instant.ofEpochMilli(now), last + 1);
        activeTime = last + 1;
      }
    }
    setState(Bytes.toBytes(activeTime));
    return activeTime;
  }
}
