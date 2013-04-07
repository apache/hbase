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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

/**
 * The class used to track off-peak hours and compactions. Off-peak compaction counter
 * is global for the entire server, hours can be different per instance of this class,
 * based on the configuration of the corresponding store.
 */
@InterfaceAudience.Private
public class OffPeakCompactions {
  private static final Log LOG = LogFactory.getLog(OffPeakCompactions.class);
  private final static Calendar calendar = new GregorianCalendar();
  private int offPeakStartHour;
  private int offPeakEndHour;

  // TODO: replace with AtomicLong, see HBASE-7437.
  /**
   * Number of off peak compactions either in the compaction queue or
   * happening now. Please lock compactionCountLock before modifying.
   */
  private static long numOutstanding = 0;

  /**
   * Lock object for numOutstandingOffPeakCompactions
   */
  private static final Object compactionCountLock = new Object();

  public OffPeakCompactions(Configuration conf) {
    offPeakStartHour = conf.getInt("hbase.offpeak.start.hour", -1);
    offPeakEndHour = conf.getInt("hbase.offpeak.end.hour", -1);
    if (!isValidHour(offPeakStartHour) || !isValidHour(offPeakEndHour)) {
      if (!(offPeakStartHour == -1 && offPeakEndHour == -1)) {
        LOG.warn("Ignoring invalid start/end hour for peak hour : start = " +
            this.offPeakStartHour + " end = " + this.offPeakEndHour +
            ". Valid numbers are [0-23]");
      }
      this.offPeakStartHour = this.offPeakEndHour = -1;
    }
  }

  /**
   * Tries making the compaction off-peak.
   * @return Whether the compaction can be made off-peak.
   */
  public boolean tryStartOffPeakRequest() {
    if (!isOffPeakHour()) return false;
    synchronized(compactionCountLock) {
      if (numOutstanding == 0) {
         numOutstanding++;
         return true;
      }
    }
    return false;
  }

  /**
   * The current compaction finished, so reset the off peak compactions count
   * if this was an off peak compaction.
   */
  public void endOffPeakRequest() {
    long newValueToLog = -1;
    synchronized(compactionCountLock) {
      newValueToLog = --numOutstanding;
    }
    LOG.info("Compaction done, numOutstandingOffPeakCompactions is now " +  newValueToLog);
  }

  /**
   * @return whether this is off-peak hour
   */
  private boolean isOffPeakHour() {
    int currentHour = calendar.get(Calendar.HOUR_OF_DAY);
    // If offpeak time checking is disabled just return false.
    if (this.offPeakStartHour == this.offPeakEndHour) {
      return false;
    }
    if (this.offPeakStartHour < this.offPeakEndHour) {
      return (currentHour >= this.offPeakStartHour && currentHour < this.offPeakEndHour);
    }
    return (currentHour >= this.offPeakStartHour || currentHour < this.offPeakEndHour);
  }

  private static boolean isValidHour(int hour) {
    return (hour >= 0 && hour <= 23);
  }
}
