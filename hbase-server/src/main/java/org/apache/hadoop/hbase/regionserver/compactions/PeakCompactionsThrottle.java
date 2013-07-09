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

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

/**
 * The class used to track peak hours and compactions. peak compaction speed
 * should be limit.
 * 
 */
@InterfaceAudience.Private
public class PeakCompactionsThrottle {
  private static final Log LOG = LogFactory.getLog(PeakCompactionsThrottle.class);

  public static final String PEAK_START_HOUR = "hbase.peak.start.hour";
  public static final String PEAK_END_HOUR = "hbase.peak.end.hour";
  public static final String PEAK_COMPACTION_SPEED_ALLOWED =
      "hbase.regionserver.compaction.peak.maxspeed";
  public static final String PEAK_COMPACTION_SPEED_CHECK_INTERVAL =
      "hbase.regionserver.compaction.speed.check.interval";

  OffPeakHours offPeakHours;
  private long start;
  private long end;
  private long maxSpeedInPeak;
  private int sleepNumber = 0;
  private int sleepTimeTotal = 0;
  int bytesWritten = 0;
  int checkInterval = 0;

  public PeakCompactionsThrottle(Configuration conf) {
    offPeakHours = OffPeakHours.getInstance(conf, PEAK_START_HOUR, PEAK_END_HOUR);
    maxSpeedInPeak = conf.getInt(PEAK_COMPACTION_SPEED_ALLOWED, 30 * 1024 * 1024 /* 30 MB/s */);
    checkInterval = conf.getInt(PEAK_COMPACTION_SPEED_CHECK_INTERVAL, 30 * 1024 * 1024 /* 30 MB */);
  }

  /**
   * start compaction
   */
  public void startCompaction() {
    start = System.currentTimeMillis();
  }

  /**
   * finish compaction
   */
  public void finishCompaction(String region, String family) {
    if (sleepNumber > 0) {
      LOG.info("Region '" + region + "' family '" + family + "' 's maxSpeedInPeak is "
          + StringUtils.humanReadableInt(maxSpeedInPeak) + "/s compaction throttle: sleep number  "
          + sleepNumber + " sleep time " + sleepTimeTotal + "(ms)");
    }
  }

  /**
   * reset start time
   */
  void resetStartTime() {
    start = System.currentTimeMillis();
  }

  /**
   * Peak compaction throttle, if it is peak time and the compaction speed is too fast, sleep for a
   * while to slow down.
   */
  public void throttle(long numOfBytes) throws IOException {
    bytesWritten += numOfBytes;
    if (bytesWritten >= checkInterval) {
      checkAndSlowFastCompact(bytesWritten);
      bytesWritten = 0;
    }
  }

  private void checkAndSlowFastCompact(long numOfBytes) throws IOException {
    if (!isPeakHour()) {
      // not peak hour, just return.
      return;
    }
    if (maxSpeedInPeak <= 0) {
      return;
    }
    end = System.currentTimeMillis();
    long minTimeAllowed = numOfBytes * 1000 / maxSpeedInPeak; // ms
    long elapsed = end - start;
    if (elapsed < minTimeAllowed) {
      // too fast
      try {
        // sleep for a while to slow down.
        Thread.sleep(minTimeAllowed - elapsed);
        sleepNumber++;
        sleepTimeTotal += (minTimeAllowed - elapsed);
      } catch (InterruptedException ie) {
        IOException iie = new InterruptedIOException();
        iie.initCause(ie);
        throw iie;
      }
    }
    resetStartTime();
  }

  /**
   * @return whether this is peak hour
   */
  private boolean isPeakHour() {
    return offPeakHours.isOffPeakHour();
  }

  /**
   * For test
   */
  public int getSleepNumber() {
    return sleepNumber;
  }
}
