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
package org.apache.hadoop.hbase.master.startupprogress;

import static org.apache.hadoop.hbase.util.EnvironmentEdgeManager.currentTime;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.yetus.audience.InterfaceAudience;

/*
  Creates a view for Startup Progress phases.
 */
@InterfaceAudience.Private
public class StartupProgressView {
  private MonitoredTask monitoredTask;
  private  DateTimeFormatter formatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of("UTC"));
  private static final String COMPLETE = "COMPLETE";
  private static final String RUNNING = "RUNNING";

  public StartupProgressView(MonitoredTask monitoredTask) {
    this.monitoredTask = monitoredTask;
  }

  /**
   * Return list of phase with starttime, endtime and status
   * @return list of phases
   */
  public List<Phase> getPhaseStatus() {
    List<Phase> phases = new ArrayList<>();
    // Make a copy of the list to avoid ConcurrentModificationException.
    List<MonitoredTask.StatusJournalEntry> journalEntries =
      ImmutableList.copyOf(this.monitoredTask.getStatusJournal());
    for (MonitoredTask.StatusJournalEntry entry: journalEntries) {
      Phase phase = new Phase(entry.getStatus());
      phase.setStartTime(formatter.format(Instant.ofEpochMilli(entry.getStartTimeStamp())));
      if (entry.getEndTimeStamp() == Long.MAX_VALUE) {
        // This means this task is still RUNNING.
        phase.setStatus(RUNNING);
        phase.setEndTime("-");
      } else {
        phase.setStatus(COMPLETE);
        phase.setEndTime(formatter.format(Instant.ofEpochMilli(entry.getEndTimeStamp())));
      }
      long elapsedTime = calculateElapsedTime(entry);
      phase.setElapsedTime(convertElapsedTimeHumanReadableTime(elapsedTime));
      phases.add(phase);
    }
    return phases;
  }

  /**
   * Calculate the elapsedTime. If this status is complete then return diff of start and end time.
   * If the status is still running, then return diff of current time and start time.
   * @param entry journal entry
   * @return elapsed time.
   */
  private long calculateElapsedTime(MonitoredTask.StatusJournalEntry entry) {
    if (entry.getEndTimeStamp() != Long.MAX_VALUE) {
      return entry.getEndTimeStamp() - entry.getStartTimeStamp();
    } else {
      return currentTime() - entry.getStartTimeStamp();
    }
  }

  /**
   * Convert the elapsedTime to human readable form (i.e 1 hour 10 mins 20 secs 10 ms)
   * @param elapsedTime elapsedTime
   * @return human readable format
   */
  private String convertElapsedTimeHumanReadableTime(long elapsedTime) {
    if (elapsedTime == Long.MAX_VALUE) {
      return "-";
    }
    long hours, minutes, seconds, milliseconds;
    milliseconds = elapsedTime % 1000;
    elapsedTime /= 1000;
    seconds = elapsedTime % 60;
    elapsedTime /= 60;
    minutes = elapsedTime % 60;
    elapsedTime /= 60;
    hours = elapsedTime % 24;
    StringBuilder sb = new StringBuilder();
    if (hours != 0) {
      sb.append(hours + " hours ");
    }
    if (minutes != 0) {
      sb.append(minutes + " mins ");
    }
    if (seconds != 0) {
      sb.append(seconds + " secs ");
    }
    sb.append(milliseconds + " ms ");
    return sb.toString();
  }
}
