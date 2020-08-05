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
package org.apache.hadoop.hbase.monitoring;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.gson.Gson;

@InterfaceAudience.Private
class MonitoredTaskImpl implements MonitoredTask {
  private long startTime;
  private long statusTime;
  private long stateTime;
  private long warnTime;

  private volatile String status;
  private volatile String description;
  
  protected volatile State state = State.RUNNING;

  private boolean journalEnabled = false;
  private List<StatusJournalEntry> journal;

  private static final Gson GSON = GsonUtil.createGson().create();

  public MonitoredTaskImpl() {
    startTime = System.currentTimeMillis();
    statusTime = startTime;
    stateTime = startTime;
    warnTime = startTime;
  }

  private static class StatusJournalEntryImpl implements StatusJournalEntry {
    private long statusTime;
    private String status;

    public StatusJournalEntryImpl(String status, long statusTime) {
      this.status = status;
      this.statusTime = statusTime;
    }

    @Override
    public String getStatus() {
      return status;
    }

    @Override
    public long getTimeStamp() {
      return statusTime;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(status);
      sb.append(" at ");
      sb.append(statusTime);
      return sb.toString();
    }
  }

  @Override
  public synchronized MonitoredTaskImpl clone() {
    try {
      return (MonitoredTaskImpl) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new AssertionError(); // Won't happen
    }
  }

  @Override
  public long getStartTime() {
    return startTime;
  }
  
  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public String getStatus() {
    return status;
  }

  @Override
  public long getStatusTime() {
    return statusTime;
  }
  
  @Override
  public State getState() {
    return state;
  }
  
  @Override
  public long getStateTime() {
    return stateTime;
  }

  @Override
  public long getWarnTime() {
    return warnTime;
  }

  @Override
  public long getCompletionTimestamp() {
    if (state == State.COMPLETE || state == State.ABORTED) {
      return stateTime;
    }
    return -1;
  }

  @Override
  public void markComplete(String status) {
    setState(State.COMPLETE);
    setStatus(status);
  }

  @Override
  public void pause(String msg) {
    setState(State.WAITING);
    setStatus(msg);
  }

  @Override
  public void resume(String msg) {
    setState(State.RUNNING);
    setStatus(msg);
  }

  @Override
  public void abort(String msg) {
    setStatus(msg);
    setState(State.ABORTED);
  }
  
  @Override
  public void setStatus(String status) {
    this.status = status;
    statusTime = System.currentTimeMillis();
    if (journalEnabled) {
      journal.add(new StatusJournalEntryImpl(this.status, statusTime));
    }
  }

  protected void setState(State state) {
    this.state = state;
    stateTime = System.currentTimeMillis();
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void setWarnTime(long t) {
    this.warnTime = t;
  }

  @Override
  public void cleanup() {
    if (state == State.RUNNING) {
      setState(State.ABORTED);
    }
  }

  /**
   * Force the completion timestamp backwards so that
   * it expires now.
   */
  @Override
  public void expireNow() {
    stateTime -= 180 * 1000;
  }

  @Override
  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("description", getDescription());
    map.put("status", getStatus());
    map.put("state", getState());
    map.put("starttimems", getStartTime());
    map.put("statustimems", getCompletionTimestamp());
    map.put("statetimems", getCompletionTimestamp());
    return map;
  }

  @Override
  public String toJSON() throws IOException {
    return GSON.toJson(toMap());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(512);
    sb.append(getDescription());
    sb.append(": status=");
    sb.append(getStatus());
    sb.append(", state=");
    sb.append(getState());
    sb.append(", startTime=");
    sb.append(getStartTime());
    sb.append(", completionTime=");
    sb.append(getCompletionTimestamp());
    return sb.toString();
  }

  /**
   * Returns the status journal. This implementation of status journal is not thread-safe. Currently
   * we use this to track various stages of flushes and compactions where we can use this/pretty
   * print for post task analysis, by which time we are already done changing states (writing to
   * journal)
   */
  @Override
  public List<StatusJournalEntry> getStatusJournal() {
    if (journal == null) {
      return Collections.emptyList();
    } else {
      return Collections.unmodifiableList(journal);
    }
  }

  /**
   * Enables journaling of this monitored task, the first invocation will lazily initialize the
   * journal. The journal implementation itself and this method are not thread safe
   */
  @Override
  public void enableStatusJournal(boolean includeCurrentStatus) {
    if (journalEnabled && journal != null) {
      return;
    }
    journalEnabled = true;
    if (journal == null) {
      journal = new ArrayList<StatusJournalEntry>();
    }
    if (includeCurrentStatus && status != null) {
      journal.add(new StatusJournalEntryImpl(status, statusTime));
    }
  }

  @Override
  public void disableStatusJournal() {
    journalEnabled = false;
  }

  @Override
  public String prettyPrintJournal() {
    if (!journalEnabled) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < journal.size(); i++) {
      StatusJournalEntry je = journal.get(i);
      sb.append(je.toString());
      if (i != 0) {
        StatusJournalEntry jep = journal.get(i-1);
        long delta = je.getTimeStamp() - jep.getTimeStamp();
        if (delta != 0) {
          sb.append(" (+" + delta + " ms)");
        }
      }
      sb.append("\n");
    }
    return sb.toString();
  }

}
