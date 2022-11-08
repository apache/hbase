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
package org.apache.hadoop.hbase.namequeues;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class WALEventTrackerPayload extends NamedQueuePayload {

  private final String rsName;
  private final String walName;
  private final long timeStamp;
  private final String state;
  private final long walLength;

  public WALEventTrackerPayload(String rsName, String walName, long timeStamp, String state,
    long walLength) {
    super(NamedQueueEvent.WAL_EVENT_TRACKER.getValue());
    this.rsName = rsName;
    this.walName = walName;
    this.timeStamp = timeStamp;
    this.state = state;
    this.walLength = walLength;
  }

  public String getRsName() {
    return rsName;
  }

  public String getWalName() {
    return walName;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public String getState() {
    return state;
  }

  public long getWalLength() {
    return walLength;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(this.getClass().getSimpleName());
    sb.append("[");
    sb.append("rsName=").append(rsName);
    sb.append(", walName=").append(walName);
    sb.append(", timeStamp=").append(timeStamp);
    sb.append(", walState=").append(state);
    sb.append(", walLength=").append(walLength);
    sb.append("]");
    return sb.toString();
  }
}
