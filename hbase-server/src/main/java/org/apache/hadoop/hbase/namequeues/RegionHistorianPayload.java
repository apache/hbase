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
public class RegionHistorianPayload extends NamedQueuePayload {
  public static final int REGION_HISTORIAN_EVENT = 4;
  private final String hostName;
  private final String regionName;
  private final String tableName;
  private final String eventType;
  private final long eventTimestamp;
  private final Long pid;
  private final Long ppid;


  public RegionHistorianPayload(String hostName, String regionName, String tableName, String eventType, long eventTimestamp, Long pid, Long ppid) {
    super(NamedQueueEvent.REGION_HISTORIAN.getValue());
    this.hostName = hostName;
    this.regionName = regionName;
    this.tableName = tableName;
    this.eventType = eventType;
    this.eventTimestamp = eventTimestamp;
    this.pid = pid;
    this.ppid = ppid;
  }


  public String getHostname() { return hostName; }
  public String getRegionName() { return regionName; }
  public String getTableName() { return tableName; }
  public String getEventType() { return eventType; }
  public long getEventTimestamp() { return eventTimestamp; }
  public long getPid() { return pid; }
  public long getPpid() { return ppid; }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(this.getClass().getSimpleName());
    sb.append(" [");
    sb.append("hostName=").append(hostName);
    sb.append(" regionID=").append(regionName);
    sb.append(" tableName=").append(tableName);
    sb.append(" eventType=").append(eventType);
    sb.append(" eventTimestamp=").append(eventTimestamp);
    sb.append(" pid=").append(pid);
    sb.append(" ppid=").append(ppid);
    sb.append("] ");
    return sb.toString();
  }
}
