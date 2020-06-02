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
package org.apache.hadoop.hbase.master.region;

import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The parameters for constructing {@link MasterRegion}.
 */
@InterfaceAudience.Private
public class MasterRegionParams {

  private Server server;

  private String regionDirName;

  private TableDescriptor tableDescriptor;

  private Long flushSize;

  private Long flushPerChanges;

  private Long flushIntervalMs;

  private Integer compactMin;

  private Integer maxWals;

  private Boolean useHsync;

  private Integer ringBufferSlotCount;

  private Long rollPeriodMs;

  private String archivedWalSuffix;

  private String archivedHFileSuffix;

  private Boolean useMetaCellComparator;

  public MasterRegionParams server(Server server) {
    this.server = server;
    return this;
  }

  public MasterRegionParams regionDirName(String regionDirName) {
    this.regionDirName = regionDirName;
    return this;
  }

  public MasterRegionParams tableDescriptor(TableDescriptor tableDescriptor) {
    this.tableDescriptor = tableDescriptor;
    return this;
  }

  public MasterRegionParams flushSize(long flushSize) {
    this.flushSize = flushSize;
    return this;
  }

  public MasterRegionParams flushPerChanges(long flushPerChanges) {
    this.flushPerChanges = flushPerChanges;
    return this;
  }

  public MasterRegionParams flushIntervalMs(long flushIntervalMs) {
    this.flushIntervalMs = flushIntervalMs;
    return this;
  }

  public MasterRegionParams compactMin(int compactMin) {
    this.compactMin = compactMin;
    return this;
  }

  public MasterRegionParams maxWals(int maxWals) {
    this.maxWals = maxWals;
    return this;
  }

  public MasterRegionParams useHsync(boolean useHsync) {
    this.useHsync = useHsync;
    return this;
  }

  public MasterRegionParams ringBufferSlotCount(int ringBufferSlotCount) {
    this.ringBufferSlotCount = ringBufferSlotCount;
    return this;
  }

  public MasterRegionParams rollPeriodMs(long rollPeriodMs) {
    this.rollPeriodMs = rollPeriodMs;
    return this;
  }

  public MasterRegionParams archivedWalSuffix(String archivedWalSuffix) {
    this.archivedWalSuffix = archivedWalSuffix;
    return this;
  }

  public MasterRegionParams archivedHFileSuffix(String archivedHFileSuffix) {
    this.archivedHFileSuffix = archivedHFileSuffix;
    return this;
  }

  public MasterRegionParams useMetaCellComparator(boolean useMetaCellComparator) {
    this.useMetaCellComparator = useMetaCellComparator;
    return this;
  }

  public Server server() {
    return server;
  }

  public String regionDirName() {
    return regionDirName;
  }

  public TableDescriptor tableDescriptor() {
    return tableDescriptor;
  }

  public long flushSize() {
    return flushSize;
  }

  public long flushPerChanges() {
    return flushPerChanges;
  }

  public long flushIntervalMs() {
    return flushIntervalMs;
  }

  public int compactMin() {
    return compactMin;
  }

  public int maxWals() {
    return maxWals;
  }

  public Boolean useHsync() {
    return useHsync;
  }

  public int ringBufferSlotCount() {
    return ringBufferSlotCount;
  }

  public long rollPeriodMs() {
    return rollPeriodMs;
  }

  public String archivedWalSuffix() {
    return archivedWalSuffix;
  }

  public String archivedHFileSuffix() {
    return archivedHFileSuffix;
  }

  public Boolean useMetaCellComparator() {
    return useMetaCellComparator;
  }
}
