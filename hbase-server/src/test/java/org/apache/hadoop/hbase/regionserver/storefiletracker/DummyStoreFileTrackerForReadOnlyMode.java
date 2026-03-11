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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyStoreFileTrackerForReadOnlyMode extends StoreFileTrackerBase {
  private static final Logger LOG =
    LoggerFactory.getLogger(DummyStoreFileTrackerForReadOnlyMode.class);

  private boolean readOnlyUsed = false;
  private boolean compactionExecuted = false;
  private boolean addExecuted = false;
  private boolean setExecuted = false;

  private static StoreContext buildStoreContext(Configuration conf, TableName tableName) {
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).build();
    HRegionFileSystem hfs = Mockito.mock(HRegionFileSystem.class);
    try {
      Mockito.when(hfs.getRegionInfo()).thenReturn(regionInfo);
      Mockito.when(hfs.getFileSystem()).thenReturn(FileSystem.get(conf));
    } catch (IOException e) {
      LOG.error("Failed to get FileSystem for StoreContext creation", e);
    }
    return StoreContext.getBuilder().withRegionFileSystem(hfs).build();
  }

  public DummyStoreFileTrackerForReadOnlyMode(Configuration conf, boolean isPrimaryReplica,
    TableName tableName) {
    super(conf, isPrimaryReplica, buildStoreContext(conf, tableName));
  }

  @Override
  protected void doAddCompactionResults(Collection<StoreFileInfo> compactedFiles,
    Collection<StoreFileInfo> newFiles) {
    compactionExecuted = true;
  }

  @Override
  protected void doSetStoreFiles(Collection<StoreFileInfo> files) throws IOException {
    setExecuted = true;
  }

  @Override
  protected List<StoreFileInfo> doLoadStoreFiles(boolean readOnly) {
    readOnlyUsed = readOnly;
    return Collections.emptyList();
  }

  @Override
  protected void doAddNewStoreFiles(Collection<StoreFileInfo> newFiles) throws IOException {
    addExecuted = true;
  }

  boolean wasReadOnlyLoad() {
    return readOnlyUsed;
  }

  boolean wasCompactionExecuted() {
    return compactionExecuted;
  }

  boolean wasAddExecuted() {
    return addExecuted;
  }

  boolean wasSetExecuted() {
    return setExecuted;
  }

  @Override
  public boolean requireWritingToTmpDirFirst() {
    return false;
  }
}
