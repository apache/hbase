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
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;

public class DummyStoreFileTrackerForReadOnlyMode extends StoreFileTrackerBase {
  private boolean readOnlyUsed = false;
  private boolean compactionExecuted = false;
  private boolean addExecuted = false;
  private boolean setExecuted = false;

  public DummyStoreFileTrackerForReadOnlyMode(Configuration conf, boolean isPrimaryReplica) {
    super(conf, isPrimaryReplica, null);
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
