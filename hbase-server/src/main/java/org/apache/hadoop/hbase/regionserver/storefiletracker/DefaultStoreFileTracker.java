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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The default implementation for store file tracker, where we do not persist the store file list,
 * and use listing when loading store files.
 */
@InterfaceAudience.Private
class DefaultStoreFileTracker extends StoreFileTrackerBase {

  public DefaultStoreFileTracker(Configuration conf, boolean isPrimaryReplica, StoreContext ctx) {
    super(conf, isPrimaryReplica, ctx);
  }

  @Override
  public boolean requireWritingToTmpDirFirst() {
    return true;
  }

  @Override
  protected void doAddNewStoreFiles(Collection<StoreFileInfo> newFiles) throws IOException {
    // NOOP
  }

  @Override
  protected void doAddCompactionResults(Collection<StoreFileInfo> compactedFiles,
    Collection<StoreFileInfo> newFiles) throws IOException {
    // NOOP
  }

  @Override
  protected List<StoreFileInfo> doLoadStoreFiles(boolean readOnly) throws IOException {
    List<StoreFileInfo> files =
      ctx.getRegionFileSystem().getStoreFiles(ctx.getFamily().getNameAsString());
    return files != null ? files : Collections.emptyList();
  }

  @Override
  protected void doSetStoreFiles(Collection<StoreFileInfo> files) throws IOException {
  }
}
