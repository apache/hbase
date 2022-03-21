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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * A store file tracker used for migrating between store file tracker implementations.
 */
@InterfaceAudience.Private
class MigrationStoreFileTracker extends StoreFileTrackerBase {

  public static final String SRC_IMPL = "hbase.store.file-tracker.migration.src.impl";

  public static final String DST_IMPL = "hbase.store.file-tracker.migration.dst.impl";

  private final StoreFileTrackerBase src;

  private final StoreFileTrackerBase dst;

  public MigrationStoreFileTracker(Configuration conf, boolean isPrimaryReplica, StoreContext ctx) {
    super(conf, isPrimaryReplica, ctx);
    this.src = StoreFileTrackerFactory.createForMigration(conf, SRC_IMPL, isPrimaryReplica, ctx);
    this.dst = StoreFileTrackerFactory.createForMigration(conf, DST_IMPL, isPrimaryReplica, ctx);
    Preconditions.checkArgument(!src.getClass().equals(dst.getClass()),
      "src and dst is the same: %s", src.getClass());
  }

  @Override
  public boolean requireWritingToTmpDirFirst() {
    // Returns true if either of the two StoreFileTracker returns true.
    // For example, if we want to migrate from a tracker implementation which can ignore the broken
    // files under data directory to a tracker implementation which can not, if we still allow
    // writing in tmp directory directly, we may have some broken files under the data directory and
    // then after we finally change the implementation which can not ignore the broken files, we
    // will be in trouble.
    return src.requireWritingToTmpDirFirst() || dst.requireWritingToTmpDirFirst();
  }

  @Override
  protected List<StoreFileInfo> doLoadStoreFiles(boolean readOnly) throws IOException {
    List<StoreFileInfo> files = src.doLoadStoreFiles(readOnly);
    if (!readOnly) {
      dst.doSetStoreFiles(files);
    }
    return files;
  }

  @Override
  protected void doAddNewStoreFiles(Collection<StoreFileInfo> newFiles) throws IOException {
    src.doAddNewStoreFiles(newFiles);
    dst.doAddNewStoreFiles(newFiles);
  }

  @Override
  protected void doAddCompactionResults(Collection<StoreFileInfo> compactedFiles,
    Collection<StoreFileInfo> newFiles) throws IOException {
    src.doAddCompactionResults(compactedFiles, newFiles);
    dst.doAddCompactionResults(compactedFiles, newFiles);
  }

  @Override
  protected void doSetStoreFiles(Collection<StoreFileInfo> files) throws IOException {
    throw new UnsupportedOperationException(
      "Should not call this method on " + getClass().getSimpleName());
  }

  static Class<? extends StoreFileTracker> getSrcTrackerClass(Configuration conf) {
    return StoreFileTrackerFactory.getStoreFileTrackerClassForMigration(conf, SRC_IMPL);
  }

  static Class<? extends StoreFileTracker> getDstTrackerClass(Configuration conf) {
    return StoreFileTrackerFactory.getStoreFileTrackerClassForMigration(conf, DST_IMPL);
  }
}
