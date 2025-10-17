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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default implementation for store file tracker, where we do not persist the store file list,
 * and use listing when loading store files.
 */
@InterfaceAudience.Private
class DefaultStoreFileTracker extends StoreFileTrackerBase {

  public DefaultStoreFileTracker(Configuration conf, boolean isPrimaryReplica, StoreContext ctx) {
    super(conf, isPrimaryReplica, ctx);
  }

  private static final Logger LOG = LoggerFactory.getLogger(DefaultStoreFileTracker.class);

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
    List<StoreFileInfo> files = getStoreFiles(ctx.getFamily().getNameAsString());
    return files != null ? files : Collections.emptyList();
  }

  @Override
  protected void doSetStoreFiles(Collection<StoreFileInfo> files) throws IOException {
  }

  /**
   * Returns the store files available for the family. This methods performs the filtering based on
   * the valid store files.
   * @param familyName Column Family Name
   * @return a set of {@link StoreFileInfo} for the specified family.
   */
  public List<StoreFileInfo> getStoreFiles(final String familyName) throws IOException {
    Path familyDir = ctx.getRegionFileSystem().getStoreDir(familyName);
    FileStatus[] files =
      CommonFSUtils.listStatus(ctx.getRegionFileSystem().getFileSystem(), familyDir);
    if (files == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No StoreFiles for: " + familyDir);
      }
      return null;
    }

    ArrayList<StoreFileInfo> storeFiles = new ArrayList<>(files.length);
    for (FileStatus status : files) {
      if (!StoreFileInfo.isValid(status)) {
        // recovered.hfiles directory is expected inside CF path when
        // hbase.wal.split.to.hfile to
        // true, refer HBASE-23740
        if (!HConstants.RECOVERED_HFILES_DIR.equals(status.getPath().getName())) {
          LOG.warn("Invalid StoreFile: {}", status.getPath());
        }
        continue;
      }
      StoreFileInfo info = ServerRegionReplicaUtil.getStoreFileInfo(conf,
        ctx.getRegionFileSystem().getFileSystem(), ctx.getRegionInfo(),
        ctx.getRegionFileSystem().getRegionInfoForFS(), familyName, status.getPath(), this);
      storeFiles.add(info);

    }
    return storeFiles;
  }
}
