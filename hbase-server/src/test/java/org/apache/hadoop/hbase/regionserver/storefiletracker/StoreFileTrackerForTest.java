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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoreFileTrackerForTest extends DefaultStoreFileTracker {

  private static final Logger LOG = LoggerFactory.getLogger(StoreFileTrackerForTest.class);
  private static ConcurrentMap<String, BlockingQueue<StoreFileInfo>> trackedFiles =
    new ConcurrentHashMap<>();
  private String storeId;

  public StoreFileTrackerForTest(Configuration conf, boolean isPrimaryReplica, StoreContext ctx) {
    super(conf, isPrimaryReplica, ctx);
    if (ctx != null && ctx.getRegionFileSystem() != null) {
      this.storeId = ctx.getRegionInfo().getEncodedName() + "-" + ctx.getFamily().getNameAsString();
      LOG.info("created storeId: {}", storeId);
      trackedFiles.computeIfAbsent(storeId, v -> new LinkedBlockingQueue<>());
    } else {
      LOG.info("ctx.getRegionFileSystem() returned null. Leaving storeId null.");
    }
  }

  @Override
  protected void doAddNewStoreFiles(Collection<StoreFileInfo> newFiles) throws IOException {
    LOG.info("adding to storeId: {}", storeId);
    trackedFiles.get(storeId).addAll(newFiles);
  }

  @Override
  protected List<StoreFileInfo> doLoadStoreFiles(boolean readOnly) throws IOException {
    return new ArrayList<>(trackedFiles.get(storeId));
  }

  public static boolean tracked(String encodedRegionName, String family, Path file) {
    BlockingQueue<StoreFileInfo> files = trackedFiles.get(encodedRegionName + "-" + family);
    return files != null && files.stream().anyMatch(s -> s.getPath().equals(file));
  }

  public static void clear() {
    trackedFiles.clear();
  }
}
