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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStoreFileTracker extends DefaultStoreFileTracker {

  private static final Logger LOG = LoggerFactory.getLogger(TestStoreFileTracker.class);
  public static Map<String, List<StoreFileInfo>> trackedFiles = new HashMap<>();
  private String storeId;

  public TestStoreFileTracker(Configuration conf, boolean isPrimaryReplica, StoreContext ctx) {
    super(conf, isPrimaryReplica, ctx);
    this.storeId = ctx.getRegionInfo().getEncodedName() + "-" + ctx.getFamily().getNameAsString();
    LOG.info("created storeId: {}", storeId);
    trackedFiles.computeIfAbsent(storeId, v -> new ArrayList<>());
  }

  @Override
  protected void doAddNewStoreFiles(Collection<StoreFileInfo> newFiles) throws IOException {
    LOG.info("adding to storeId: {}", storeId);
    trackedFiles.get(storeId).addAll(newFiles);
  }

  @Override
  public List<StoreFileInfo> load() throws IOException {
    return trackedFiles.get(storeId);
  }
}
