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

package org.apache.hadoop.hbase.util.compaction;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

/**
 * This request helps determine if a region has to be compacted based on table's TTL.
 */
@InterfaceAudience.Private
public class MajorCompactionTTLRequest extends MajorCompactionRequest {

  private static final Logger LOG = LoggerFactory.getLogger(MajorCompactionTTLRequest.class);

  MajorCompactionTTLRequest(Connection connection, RegionInfo region) {
    super(connection, region);
  }

  static Optional<MajorCompactionRequest> newRequest(Connection connection, RegionInfo info,
      TableDescriptor htd) throws IOException {
    MajorCompactionTTLRequest request = new MajorCompactionTTLRequest(connection, info);
    return request.createRequest(connection, htd);
  }

  private Optional<MajorCompactionRequest> createRequest(Connection connection, TableDescriptor htd)
      throws IOException {
    Map<String, Long> familiesToCompact = getStoresRequiringCompaction(htd);
    MajorCompactionRequest request = null;
    if (!familiesToCompact.isEmpty()) {
      LOG.debug("Compaction families for region: " + region + " CF: " + familiesToCompact.keySet());
      request = new MajorCompactionTTLRequest(connection, region);
    }
    return Optional.ofNullable(request);
  }

  Map<String, Long> getStoresRequiringCompaction(TableDescriptor htd) throws IOException {
    HRegionFileSystem fileSystem = getFileSystem();
    Map<String, Long> familyTTLMap = Maps.newHashMap();
    for (ColumnFamilyDescriptor descriptor : htd.getColumnFamilies()) {
      long ts = getColFamilyCutoffTime(descriptor);
      // If the table's TTL is forever, lets not compact any of the regions.
      if (ts > 0 && shouldCFBeCompacted(fileSystem, descriptor.getNameAsString(), ts)) {
        familyTTLMap.put(descriptor.getNameAsString(), ts);
      }
    }
    return familyTTLMap;
  }

  // If the CF has no TTL, return -1, else return the current time - TTL.
  private long getColFamilyCutoffTime(ColumnFamilyDescriptor colDesc) {
    if (colDesc.getTimeToLive() == HConstants.FOREVER) {
      return -1;
    }
    return System.currentTimeMillis() - (colDesc.getTimeToLive() * 1000L);
  }

  @Override
  protected boolean shouldIncludeStore(HRegionFileSystem fileSystem, String family,
      Collection<StoreFileInfo> storeFiles, long ts) throws IOException {

    for (StoreFileInfo storeFile : storeFiles) {
      // Lets only compact when all files are older than TTL
      if (storeFile.getModificationTime() >= ts) {
        LOG.info("There is atleast one file in store: " + family + " file: " + storeFile.getPath()
            + " with timestamp " + storeFile.getModificationTime()
            + " for region: " + fileSystem.getRegionInfo().getEncodedName()
            + " older than TTL: " + ts);
        return false;
      }
    }
    return true;
  }
}
