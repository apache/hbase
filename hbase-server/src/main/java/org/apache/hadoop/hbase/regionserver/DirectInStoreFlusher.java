/**
 *
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * A StoreFlusher that writes hfiles directly into the actual store directory,
 * instead of a temp dir..
 */
@InterfaceAudience.Private
public class DirectInStoreFlusher extends DefaultStoreFlusher {

  public DirectInStoreFlusher(Configuration conf, HStore store) {
    super(conf, store);
  }

  @Override
  public List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushId,
      MonitoredTask status, ThroughputController throughputController,
      FlushLifeCycleTracker tracker) throws IOException {
    return flushSnapshot(snapshot, cacheFlushId, status, throughputController, tracker,
      s -> createWriter(snapshot.getCellsCount(),
            s.getColumnFamilyDescriptor().getCompressionType(),
            snapshot.isTagsPresent()));
  }

  public StoreFileWriter createWriter(long maxKeyCount, Compression.Algorithm compression,
      boolean includesTag) throws IOException {
    Path familyDir = new Path(store.getRegionFileSystem().getRegionDir(),
      store.getColumnFamilyName());
    HFileContext hFileContext = store.createFileContext(compression, true,
      includesTag, store.getStoreContext().getEncryptionContext());

    StoreFileWriter.Builder builder = new StoreFileWriter.Builder(conf, store.getCacheConfig(),
      store.getFileSystem())
      .withOutputDir(familyDir)
      .withBloomType(store.getColumnFamilyDescriptor().getBloomFilterType())
      .withMaxKeyCount(maxKeyCount)
      .withFavoredNodes(getFavoredNodes(store.getHRegion()))
      .withFileContext(hFileContext)
      .withShouldDropCacheBehind(false)
      .withCompactedFilesSupplier(store::getCompactedFiles);
    return builder.build();
  }

  private InetSocketAddress[] getFavoredNodes(HRegion region){
    InetSocketAddress[] favoredNodes = null;
    if (region.getRegionServerServices() != null) {
      favoredNodes = region.getRegionServerServices().getFavoredNodesForRegion(
        region.getRegionInfo().getEncodedName());
    }
    return favoredNodes;
  }

}
