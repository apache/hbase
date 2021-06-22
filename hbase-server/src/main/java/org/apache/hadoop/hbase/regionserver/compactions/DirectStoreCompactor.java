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

package org.apache.hadoop.hbase.regionserver.compactions;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class DirectStoreCompactor extends DefaultCompactor {
  public DirectStoreCompactor(Configuration conf, HStore store) {
    super(conf, store);
  }

  @Override
  protected StoreFileWriter initWriter(FileDetails fd, boolean shouldDropBehind, boolean major)
    throws IOException {
    // When all MVCC readpoints are 0, don't write them.
    // See HBASE-8166, HBASE-12600, and HBASE-13389.
    return createWriterInFamilyDir(fd.maxKeyCount,
      major ? majorCompactionCompression : minorCompactionCompression,
      fd.maxMVCCReadpoint > 0, fd.maxTagsLength > 0,
      shouldDropBehind, fd.totalCompactedFilesSize);
  }

  private StoreFileWriter createWriterInFamilyDir(long maxKeyCount,
      Compression.Algorithm compression, boolean includeMVCCReadpoint, boolean includesTag,
        boolean shouldDropBehind, long totalCompactedFilesSize) throws IOException {
    final CacheConfig writerCacheConf;
    // Don't cache data on write on compactions.
    writerCacheConf = new CacheConfig(store.getCacheConfig());
    writerCacheConf.enableCacheOnWrite(totalCompactedFilesSize);
    InetSocketAddress[] favoredNodes = null;
    if (store.getHRegion().getRegionServerServices() != null) {
      favoredNodes = store.getHRegion().getRegionServerServices().getFavoredNodesForRegion(
        store.getHRegion().getRegionInfo().getEncodedName());
    }
    HFileContext hFileContext = store.createFileContext(compression, includeMVCCReadpoint,
      includesTag, store.getCryptoContext());
    Path familyDir = new Path(store.getRegionFileSystem().getRegionDir(),
      store.getColumnFamilyDescriptor().getNameAsString());
    StoreFileWriter.Builder builder = new StoreFileWriter.Builder(conf, writerCacheConf,
      store.getFileSystem())
      .withOutputDir(familyDir)
      .withBloomType(store.getColumnFamilyDescriptor().getBloomFilterType())
      .withMaxKeyCount(maxKeyCount)
      .withFavoredNodes(favoredNodes)
      .withFileContext(hFileContext)
      .withShouldDropCacheBehind(shouldDropBehind)
      .withCompactedFilesSupplier(() -> store.getCompactedFiles());
    return builder.build();
  }

  /**
   * Overrides Compactor original implementation, assuming the passed file is already in the store
   * directory, thus it only creates the related HStoreFile for the passed Path.
   * @param newFile the new file created.
   * @return
   * @throws IOException
   */
  @Override
  protected HStoreFile createFileInStoreDir(Path newFile) throws IOException {
    return this.store.createStoreFileAndReader(newFile);
  }
}
