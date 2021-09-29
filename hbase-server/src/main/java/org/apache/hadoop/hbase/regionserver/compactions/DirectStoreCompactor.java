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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alternative Compactor implementation that writes compacted files straight
 * into the store directory.
 *
 * This class extends <code>DefaultCompactor</code> class,
 * modifying original behaviour of <code>initWriter</code> and <code>createFileInStoreDir</code>
 * methods to create compacted files in the final store directory, rather than temp, avoid the
 * need to perform renames at compaction commit time.
 */
@InterfaceAudience.Private
public class DirectStoreCompactor extends DefaultCompactor {
  private static final Logger LOG = LoggerFactory.getLogger(DirectStoreCompactor.class);

  private final List<Path> compactionPaths = new ArrayList<>();

  public DirectStoreCompactor(Configuration conf, HStore store) {
    super(conf, store);
  }

  /**
   * Overrides <code>Compactor</code> original implementation to create the resulting file directly
   * in the store dir, rather than temp, in order to avoid the need for rename at commit time.
   * @param fd the file details.
   * @param shouldDropBehind boolean for the drop-behind output stream cache settings.
   * @param major if compaction is major.
   * @return an instance of StoreFileWriter for the given file details.
   * @throws IOException if any error occurs.
   */
  @Override
  protected StoreFileWriter initWriter(FileDetails fd, boolean shouldDropBehind, boolean major)
    throws IOException {
    // When all MVCC readpoints are 0, don't write them.
    // See HBASE-8166, HBASE-12600, and HBASE-13389.
    StoreFileWriter writer = createWriterInStoreDir(fd.maxKeyCount,
      major ? majorCompactionCompression : minorCompactionCompression,
      fd.maxMVCCReadpoint > 0, fd.maxTagsLength > 0,
      shouldDropBehind, fd.getTotalCompactedFilesSize());
    synchronized (compactionPaths){
        compactionPaths.add(writer.getPath());
      }
    return writer;
  }

  private StoreFileWriter createWriterInStoreDir(long maxKeyCount,
      Compression.Algorithm compression, boolean includeMVCCReadpoint, boolean includesTag,
        boolean shouldDropBehind, long totalCompactedFilesSize) throws IOException {
    final CacheConfig writerCacheConf = new CacheConfig(store.getCacheConfig());
    writerCacheConf.enableCacheOnWriteForCompactions(totalCompactedFilesSize);
    InetSocketAddress[] favoredNodes = store.getStoreContext().getFavoredNodes();
    HFileContext hFileContext = store.getStoreEngine().
      createFileContext(compression, includeMVCCReadpoint,includesTag,
        store.getStoreContext().getEncryptionContext(), store.getColumnFamilyDescriptor(),
        store.getTableName(), store.getComparator(), conf);
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
   * @param fileProvider a lambda expression with logic for loading a HStoreFile given a Path.
   * @return HStoreFile reference for the newly created file.
   * @throws IOException if any error occurs.
   */
  @Override
  protected HStoreFile createFileInStoreDir(Path newFile, StoreFileProvider fileProvider)
      throws IOException {
    return fileProvider.createFile(newFile);
  }

  /**
   * Removes finalized or deleted compaction target files from current list
   * @param result
   */
  @Override public void filesDone(Collection<Path> result) {
    synchronized (compactionPaths) {
      compactionPaths.removeAll(result);
      //this should not happen
      if (!compactionPaths.isEmpty()) {
        LOG.warn("Unfinished compaction files: {}", StringUtils.join(compactionPaths, ", "));
        compactionPaths.clear();
      }
    }
  }

  public List<Path> getCompactionPaths(){
    return Lists.newArrayList(compactionPaths);
  }
}
