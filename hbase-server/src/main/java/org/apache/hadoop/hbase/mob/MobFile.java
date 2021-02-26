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
package org.apache.hadoop.hbase.mob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The mob file.
 */
@InterfaceAudience.Private
public class MobFile {

  private HStoreFile sf;

  // internal use only for sub classes
  protected MobFile() {
  }

  protected MobFile(HStoreFile sf) {
    this.sf = sf;
  }

  /**
   * Internal use only. This is used by the sweeper.
   *
   * @return The store file scanner.
   * @throws IOException
   */
  public StoreFileScanner getScanner() throws IOException {
    List<HStoreFile> sfs = new ArrayList<>();
    sfs.add(sf);
    List<StoreFileScanner> sfScanners = StoreFileScanner.getScannersForStoreFiles(sfs, false, true,
        false, false, sf.getMaxMemStoreTS());

    return sfScanners.get(0);
  }

  /**
   * Reads a cell from the mob file.
   * @param search The cell need to be searched in the mob file.
   * @param cacheMobBlocks Should this scanner cache blocks.
   * @return The cell in the mob file.
   * @throws IOException
   */
  public MobCell readCell(Cell search, boolean cacheMobBlocks) throws IOException {
    return readCell(search, cacheMobBlocks, sf.getMaxMemStoreTS());
  }

  /**
   * Reads a cell from the mob file.
   * @param search The cell need to be searched in the mob file.
   * @param cacheMobBlocks Should this scanner cache blocks.
   * @param readPt the read point.
   * @return The cell in the mob file.
   * @throws IOException
   */
  public MobCell readCell(Cell search, boolean cacheMobBlocks, long readPt) throws IOException {
    StoreFileScanner scanner = null;
    boolean succ = false;
    try {
      List<StoreFileScanner> sfScanners = StoreFileScanner.getScannersForStoreFiles(
        Collections.singletonList(sf), cacheMobBlocks, true, false, false, readPt);
      if (!sfScanners.isEmpty()) {
        scanner = sfScanners.get(0);
        if (scanner.seek(search)) {
          MobCell mobCell = new MobCell(scanner.peek(), scanner);
          succ = true;
          return mobCell;
        }
      }
      return null;
    } finally {
      if (scanner != null && !succ) {
        scanner.close();
      }
    }
  }

  /**
   * Gets the file name.
   * @return The file name.
   */
  public String getFileName() {
    return sf.getPath().getName();
  }

  /**
   * Opens the underlying reader.
   * It's not thread-safe. Use MobFileCache.openFile() instead.
   * @throws IOException
   */
  public void open() throws IOException {
    sf.initReader();
  }

  /**
   * Closes the underlying reader, but do no evict blocks belonging to this file.
   * It's not thread-safe. Use MobFileCache.closeFile() instead.
   * @throws IOException
   */
  public void close() throws IOException {
    if (sf != null) {
      sf.closeStoreFile(false);
      sf = null;
    }
  }

  /**
   * Creates an instance of the MobFile.
   * @param fs The file system.
   * @param path The path of the underlying StoreFile.
   * @param conf The configuration.
   * @param cacheConf The CacheConfig.
   * @return An instance of the MobFile.
   * @throws IOException
   */
  public static MobFile create(FileSystem fs, Path path, Configuration conf, CacheConfig cacheConf)
      throws IOException {
    // XXX: primaryReplica is only used for constructing the key of block cache so it is not a
    // critical problem if we pass the wrong value, so here we always pass true. Need to fix later.
    HStoreFile sf = new HStoreFile(fs, path, conf, cacheConf, BloomType.NONE, true);
    return new MobFile(sf);
  }
}
