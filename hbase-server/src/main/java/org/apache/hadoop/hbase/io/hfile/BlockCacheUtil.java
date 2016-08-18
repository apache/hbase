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
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.stats.Snapshot;

/**
 * Utilty for aggregating counts in CachedBlocks and toString/toJSON CachedBlocks and BlockCaches.
 * No attempt has been made at making this thread safe.
 */
@InterfaceAudience.Private
public class BlockCacheUtil {

  public static final long NANOS_PER_SECOND = 1000000000;
  /**
   * Needed making histograms.
   */
  private static final MetricsRegistry METRICS = new MetricsRegistry();

  /**
   * Needed generating JSON.
   */
  private static final ObjectMapper MAPPER = new ObjectMapper();
  static {
    MAPPER.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
    MAPPER.configure(SerializationConfig.Feature.FLUSH_AFTER_WRITE_VALUE, true);
    MAPPER.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
  }

  /**
   * @param cb
   * @return The block content as String.
   */
  public static String toString(final CachedBlock cb, final long now) {
    return "filename=" + cb.getFilename() + ", " + toStringMinusFileName(cb, now);
  }

  /**
   * Little data structure to hold counts for a file.
   * Used doing a toJSON.
   */
  static class CachedBlockCountsPerFile {
    private int count = 0;
    private long size = 0;
    private int countData = 0;
    private long sizeData = 0;
    private final String filename;

    CachedBlockCountsPerFile(final String filename) {
      this.filename = filename;
    }

    public int getCount() {
      return count;
    }

    public long getSize() {
      return size;
    }

    public int getCountData() {
      return countData;
    }

    public long getSizeData() {
      return sizeData;
    }

    public String getFilename() {
      return filename;
    }
  }

  /**
   * @param filename
   * @param blocks
   * @return A JSON String of <code>filename</code> and counts of <code>blocks</code>
   * @throws JsonGenerationException
   * @throws JsonMappingException
   * @throws IOException
   */
  public static String toJSON(final String filename, final NavigableSet<CachedBlock> blocks)
  throws JsonGenerationException, JsonMappingException, IOException {
    CachedBlockCountsPerFile counts = new CachedBlockCountsPerFile(filename);
    for (CachedBlock cb: blocks) {
      counts.count++;
      counts.size += cb.getSize();
      BlockType bt = cb.getBlockType();
      if (bt != null && bt.isData()) {
        counts.countData++;
        counts.sizeData += cb.getSize();
      }
    }
    return MAPPER.writeValueAsString(counts);
  }

  /**
   * @param cbsbf
   * @return JSON string of <code>cbsf</code> aggregated
   * @throws JsonGenerationException
   * @throws JsonMappingException
   * @throws IOException
   */
  public static String toJSON(final CachedBlocksByFile cbsbf)
  throws JsonGenerationException, JsonMappingException, IOException {
    return MAPPER.writeValueAsString(cbsbf);
  }

  /**
   * @param bc
   * @return JSON string of <code>bc</code> content.
   * @throws JsonGenerationException
   * @throws JsonMappingException
   * @throws IOException
   */
  public static String toJSON(final BlockCache bc)
  throws JsonGenerationException, JsonMappingException, IOException {
    return MAPPER.writeValueAsString(bc);
  }

  /**
   * @param cb
   * @return The block content of <code>bc</code> as a String minus the filename.
   */
  public static String toStringMinusFileName(final CachedBlock cb, final long now) {
    return "offset=" + cb.getOffset() +
      ", size=" + cb.getSize() +
      ", age=" + (now - cb.getCachedTime()) +
      ", type=" + cb.getBlockType() +
      ", priority=" + cb.getBlockPriority();
  }

  /**
   * Get a {@link CachedBlocksByFile} instance and load it up by iterating content in
   * {@link BlockCache}.
   * @param conf Used to read configurations
   * @param bc Block Cache to iterate.
   * @return Laoded up instance of CachedBlocksByFile
   */
  public static CachedBlocksByFile getLoadedCachedBlocksByFile(final Configuration conf,
      final BlockCache bc) {
    CachedBlocksByFile cbsbf = new CachedBlocksByFile(conf);
    for (CachedBlock cb: bc) {
      if (cbsbf.update(cb)) break;
    }
    return cbsbf;
  }

  /**
   * Use one of these to keep a running account of cached blocks by file.  Throw it away when done.
   * This is different than metrics in that it is stats on current state of a cache.
   * See getLoadedCachedBlocksByFile
   */
  @JsonIgnoreProperties({"cachedBlockStatsByFile"})
  public static class CachedBlocksByFile {
    private int count;
    private int dataBlockCount;
    private long size;
    private long dataSize;
    private final long now = System.nanoTime();
    private final int max;
    public static final int DEFAULT_MAX = 100000;
 
    CachedBlocksByFile() {
      this(null);
    }

    CachedBlocksByFile(final Configuration c) {
      this.max = c == null? DEFAULT_MAX:
        c.getInt("hbase.ui.blockcache.by.file.max", DEFAULT_MAX);
    }

    /**
     * Map by filename. use concurent utils because we want our Map and contained blocks sorted.
     */
    private NavigableMap<String, NavigableSet<CachedBlock>> cachedBlockByFile =
      new ConcurrentSkipListMap<String, NavigableSet<CachedBlock>>();
    Histogram age = METRICS.newHistogram(CachedBlocksByFile.class, "age");

    /**
     * @param cb
     * @return True if full.... if we won't be adding any more.
     */
    public boolean update(final CachedBlock cb) {
      if (isFull()) return true;
      NavigableSet<CachedBlock> set = this.cachedBlockByFile.get(cb.getFilename());
      if (set == null) {
        set = new ConcurrentSkipListSet<CachedBlock>();
        this.cachedBlockByFile.put(cb.getFilename(), set);
      }
      set.add(cb);
      this.size += cb.getSize();
      this.count++;
      BlockType bt = cb.getBlockType();
      if (bt != null && bt.isData()) {
        this.dataBlockCount++;
        this.dataSize += cb.getSize();
      }
      long age = (this.now - cb.getCachedTime())/NANOS_PER_SECOND;
      this.age.update(age);
      return false;
    }

    /**
     * @return True if full; i.e. there are more items in the cache but we only loaded up
     * the maximum set in configuration <code>hbase.ui.blockcache.by.file.max</code>
     * (Default: DEFAULT_MAX).
     */
    public boolean isFull() {
      return this.count >= this.max;
    }
 
    public NavigableMap<String, NavigableSet<CachedBlock>> getCachedBlockStatsByFile() {
      return this.cachedBlockByFile;
    }

    /**
     * @return count of blocks in the cache
     */
    public int getCount() {
      return count;
    }

    public int getDataCount() {
      return dataBlockCount;
    }

    /**
     * @return size of blocks in the cache
     */
    public long getSize() {
      return size;
    }

    /**
     * @return Size of data.
     */
    public long getDataSize() {
      return dataSize;
    }

    public AgeSnapshot getAgeInCacheSnapshot() {
      return new AgeSnapshot(this.age);
    }

    @Override
    public String toString() {
      Snapshot snapshot = this.age.getSnapshot();
      return "count=" + count + ", dataBlockCount=" + this.dataBlockCount + ", size=" + size +
          ", dataSize=" + getDataSize() +
          ", mean age=" + this.age.mean() + ", stddev age=" + this.age.stdDev() +
          ", min age=" + this.age.min() + ", max age=" + this.age.max() +
          ", 95th percentile age=" + snapshot.get95thPercentile() +
          ", 99th percentile age=" + snapshot.get99thPercentile();
    }
  }
}
