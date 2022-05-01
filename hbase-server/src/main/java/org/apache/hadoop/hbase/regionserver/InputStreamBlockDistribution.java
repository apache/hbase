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
package org.apache.hadoop.hbase.regionserver;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.io.FileLink;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes the HDFSBlockDistribution for a file based on the underlying located blocks for an
 * HdfsDataInputStream reading that file. The backing DFSInputStream.getAllBlocks involves
 * allocating an array of numBlocks size per call. It may also involve calling the namenode, if the
 * DFSInputStream has not fetched all the blocks yet. In order to avoid allocation pressure, we
 * cache the computed distribution for a configurable period of time.
 * <p>
 * This class only gets instantiated for the <b>first</b> FSDataInputStream of each StoreFile (i.e.
 * the one backing {@link HStoreFile#initialReader}). It's then used to dynamically update the value
 * returned by {@link HStoreFile#getHDFSBlockDistribution()}.
 * <p>
 * Once the backing FSDataInputStream is closed, we should not expect the distribution result to
 * change anymore. This is ok becuase the initialReader's InputStream is only closed when the
 * StoreFile itself is closed, at which point nothing will be querying getHDFSBlockDistribution
 * anymore. If/When the StoreFile is reopened, a new {@link InputStreamBlockDistribution} will be
 * created for the new initialReader.
 */
@InterfaceAudience.Private
public class InputStreamBlockDistribution {
  private static final Logger LOG = LoggerFactory.getLogger(InputStreamBlockDistribution.class);

  private static final String HBASE_LOCALITY_INPUTSTREAM_DERIVE_ENABLED =
    "hbase.locality.inputstream.derive.enabled";
  private static final boolean DEFAULT_HBASE_LOCALITY_INPUTSTREAM_DERIVE_ENABLED = false;

  private static final String HBASE_LOCALITY_INPUTSTREAM_DERIVE_CACHE_PERIOD =
    "hbase.locality.inputstream.derive.cache.period";
  private static final int DEFAULT_HBASE_LOCALITY_INPUTSTREAM_DERIVE_CACHE_PERIOD = 60_000;

  private final FSDataInputStream stream;
  private final StoreFileInfo fileInfo;
  private final int cachePeriodMs;

  private HDFSBlocksDistribution hdfsBlocksDistribution;
  private long lastCachedAt;
  private boolean streamUnsupported;

  /**
   * This should only be called for the first FSDataInputStream of a StoreFile, in
   * {@link HStoreFile#open()}.
   * @see InputStreamBlockDistribution
   * @param stream   the input stream to derive locality from
   * @param fileInfo the StoreFileInfo for the related store file
   */
  public InputStreamBlockDistribution(FSDataInputStream stream, StoreFileInfo fileInfo) {
    this.stream = stream;
    this.fileInfo = fileInfo;
    this.cachePeriodMs = fileInfo.getConf().getInt(HBASE_LOCALITY_INPUTSTREAM_DERIVE_CACHE_PERIOD,
      DEFAULT_HBASE_LOCALITY_INPUTSTREAM_DERIVE_CACHE_PERIOD);
    this.lastCachedAt = EnvironmentEdgeManager.currentTime();
    this.streamUnsupported = false;
    this.hdfsBlocksDistribution = fileInfo.getHDFSBlockDistribution();
  }

  /**
   * True if we should derive StoreFile HDFSBlockDistribution from the underlying input stream
   */
  public static boolean isEnabled(Configuration conf) {
    return conf.getBoolean(HBASE_LOCALITY_INPUTSTREAM_DERIVE_ENABLED,
      DEFAULT_HBASE_LOCALITY_INPUTSTREAM_DERIVE_ENABLED);
  }

  /**
   * Get the HDFSBlocksDistribution derived from the StoreFile input stream, re-computing if cache
   * is expired.
   */
  public synchronized HDFSBlocksDistribution getHDFSBlockDistribution() {
    if (EnvironmentEdgeManager.currentTime() - lastCachedAt > cachePeriodMs) {
      try {
        LOG.debug("Refreshing HDFSBlockDistribution for {}", fileInfo);
        computeBlockDistribution();
      } catch (IOException e) {
        LOG.warn("Failed to recompute block distribution for {}. Falling back on cached value.",
          fileInfo, e);
      }
    }
    return hdfsBlocksDistribution;
  }

  private void computeBlockDistribution() throws IOException {
    lastCachedAt = EnvironmentEdgeManager.currentTime();

    FSDataInputStream stream;
    if (fileInfo.isLink()) {
      stream = FileLink.getUnderlyingFileLinkInputStream(this.stream);
    } else {
      stream = this.stream;
    }

    if (!(stream instanceof HdfsDataInputStream)) {
      if (!streamUnsupported) {
        LOG.warn(
          "{} for storeFileInfo={}, isLink={}, is not an HdfsDataInputStream so cannot be "
            + "used to derive locality. Falling back on cached value.",
          stream, fileInfo, fileInfo.isLink());
        streamUnsupported = true;
      }
      return;
    }

    streamUnsupported = false;
    hdfsBlocksDistribution = FSUtils.computeHDFSBlocksDistribution((HdfsDataInputStream) stream);
  }

  /**
   * For tests only, sets lastCachedAt so we can force a refresh
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  synchronized void setLastCachedAt(long timestamp) {
    lastCachedAt = timestamp;
  }

  /**
   * For tests only, returns the configured cache period
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  long getCachePeriodMs() {
    return cachePeriodMs;
  }

  /**
   * For tests only, returns whether the passed stream is supported
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  boolean isStreamUnsupported() {
    return streamUnsupported;
  }
}
