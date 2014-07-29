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

package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;

/**
 * Contains utility methods for manipulating HBase meta tables.
 * Be sure to call {@link #shutdown()} when done with this class so it closes
 * resources opened during meta processing (ROOT, META, etc.).  Be careful
 * how you use this class.  If used during migrations, be careful when using
 * this class to check whether migration is needed.
 */
@InterfaceAudience.Private
public class MetaUtils {
  private static final Log LOG = LogFactory.getLog(MetaUtils.class);
  private final Configuration conf;
  private FileSystem fs;
  private WALFactory walFactory;
  private HRegion metaRegion;
  private Map<byte [], HRegion> metaRegions = Collections.synchronizedSortedMap(
    new TreeMap<byte [], HRegion>(Bytes.BYTES_COMPARATOR));

  /** Default constructor
   * @throws IOException e
   */
  public MetaUtils() throws IOException {
    this(HBaseConfiguration.create());
  }

  /**
   * @param conf Configuration
   * @throws IOException e
   */
  public MetaUtils(Configuration conf) throws IOException {
    this.conf = conf;
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    this.metaRegion = null;
    initialize();
  }

  /**
   * Verifies that DFS is available and that HBase is off-line.
   * @throws IOException e
   */
  private void initialize() throws IOException {
    this.fs = FileSystem.get(this.conf);
  }

  /**
   * @return the WAL associated with the given region
   * @throws IOException e
   */
  public synchronized WAL getLog(HRegionInfo info) throws IOException {
    if (this.walFactory == null) {
      String logName = 
          HConstants.HREGION_LOGDIR_NAME + "_" + System.currentTimeMillis();
      final Configuration walConf = new Configuration(this.conf);
      FSUtils.setRootDir(walConf, fs.getHomeDirectory());
      this.walFactory = new WALFactory(walConf, null, logName);
    }
    final byte[] region = info.getEncodedNameAsBytes();
    return info.isMetaRegion() ? walFactory.getMetaWAL(region) : walFactory.getWAL(region);
  }

  /**
   * @return HRegion for meta region
   * @throws IOException e
   */
  public HRegion getMetaRegion() throws IOException {
    if (this.metaRegion == null) {
      openMetaRegion();
    }
    return this.metaRegion;
  }

  /**
   * Closes catalog regions if open. Also closes and deletes the WAL. You
   * must call this method if you want to persist changes made during a
   * MetaUtils edit session.
   */
  public synchronized void shutdown() {
    if (this.metaRegion != null) {
      try {
        this.metaRegion.close();
      } catch (IOException e) {
        LOG.error("closing meta region", e);
      } finally {
        this.metaRegion = null;
      }
    }
    try {
      for (HRegion r: metaRegions.values()) {
        LOG.info("CLOSING hbase:meta " + r.toString());
        r.close();
      }
    } catch (IOException e) {
      LOG.error("closing meta region", e);
    } finally {
      metaRegions.clear();
    }
    try {
      if (this.walFactory != null) {
        this.walFactory.close();
      }
    } catch (IOException e) {
      LOG.error("closing WAL", e);
    }
  }

  private synchronized HRegion openMetaRegion() throws IOException {
    if (this.metaRegion != null) {
      return this.metaRegion;
    }
    this.metaRegion = HRegion.openHRegion(HRegionInfo.FIRST_META_REGIONINFO,
      HTableDescriptor.META_TABLEDESC, getLog(HRegionInfo.FIRST_META_REGIONINFO),
      this.conf);
    this.metaRegion.compactStores();
    return this.metaRegion;
  }
}
