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

package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.mob.MobFileCache;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client scanner for a region opened for read-only on the client side. Assumes region data
 * is not changing.
 */
@InterfaceAudience.Private
public class ClientSideRegionScanner extends AbstractClientScanner {

  private static final Logger LOG = LoggerFactory.getLogger(ClientSideRegionScanner.class);

  private HRegion region;
  RegionScanner scanner;
  List<Cell> values;

  public ClientSideRegionScanner(Configuration conf, FileSystem fs,
      Path rootDir, TableDescriptor htd, RegionInfo hri, Scan scan, ScanMetrics scanMetrics)
      throws IOException {
    // region is immutable, set isolation level
    scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);

    htd = TableDescriptorBuilder.newBuilder(htd).setReadOnly(true).build();

    // open region from the snapshot directory
    region = HRegion.newHRegion(CommonFSUtils.getTableDir(rootDir, htd.getTableName()), null, fs,
      conf, hri, htd, null);
    region.setRestoredRegion(true);
    // non RS process does not have a block cache, and this a client side scanner,
    // create one for MapReduce jobs to cache the INDEX block by setting to use
    // IndexOnlyLruBlockCache and set a value to HBASE_CLIENT_SCANNER_BLOCK_CACHE_SIZE_KEY
    conf.set(BlockCacheFactory.BLOCKCACHE_POLICY_KEY, "IndexOnlyLRU");
    conf.setIfUnset(HConstants.HFILE_ONHEAP_BLOCK_CACHE_FIXED_SIZE_KEY,
        String.valueOf(HConstants.HBASE_CLIENT_SCANNER_ONHEAP_BLOCK_CACHE_FIXED_SIZE_DEFAULT));
    // don't allow L2 bucket cache for non RS process to avoid unexpected disk usage.
    conf.unset(HConstants.BUCKET_CACHE_IOENGINE_KEY);
    region.setBlockCache(BlockCacheFactory.createBlockCache(conf));
    // we won't initialize the MobFileCache when not running in RS process. so provided an
    // initialized cache. Consider the case: an CF was set from an mob to non-mob. if we only
    // initialize cache for MOB region, NPE from HMobStore will still happen. So Initialize the
    // cache for every region although it may hasn't any mob CF, BTW the cache is very light-weight.
    region.setMobFileCache(new MobFileCache(conf));
    region.initialize();

    // create an internal region scanner
    this.scanner = region.getScanner(scan);
    values = new ArrayList<>();

    if (scanMetrics == null) {
      initScanMetrics(scan);
    } else {
      this.scanMetrics = scanMetrics;
    }
    region.startRegionOperation();
  }

  @Override
  public Result next() throws IOException {
    values.clear();
    scanner.nextRaw(values);
    if (values.isEmpty()) {
      //we are done
      return null;
    }

    Result result = Result.create(values);
    if (this.scanMetrics != null) {
      long resultSize = 0;
      for (Cell cell : values) {
        resultSize += PrivateCellUtil.estimatedSerializedSizeOf(cell);
      }
      this.scanMetrics.countOfBytesInResults.addAndGet(resultSize);
      this.scanMetrics.countOfRowsScanned.incrementAndGet();
    }

    return result;
  }

  @Override
  public void close() {
    if (this.scanner != null) {
      try {
        this.scanner.close();
        this.scanner = null;
      } catch (IOException ex) {
        LOG.warn("Exception while closing scanner", ex);
      }
    }
    if (this.region != null) {
      try {
        this.region.closeRegionOperation();
        this.region.close(true);
        this.region = null;
      } catch (IOException ex) {
        LOG.warn("Exception while closing region", ex);
      }
    }
  }

  HRegion getRegion() {
    return region;
  }

  @Override
  public boolean renewLease() {
    throw new UnsupportedOperationException();
  }
}
