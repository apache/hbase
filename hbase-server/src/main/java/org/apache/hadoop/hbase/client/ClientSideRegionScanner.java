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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.mortbay.log.Log;

/**
 * A client scanner for a region opened for read-only on the client side. Assumes region data
 * is not changing.
 */
@InterfaceAudience.Private
public class ClientSideRegionScanner extends AbstractClientScanner {

  private HRegion region;
  RegionScanner scanner;
  List<Cell> values;

  public ClientSideRegionScanner(Configuration conf, FileSystem fs,
      Path rootDir, HTableDescriptor htd, HRegionInfo hri, Scan scan, ScanMetrics scanMetrics)
          throws IOException {

    // region is immutable, set isolation level
    scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);

    // open region from the snapshot directory
    this.region = HRegion.openHRegion(conf, fs, rootDir, hri, htd, null, null, null);

    // create an internal region scanner
    this.scanner = region.getScanner(scan);
    values = new ArrayList<Cell>();

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
        resultSize += CellUtil.estimatedSerializedSizeOf(cell);
      }
      this.scanMetrics.countOfBytesInResults.addAndGet(resultSize);
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
        Log.warn("Exception while closing scanner", ex);
      }
    }
    if (this.region != null) {
      try {
        this.region.closeRegionOperation();
        this.region.close(true);
        this.region = null;
      } catch (IOException ex) {
        Log.warn("Exception while closing region", ex);
      }
    }
  }

  @Override
  public boolean renewLease() {
    throw new UnsupportedOperationException();
  }
}
