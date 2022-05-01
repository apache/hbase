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
package org.apache.hadoop.hbase.master.region;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrap a {@link RegionScanner} as a {@link ResultScanner}.
 */
@InterfaceAudience.Private
class RegionScannerAsResultScanner implements ResultScanner {

  private static final Logger LOG = LoggerFactory.getLogger(RegionScannerAsResultScanner.class);

  private final RegionScanner scanner;

  private boolean moreRows = true;

  private final List<Cell> cells = new ArrayList<>();

  RegionScannerAsResultScanner(RegionScanner scanner) {
    this.scanner = scanner;
  }

  @Override
  public boolean renewLease() {
    return true;
  }

  @Override
  public Result next() throws IOException {
    if (!moreRows) {
      return null;
    }
    for (;;) {
      moreRows = scanner.next(cells);
      if (cells.isEmpty()) {
        if (!moreRows) {
          return null;
        } else {
          continue;
        }
      }
      Result result = Result.create(cells);
      cells.clear();
      return result;
    }
  }

  @Override
  public ScanMetrics getScanMetrics() {
    return null;
  }

  @Override
  public void close() {
    try {
      scanner.close();
    } catch (IOException e) {
      LOG.warn("Failed to close scanner", e);
    }
  }
}
