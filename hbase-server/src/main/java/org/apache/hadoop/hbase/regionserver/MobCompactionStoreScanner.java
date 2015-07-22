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
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Scanner scans the MOB Store. Coalesce KeyValue stream into List<KeyValue>
 * for a single row. It's only used in the compaction of mob-enabled columns.
 * It outputs the normal cells and delete markers when outputDeleteMarkers is set as true.
 */
@InterfaceAudience.Private
public class MobCompactionStoreScanner extends StoreScanner {

  /*
   * The delete markers are probably contained in the output of the scanner, for instance the
   * minor compaction. If outputDeleteMarkers is set as true, these delete markers could be
   * written to the del file, otherwise it's not allowed.
   */
  protected boolean outputDeleteMarkers;

  /**
   * Used for compactions.<p>
   *
   * Opens a scanner across specified StoreFiles.
   * @param store who we scan
   * @param scan the spec
   * @param scanners ancillary scanners
   * @param smallestReadPoint the readPoint that we should use for tracking
   *          versions
   */
  public MobCompactionStoreScanner(Store store, ScanInfo scanInfo, Scan scan,
      List<? extends KeyValueScanner> scanners, ScanType scanType, long smallestReadPoint,
      long earliestPutTs, boolean outputDeleteMarkers) throws IOException {
    super(store, scanInfo, scan, scanners, scanType, smallestReadPoint, earliestPutTs);
    this.outputDeleteMarkers = outputDeleteMarkers;
  }

  /**
   * Gets whether the delete markers could be written to the del files.
   * @return True if the delete markers could be written del files, false if it's not allowed.
   */
  public boolean isOutputDeleteMarkers() {
    return this.outputDeleteMarkers;
  }
}
