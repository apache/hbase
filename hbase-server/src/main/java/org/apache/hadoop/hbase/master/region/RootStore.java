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
package org.apache.hadoop.hbase.master.region;

import static org.apache.hadoop.hbase.HConstants.NO_NONCE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.util.AtomicUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper of {@link MasterRegion} to support root table storage.
 */
@InterfaceAudience.Private
public class RootStore {

  private static final Logger LOG = LoggerFactory.getLogger(RootStore.class);

  private final MasterRegion region;

  private final AtomicLong lastModifiedSeqId = new AtomicLong(HConstants.NO_SEQNUM);

  public RootStore(MasterRegion region) {
    this.region = region;
    lastModifiedSeqId.set(region.getReadPoint());
    region.getWAL().registerWALActionsListener(new WALActionsListener() {

      @Override
      public void postAppend(long entryLen, long elapsedTimeMillis, WALKey logKey, WALEdit logEdit)
        throws IOException {
        for (byte[] family : logEdit.getFamilies()) {
          // we only care about catalog family
          if (!Bytes.equals(family, HConstants.CATALOG_FAMILY)) {
            return;
          }
        }
        AtomicUtils.updateMax(lastModifiedSeqId, logKey.getSequenceId());
      }
    });
  }

  public ResultScanner getScanner(Scan scan) throws IOException {
    return new RegionScannerAsResultScanner(region.getScanner(scan));
  }

  public Result get(Get get) throws IOException {
    return region.get(get);
  }

  public void put(Put put) throws IOException {
    region.update(r -> r.put(put));
  }

  public void delete(Delete delete) throws IOException {
    region.update(r -> r.delete(delete));
  }

  public void delete(List<Delete> deletes) throws IOException {
    region.update(r -> {
      for (Delete delete : deletes) {
        r.delete(delete);
      }
    });
  }

  public void multiMutate(List<Mutation> mutations) throws IOException {
    region.update(r -> {
      List<byte[]> rowsToLock =
        mutations.stream().map(Mutation::getRow).collect(Collectors.toList());
      r.mutateRowsWithLocks(mutations, rowsToLock, NO_NONCE, NO_NONCE);
    });
  }

  public List<RegionLocations> getAllMetaRegionLocations(boolean excludeOfflinedSplitParents)
    throws IOException {
    List<RegionLocations> list = new ArrayList<>();
    try (ResultScanner scanner = getScanner(new Scan().addFamily(HConstants.CATALOG_FAMILY))) {
      for (;;) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        RegionLocations locs = CatalogFamilyFormat.getRegionLocations(result);
        if (locs == null) {
          LOG.warn("No locations in {}", result);
          continue;
        }
        HRegionLocation loc = locs.getRegionLocation();
        if (loc == null) {
          LOG.warn("No non null location in {}", result);
          continue;
        }
        RegionInfo info = loc.getRegion();
        if (info == null) {
          LOG.warn("No serialized RegionInfo in {}", result);
          continue;
        }
        if (excludeOfflinedSplitParents && info.isSplitParent()) {
          continue;
        }
        list.add(locs);
      }
    }
    return list;
  }

  public Pair<Long, List<RegionLocations>> sync(long lastSyncSeqId) throws IOException {
    long lastModSeqId = Math.min(lastModifiedSeqId.get(), region.getReadPoint());
    if (lastModSeqId <= lastSyncSeqId) {
      return Pair.newPair(lastSyncSeqId, Collections.emptyList());
    }
    return Pair.newPair(lastModSeqId, getAllMetaRegionLocations(false));
  }
}
