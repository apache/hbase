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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.client.ConnectionUtils.locateRow;
import static org.apache.hadoop.hbase.client.ConnectionUtils.locateRowBefore;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaCellComparator;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionLocateType;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * A cache of meta region locations.
 */
@InterfaceAudience.Private
public class MetaLocationCache implements Stoppable {

  private static final Logger LOG = LoggerFactory.getLogger(MetaLocationCache.class);

  @VisibleForTesting
  static final String SYNC_INTERVAL_SECONDS =
    "hbase.master.meta-location-cache.sync-interval-seconds";

  // default sync every 1 second.
  @VisibleForTesting
  static final int DEFAULT_SYNC_INTERVAL_SECONDS = 1;

  private static final String FETCH_TIMEOUT_MS =
    "hbase.master.meta-location-cache.fetch-timeout-ms";

  // default timeout 1 second
  private static final int DEFAULT_FETCH_TIMEOUT_MS = 1000;

  @VisibleForTesting
  static final class CacheHolder {

    final long lastSyncSeqId;

    // use the primary replica as key since all replicas are in the same row
    final Map<RegionInfo, Long> lastAppliedSeqIds;

    final NavigableMap<byte[], RegionLocations> cache;

    final List<HRegionLocation> all;

    private static <T> Collector<T, ?, NavigableMap<byte[], RegionLocations>> toCache(
      Function<? super T, byte[]> keyMapper, Function<? super T, RegionLocations> valueMapper) {
      return Collectors.collectingAndThen(Collectors.toMap(keyMapper, valueMapper, (u, v) -> {
        throw new IllegalStateException();
      }, () -> new TreeMap<>(MetaCellComparator.ROW_COMPARATOR)),
        Collections::unmodifiableNavigableMap);
    }

    CacheHolder(long lastSyncSeqId, List<HRegionLocation> all) {
      this.lastSyncSeqId = lastSyncSeqId;
      this.lastAppliedSeqIds = Collections.emptyMap();
      this.all = Collections.unmodifiableList(all);
      NavigableMap<byte[], SortedSet<HRegionLocation>> startKeyToLocs =
        new TreeMap<>(MetaCellComparator.ROW_COMPARATOR);
      for (HRegionLocation loc : all) {
        if (loc == null || (loc != null && loc.getRegion().isSplitParent())) {
          continue;
        }
        startKeyToLocs.computeIfAbsent(loc.getRegion().getStartKey(),
          k -> new TreeSet<>((l1, l2) -> l1.getRegion().compareTo(l2.getRegion()))).add(loc);
      }
      this.cache = startKeyToLocs.entrySet().stream()
        .collect(toCache(Map.Entry::getKey, e -> new RegionLocations(e.getValue())));
    }

    CacheHolder(long lastSyncSeqId, Map<RegionInfo, Long> lastAppliedSeqIds,
      Map<RegionInfo, RegionLocations> primaryRegionInfo2Locs) {
      this.lastSyncSeqId = lastSyncSeqId;
      this.lastAppliedSeqIds = lastAppliedSeqIds;
      this.cache = primaryRegionInfo2Locs.entrySet().stream().filter(e -> {
        HRegionLocation loc = e.getValue().getRegionLocation();
        return loc != null && !loc.getRegion().isSplitParent();
      }).collect(toCache(e -> e.getKey().getStartKey(), Map.Entry::getValue));
      this.all =
        primaryRegionInfo2Locs.values().stream().flatMap(rls -> Stream.of(rls.getRegionLocations()))
          .filter(hrl -> hrl != null).collect(Collectors.toList());
    }

    long getLastAppliedSeqId(RegionInfo region) {
      Long lastAppliedSeqId = lastAppliedSeqIds.get(region);
      if (lastAppliedSeqId != null) {
        return lastAppliedSeqId.longValue();
      } else {
        return lastSyncSeqId;
      }
    }
  }

  @VisibleForTesting
  final AtomicReference<CacheHolder> holder = new AtomicReference<>();

  private final ScheduledChore refreshChore;

  private volatile boolean stopped = false;

  MetaLocationCache(MasterServices master) {
    int syncIntervalSeconds =
      master.getConfiguration().getInt(SYNC_INTERVAL_SECONDS, DEFAULT_SYNC_INTERVAL_SECONDS);
    int fetchTimeoutMs =
      master.getConfiguration().getInt(FETCH_TIMEOUT_MS, DEFAULT_FETCH_TIMEOUT_MS);
    refreshChore = new ScheduledChore(getClass().getSimpleName() + "-Sync-Chore", this,
      syncIntervalSeconds, 0, TimeUnit.SECONDS) {

      @Override
      protected void chore() {
        AsyncClusterConnection conn = master.getAsyncClusterConnection();
        if (conn != null) {
          final CacheHolder ch = holder.get();
          long lastSyncSeqId = ch != null ? ch.lastSyncSeqId : HConstants.NO_SEQNUM;
          addListener(conn.syncRoot(lastSyncSeqId, fetchTimeoutMs), (resp, error) -> {
            if (error != null) {
              LOG.warn("Failed to sync root data from active master", error);
              return;
            }
            long lastModifiedSeqId = resp.getFirst().longValue();
            if (ch == null || lastModifiedSeqId > ch.lastSyncSeqId && holder.get() == ch) {
              // since we may trigger cache refresh when locating, here we use CAS to avoid race
              holder.compareAndSet(ch, new CacheHolder(lastModifiedSeqId, resp.getSecond()));
            }
          });
        }
      }
    };
    master.getChoreService().scheduleChore(refreshChore);
  }

  RegionLocations locateMeta(byte[] row, RegionLocateType locateType) {
    if (locateType == RegionLocateType.AFTER) {
      // as we know the exact row after us, so we can just create the new row, and use the same
      // algorithm to locate it.
      row = Arrays.copyOf(row, row.length + 1);
      locateType = RegionLocateType.CURRENT;
    }
    CacheHolder holder = this.holder.get();
    if (holder == null) {
      refreshChore.triggerNow();
      return null;
    }
    return locateType.equals(RegionLocateType.BEFORE) ?
      locateRowBefore(holder.cache, TableName.META_TABLE_NAME, row, RegionInfo.DEFAULT_REPLICA_ID) :
      locateRow(holder.cache, TableName.META_TABLE_NAME, row, RegionInfo.DEFAULT_REPLICA_ID);
  }

  List<HRegionLocation> getAllMetaRegionLocations(boolean excludeOfflinedSplitParents) {
    CacheHolder holder = this.holder.get();
    if (holder == null) {
      refreshChore.triggerNow();
      return Collections.emptyList();
    }
    if (!excludeOfflinedSplitParents) {
      // just return all the locations
      return holder.all;
    } else {
      return holder.all.stream().filter(l -> !l.getRegion().isSplitParent())
        .collect(Collectors.toList());
    }
  }

  private boolean isLocationRelated(Cell cell) {
    byte[] qualifier = CellUtil.cloneQualifier(cell);
    return Bytes.equals(qualifier, HConstants.REGIONINFO_QUALIFIER) ||
      Bytes.startsWith(qualifier, HConstants.SERVER_QUALIFIER) ||
      Bytes.startsWith(qualifier, HConstants.SEQNUM_QUALIFIER);
  }

  private void logSkip(Cell cell) {
    LOG.debug("Skip applying root edit cell {} as it does not related to meta location", cell);
  }

  // align with the region info parsed from region name, where we do not have end key.
  // see CatalogFamilyFormat.parseRegionInfoFromRegionName
  private RegionInfo toRegionInfoKey(RegionInfo region) {
    return RegionInfoBuilder.newBuilder(region.getTable()).setStartKey(region.getStartKey())
      .setRegionId(region.getRegionId()).build();
  }

  private static final class RootEdit {

    final Cell.Type type;

    // for put
    final List<Cell> cells;

    // for delete region replicas
    final MutableInt minimumDeletedReplicaId;

    RootEdit(Cell.Type type) {
      this.type = type;
      if (type == Cell.Type.Put) {
        this.cells = new ArrayList<>();
        this.minimumDeletedReplicaId = null;
      } else if (type == Cell.Type.DeleteColumn) {
        this.minimumDeletedReplicaId = new MutableInt(Integer.MAX_VALUE);
        this.cells = null;
      } else {
        this.cells = null;
        this.minimumDeletedReplicaId = null;
      }
    }

    void addPutCell(Cell cell) {
      cells.add(cell);
    }

    void addDeleteReplicaCell(Cell cell) {
      byte[] qualifier = CellUtil.cloneQualifier(cell);
      //
      int delimiterIndex = qualifier.length - 5;
      if (delimiterIndex < 0 || qualifier[delimiterIndex] != RegionInfo.REPLICA_ID_DELIMITER) {
        return;
      }
      String replicaIdStr = Bytes.toString(qualifier, delimiterIndex + 1);
      int replicaId = Integer.parseInt(replicaIdStr, 16);
      if (replicaId < minimumDeletedReplicaId.intValue()) {
        minimumDeletedReplicaId.setValue(replicaId);
      }
    }
  }

  private Map<RegionInfo, RootEdit> groupRootEdits(Pair<Long, List<Cell>> rootEdit,
    CacheHolder holder, Map<RegionInfo, Long> lastAppliedSeqIds) throws IOException {
    long seqId = rootEdit.getFirst().longValue();
    // group the cells by region info, and also filter out useless cells, an empty list means
    // delete family
    // we only care about put and delete family, as for other edits to root such as delete
    // merge qualifiers are not related to meta location
    Map<RegionInfo, RootEdit> groupByPrimaryRegionInfo = new HashMap<>();
    for (Cell cell : rootEdit.getSecond()) {
      byte[] row = CellUtil.cloneRow(cell);
      RegionInfo primaryRegionInfo = CatalogFamilyFormat.parseRegionInfoFromRegionName(row);
      long lastAppliedSeqId = holder.getLastAppliedSeqId(primaryRegionInfo);
      if (lastAppliedSeqId >= seqId) {
        continue;
      }
      lastAppliedSeqIds.put(primaryRegionInfo, seqId);

      switch (cell.getType()) {
        case Put:
          // this is for adding new regions(including new replicas), move regions, split regions
          // and merge regions
          if (isLocationRelated(cell)) {
            groupByPrimaryRegionInfo
              .computeIfAbsent(primaryRegionInfo, k -> new RootEdit(Cell.Type.Put))
              .addPutCell(cell);
          } else {
            logSkip(cell);
          }
          break;
        case DeleteFamily:
          // this is for removing split parent and merge parents
          groupByPrimaryRegionInfo.put(primaryRegionInfo, new RootEdit(Cell.Type.DeleteFamily));
          break;
        case DeleteColumn:
          // this is for deleting replicas
          if (isLocationRelated(cell)) {
            groupByPrimaryRegionInfo
              .computeIfAbsent(primaryRegionInfo, k -> new RootEdit(Cell.Type.DeleteColumn))
              .addDeleteReplicaCell(cell);
          } else {
            logSkip(cell);
          }
          break;
        default:
          logSkip(cell);
          break;
      }
    }
    return groupByPrimaryRegionInfo;
  }

  private void applyRootEdit(Map<RegionInfo, RootEdit> groupByPrimaryRegionInfo,
    Map<RegionInfo, RegionLocations> primaryRegionInfo2Locs) {
    groupByPrimaryRegionInfo.forEach((primaryRegionInfo, rootEdit) -> {
      switch (rootEdit.type) {
        case Put:
          List<Cell> cells = rootEdit.cells;
          Collections.sort(cells, MetaCellComparator.META_COMPARATOR);
          Result result = Result.create(cells);
          RegionLocations locs = CatalogFamilyFormat.getRegionLocations(result);
          if (locs != null) {
            // for split parent, we skip the mergeLocations call as it is not designed to be used
            // for this, just replace the old locs
            if (locs.getRegionLocation().getRegion().isSplitParent()) {
              primaryRegionInfo2Locs.put(primaryRegionInfo, locs);
            } else {
              RegionLocations oldLocs = primaryRegionInfo2Locs.get(primaryRegionInfo);
              if (oldLocs != null) {
                RegionLocations newLocs = oldLocs.combineLocations(locs);
                primaryRegionInfo2Locs.put(primaryRegionInfo, newLocs);
              } else {
                primaryRegionInfo2Locs.put(primaryRegionInfo, locs);
              }
            }
          } else {
            LOG.warn("Got null region locations when applying root edit {}", result);
          }
          break;
        case DeleteFamily:
          primaryRegionInfo2Locs.remove(primaryRegionInfo);
          break;
        case DeleteColumn:
          RegionLocations oldLocs = primaryRegionInfo2Locs.get(primaryRegionInfo);
          if (oldLocs == null) {
            break;
          }
          HRegionLocation[] oldHrls = oldLocs.getRegionLocations();
          if (oldHrls.length < rootEdit.minimumDeletedReplicaId.intValue() ||
            rootEdit.minimumDeletedReplicaId.intValue() <= 0) {
            break;
          }
          RegionLocations newLocs = new RegionLocations(
            Arrays.copyOf(oldHrls, rootEdit.minimumDeletedReplicaId.intValue()));
          primaryRegionInfo2Locs.put(primaryRegionInfo, newLocs);
          break;
        default:
          throw new IllegalArgumentException("Unsupported cell type: " + rootEdit.type);
      }
    });
  }

  synchronized void applyRootEdits(List<Pair<Long, List<Cell>>> rootEdits) {
    CacheHolder holder = this.holder.get();
    if (holder == null) {
      LOG.info("Skip applying root edits since we haven't done a full sync yet");
      // if there is no cache yet, give up applying edits
      refreshChore.triggerNow();
      return;
    }
    LOG.debug("Apply root edits {}", rootEdits);
    // the key is the primary replica
    Map<RegionInfo, Long> lastAppliedSeqIds = new HashMap<>(holder.lastAppliedSeqIds);
    // we can not use holder.cache directly here as it does not contain split parent
    Map<RegionInfo, RegionLocations> primaryRegionInfo2Locs = new HashMap<>();
    holder.all.stream().collect(Collectors.groupingBy(hrl -> toRegionInfoKey(hrl.getRegion())))
      .forEach((k, v) -> primaryRegionInfo2Locs.put(k, new RegionLocations(v)));
    for (Pair<Long, List<Cell>> rootEdit : rootEdits) {
      Map<RegionInfo, RootEdit> groupByPrimaryRegionInfo;
      try {
        groupByPrimaryRegionInfo = groupRootEdits(rootEdit, holder, lastAppliedSeqIds);
      } catch (Exception e) {
        LOG.warn("Failed to group root edit {}", rootEdit, e);
        continue;
      }
      applyRootEdit(groupByPrimaryRegionInfo, primaryRegionInfo2Locs);
    }
    // since the method is synchronized, we can only fail the CAS when there is a full sync at the
    // same time, then we could just give up and use the result of the full sync, so here we do
    // not
    // retry again when the CAS fails.
    this.holder.compareAndSet(holder,
      new CacheHolder(holder.lastSyncSeqId, lastAppliedSeqIds, primaryRegionInfo2Locs));
  }

  @Override
  public void stop(String why) {
    LOG.info("Stopping meta location cache: {}", why);
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }
}
