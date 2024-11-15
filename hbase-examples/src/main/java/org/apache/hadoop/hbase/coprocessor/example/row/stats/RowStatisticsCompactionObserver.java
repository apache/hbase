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
package org.apache.hadoop.hbase.coprocessor.example.row.stats;

import static org.apache.hadoop.hbase.coprocessor.example.row.stats.utils.RowStatisticsTableUtil.CF;
import static org.apache.hadoop.hbase.coprocessor.example.row.stats.utils.RowStatisticsTableUtil.NAMESPACE;
import static org.apache.hadoop.hbase.coprocessor.example.row.stats.utils.RowStatisticsTableUtil.NAMESPACED_TABLE_NAME;
import static org.apache.hadoop.hbase.coprocessor.example.row.stats.utils.RowStatisticsTableUtil.TABLE_RECORDER_KEY;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RawCellBuilder;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.recorder.RowStatisticsRecorder;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.recorder.RowStatisticsTableRecorder;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.utils.RowStatisticsUtil;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Shipper;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class RowStatisticsCompactionObserver
  implements RegionCoprocessor, RegionObserver, MasterCoprocessor, MasterObserver {

  private static final Logger LOG = LoggerFactory.getLogger(RowStatisticsCompactionObserver.class);

  // From private field BucketAllocator.DEFAULT_BUCKET_SIZES
  private static final long DEFAULT_MAX_BUCKET_SIZE = 512 * 1024 + 1024;
  private static final ConcurrentMap<TableName, Long> TABLE_COUNTERS = new ConcurrentHashMap();
  private static final String ROW_STATISTICS_DROPPED = "rowStatisticsDropped";
  private static final String ROW_STATISTICS_PUT_FAILED = "rowStatisticsPutFailures";
  private Counter rowStatisticsDropped;
  private Counter rowStatisticsPutFailed;
  private long maxCacheSize;
  private final RowStatisticsRecorder recorder;

  @InterfaceAudience.Private
  public RowStatisticsCompactionObserver(RowStatisticsRecorder recorder) {
    this.recorder = recorder;
  }

  public RowStatisticsCompactionObserver() {
    this(null);
  }

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    if (!(e instanceof RegionCoprocessorEnvironment)) {
      return;
    }
    RegionCoprocessorEnvironment regionEnv = (RegionCoprocessorEnvironment) e;
    if (regionEnv.getRegionInfo().getTable().isSystemTable()) {
      return;
    }
    String[] configuredBuckets =
      regionEnv.getConfiguration().getStrings(BlockCacheFactory.BUCKET_CACHE_BUCKETS_KEY);
    maxCacheSize = DEFAULT_MAX_BUCKET_SIZE;
    if (configuredBuckets != null && configuredBuckets.length > 0) {
      String lastBucket = configuredBuckets[configuredBuckets.length - 1];
      try {
        maxCacheSize = Integer.parseInt(lastBucket.trim());
      } catch (NumberFormatException ex) {
        LOG.warn("Failed to parse {} value {} as int", BlockCacheFactory.BUCKET_CACHE_BUCKETS_KEY,
          lastBucket, ex);
      }
    }
    rowStatisticsDropped =
      regionEnv.getMetricRegistryForRegionServer().counter(ROW_STATISTICS_DROPPED);
    rowStatisticsPutFailed =
      regionEnv.getMetricRegistryForRegionServer().counter(ROW_STATISTICS_PUT_FAILED);
    TableName tableName = regionEnv.getRegionInfo().getTable();
    TABLE_COUNTERS.merge(tableName, 1L, Long::sum);
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    if (!(e instanceof RegionCoprocessorEnvironment)) {
      return;
    }
    RegionCoprocessorEnvironment regionEnv = (RegionCoprocessorEnvironment) e;
    if (regionEnv.getRegionInfo().getTable().isSystemTable()) {
      return;
    }
    TableName tableName = regionEnv.getRegionInfo().getTable();
    long tableCount = TABLE_COUNTERS.merge(tableName, -1L, Long::sum);
    if (tableCount == 0) {
      long regionCount = 0;
      for (long count : TABLE_COUNTERS.values()) {
        regionCount += count;
      }
      if (regionCount == 0) {
        regionEnv.getMetricRegistryForRegionServer().remove(ROW_STATISTICS_DROPPED,
          rowStatisticsDropped);
        regionEnv.getMetricRegistryForRegionServer().remove(ROW_STATISTICS_PUT_FAILED,
          rowStatisticsPutFailed);
        RowStatisticsTableRecorder tableRecorder =
          (RowStatisticsTableRecorder) regionEnv.getSharedData().get(TABLE_RECORDER_KEY);
        if (tableRecorder != null) {
          regionEnv.getSharedData().remove(TABLE_RECORDER_KEY, tableRecorder);
          tableRecorder.close();
        }
      }
    }
  }

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
    throws IOException {
    try (Admin admin = ctx.getEnvironment().getConnection().getAdmin()) {
      if (admin.tableExists(NAMESPACED_TABLE_NAME)) {
        LOG.info("Table {} already exists. Skipping table creation process.",
          NAMESPACED_TABLE_NAME);
      } else {
        boolean shouldCreateNamespace =
          Arrays.stream(admin.listNamespaces()).filter(namespace -> namespace.equals(NAMESPACE))
            .collect(Collectors.toUnmodifiableSet()).isEmpty();
        if (shouldCreateNamespace) {
          NamespaceDescriptor nd = NamespaceDescriptor.create(NAMESPACE).build();
          try {
            admin.createNamespace(nd);
          } catch (IOException e) {
            LOG.error("Failed to create namespace {}", NAMESPACE, e);
          }
        }
        ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(CF).setMaxVersions(25)
          .setTimeToLive((int) Duration.ofDays(7).toSeconds()).build();
        TableDescriptor td =
          TableDescriptorBuilder.newBuilder(NAMESPACED_TABLE_NAME).setColumnFamily(cfd).build();
        LOG.info("Creating table {}", NAMESPACED_TABLE_NAME);
        try {
          admin.createTable(td);
        } catch (IOException e) {
          LOG.error("Failed to create table {}", NAMESPACED_TABLE_NAME, e);
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to get Connection or Admin. Cannot determine if table {} exists.",
        NAMESPACED_TABLE_NAME, e);
    }
  }

  @Override
  public InternalScanner preCompact(ObserverContext<? extends RegionCoprocessorEnvironment> context,
    Store store, InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
    CompactionRequest request) {
    if (store.getTableName().isSystemTable()) {
      return scanner;
    }
    int blocksize = store.getColumnFamilyDescriptor().getBlocksize();
    boolean isMajor = request.isMajor();
    RowStatisticsImpl stats = new RowStatisticsImpl(store.getTableName().getNameAsString(),
      store.getRegionInfo().getEncodedName(), store.getColumnFamilyName(), blocksize, maxCacheSize,
      isMajor);
    return new RowStatisticsScanner(scanner, stats, context.getEnvironment(), recorder);
  }

  private static class RowStatisticsScanner implements InternalScanner, Shipper {

    private final InternalScanner scanner;
    private final Shipper shipper;
    private final RowStatisticsImpl rowStatistics;
    private final RegionCoprocessorEnvironment regionEnv;
    private final Counter rowStatisticsDropped;
    private final Counter rowStatisticsPutFailed;
    private final RowStatisticsRecorder customRecorder;
    private RawCellBuilder cellBuilder;
    private Cell lastCell;

    public RowStatisticsScanner(InternalScanner scanner, RowStatisticsImpl rowStatistics,
      RegionCoprocessorEnvironment regionEnv, RowStatisticsRecorder customRecorder) {
      this.scanner = scanner;
      if (scanner instanceof Shipper) {
        this.shipper = (Shipper) scanner;
      } else {
        this.shipper = null;
      }
      this.rowStatistics = rowStatistics;
      this.regionEnv = regionEnv;
      this.cellBuilder = regionEnv.getCellBuilder();
      this.rowStatisticsDropped =
        regionEnv.getMetricRegistryForRegionServer().counter(ROW_STATISTICS_DROPPED);
      this.rowStatisticsPutFailed =
        regionEnv.getMetricRegistryForRegionServer().counter(ROW_STATISTICS_PUT_FAILED);
      this.customRecorder = customRecorder;
    }

    @Override
    public boolean next(List<? super ExtendedCell> result, ScannerContext scannerContext)
      throws IOException {
      boolean ret = scanner.next(result, scannerContext);
      consumeCells(result);
      return ret;
    }

    @Override
    public boolean next(List<? super ExtendedCell> result) throws IOException {
      boolean ret = scanner.next(result);
      consumeCells(result);
      return ret;
    }

    @Override
    public void close() throws IOException {
      rowStatistics.handleRowChanged(lastCell);
      rowStatistics.shipped(cellBuilder);
      record();
      scanner.close();
    }

    @Override
    public void shipped() throws IOException {
      if (shipper != null) {
        lastCell = RowStatisticsUtil.cloneWithoutValue(cellBuilder, lastCell);
        rowStatistics.shipped(cellBuilder);
        shipper.shipped();
      }
    }

    private void consumeCells(List<? super ExtendedCell> result) {
      if (result.isEmpty()) {
        return;
      }
      // each next() call returns at most 1 row (maybe less for large rows)
      // so we just need to check if the first cell has changed rows
      ExtendedCell first = (ExtendedCell) result.get(0);
      if (rowChanged(first)) {
        rowStatistics.handleRowChanged(lastCell);
      }
      for (int i = 0; i < result.size(); i++) {
        ExtendedCell cell = (ExtendedCell) result.get(i);
        rowStatistics.consumeCell(cell);
        lastCell = cell;
      }
    }

    private boolean rowChanged(Cell cell) {
      if (lastCell == null) {
        return false;
      }
      return !CellUtil.matchingRows(lastCell, cell);
    }

    private void record() {
      RowStatisticsTableRecorder tableRecorder =
        (RowStatisticsTableRecorder) regionEnv.getSharedData().computeIfAbsent(TABLE_RECORDER_KEY,
          k -> RowStatisticsTableRecorder.forClusterConnection(regionEnv.getConnection(),
            rowStatisticsDropped, rowStatisticsPutFailed));
      if (tableRecorder != null) {
        tableRecorder.record(this.rowStatistics,
          Optional.of(regionEnv.getRegion().getRegionInfo().getRegionName()));
      } else {
        LOG.error(
          "Failed to initialize a TableRecorder. Will not record row statistics for region={}",
          rowStatistics.getRegion());
        rowStatisticsDropped.increment();
      }
      if (customRecorder != null) {
        customRecorder.record(this.rowStatistics, Optional.empty());
      }
    }
  }
}
