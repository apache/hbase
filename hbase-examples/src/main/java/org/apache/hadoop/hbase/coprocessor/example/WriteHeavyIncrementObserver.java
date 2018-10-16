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
package org.apache.hadoop.hbase.coprocessor.example;

import java.io.IOException;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.IntStream;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanOptions;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.math.IntMath;

/**
 * An example for implementing a counter that reads is much less than writes, i.e, write heavy.
 * <p>
 * We will convert increment to put, and do aggregating when get. And of course the return value of
 * increment is useless then.
 * <p>
 * Notice that this is only an example so we do not handle most corner cases, for example, you must
 * provide a qualifier when doing a get.
 */
@InterfaceAudience.Private
public class WriteHeavyIncrementObserver implements RegionCoprocessor, RegionObserver {

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      ScanOptions options, FlushLifeCycleTracker tracker) throws IOException {
    options.readAllVersions();
  }

  private Cell createCell(byte[] row, byte[] family, byte[] qualifier, long ts, long value) {
    return CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(row)
        .setType(Cell.Type.Put).setFamily(family).setQualifier(qualifier)
        .setTimestamp(ts).setValue(Bytes.toBytes(value)).build();
  }

  private InternalScanner wrap(byte[] family, InternalScanner scanner) {
    return new InternalScanner() {

      private List<Cell> srcResult = new ArrayList<>();

      private byte[] row;

      private byte[] qualifier;

      private long timestamp;

      private long sum;

      @Override
      public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
        boolean moreRows = scanner.next(srcResult, scannerContext);
        if (srcResult.isEmpty()) {
          if (!moreRows && row != null) {
            result.add(createCell(row, family, qualifier, timestamp, sum));
          }
          return moreRows;
        }
        Cell firstCell = srcResult.get(0);
        // Check if there is a row change first. All the cells will come from the same row so just
        // check the first one once is enough.
        if (row == null) {
          row = CellUtil.cloneRow(firstCell);
          qualifier = CellUtil.cloneQualifier(firstCell);
        } else if (!CellUtil.matchingRows(firstCell, row)) {
          result.add(createCell(row, family, qualifier, timestamp, sum));
          row = CellUtil.cloneRow(firstCell);
          qualifier = CellUtil.cloneQualifier(firstCell);
          sum = 0;
        }
        srcResult.forEach(c -> {
          if (CellUtil.matchingQualifier(c, qualifier)) {
            sum += Bytes.toLong(c.getValueArray(), c.getValueOffset());
          } else {
            result.add(createCell(row, family, qualifier, timestamp, sum));
            qualifier = CellUtil.cloneQualifier(c);
            sum = Bytes.toLong(c.getValueArray(), c.getValueOffset());
          }
          timestamp = c.getTimestamp();
        });
        if (!moreRows) {
          result.add(createCell(row, family, qualifier, timestamp, sum));
        }
        srcResult.clear();
        return moreRows;
      }

      @Override
      public void close() throws IOException {
        scanner.close();
      }
    };
  }

  @Override
  public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      InternalScanner scanner, FlushLifeCycleTracker tracker) throws IOException {
    return wrap(store.getColumnFamilyDescriptor().getName(), scanner);
  }

  @Override
  public void preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      ScanType scanType, ScanOptions options, CompactionLifeCycleTracker tracker,
      CompactionRequest request) throws IOException {
    options.readAllVersions();
  }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
      CompactionRequest request) throws IOException {
    return wrap(store.getColumnFamilyDescriptor().getName(), scanner);
  }

  @Override
  public void preMemStoreCompactionCompactScannerOpen(
      ObserverContext<RegionCoprocessorEnvironment> c, Store store, ScanOptions options)
      throws IOException {
    options.readAllVersions();
  }

  @Override
  public InternalScanner preMemStoreCompactionCompact(
      ObserverContext<RegionCoprocessorEnvironment> c, Store store, InternalScanner scanner)
      throws IOException {
    return wrap(store.getColumnFamilyDescriptor().getName(), scanner);
  }

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<Cell> result)
      throws IOException {
    Scan scan =
        new Scan().withStartRow(get.getRow()).withStopRow(get.getRow(), true).readAllVersions();
    NavigableMap<byte[], NavigableMap<byte[], MutableLong>> sums =
        new TreeMap<>(Bytes.BYTES_COMPARATOR);
    get.getFamilyMap().forEach((cf, cqs) -> {
      NavigableMap<byte[], MutableLong> ss = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      sums.put(cf, ss);
      cqs.forEach(cq -> {
        ss.put(cq, new MutableLong(0));
        scan.addColumn(cf, cq);
      });
    });
    List<Cell> cells = new ArrayList<>();
    try (RegionScanner scanner = c.getEnvironment().getRegion().getScanner(scan)) {
      boolean moreRows;
      do {
        moreRows = scanner.next(cells);
        for (Cell cell : cells) {
          byte[] family = CellUtil.cloneFamily(cell);
          byte[] qualifier = CellUtil.cloneQualifier(cell);
          long value = Bytes.toLong(cell.getValueArray(), cell.getValueOffset());
          sums.get(family).get(qualifier).add(value);
        }
        cells.clear();
      } while (moreRows);
    }
    sums.forEach((cf, m) -> m.forEach((cq, s) -> result
        .add(createCell(get.getRow(), cf, cq, HConstants.LATEST_TIMESTAMP, s.longValue()))));
    c.bypass();
  }

  private final int mask;
  private final MutableLong[] lastTimestamps;
  {
    int stripes =
        1 << IntMath.log2(Runtime.getRuntime().availableProcessors(), RoundingMode.CEILING);
    lastTimestamps =
        IntStream.range(0, stripes).mapToObj(i -> new MutableLong()).toArray(MutableLong[]::new);
    mask = stripes - 1;
  }

  // We need make sure the different put uses different timestamp otherwise we may lost some
  // increments. This is a known issue for HBase.
  private long getUniqueTimestamp(byte[] row) {
    int slot = Bytes.hashCode(row) & mask;
    MutableLong lastTimestamp = lastTimestamps[slot];
    long now = System.currentTimeMillis();
    synchronized (lastTimestamp) {
      long pt = lastTimestamp.longValue() >> 10;
      if (now > pt) {
        lastTimestamp.setValue(now << 10);
      } else {
        lastTimestamp.increment();
      }
      return lastTimestamp.longValue();
    }
  }

  @Override
  public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment)
      throws IOException {
    byte[] row = increment.getRow();
    Put put = new Put(row);
    long ts = getUniqueTimestamp(row);
    for (Map.Entry<byte[], List<Cell>> entry : increment.getFamilyCellMap().entrySet()) {
      for (Cell cell : entry.getValue()) {
        put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(row)
            .setFamily(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
            .setQualifier(cell.getQualifierArray(), cell.getQualifierOffset(),
              cell.getQualifierLength())
            .setValue(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())
            .setType(Cell.Type.Put).setTimestamp(ts).build());
      }
    }
    c.getEnvironment().getRegion().put(put);
    c.bypass();
    return Result.EMPTY_RESULT;
  }

  @Override
  public void preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Store store,
      ScanOptions options) throws IOException {
    options.readAllVersions();
  }
}
