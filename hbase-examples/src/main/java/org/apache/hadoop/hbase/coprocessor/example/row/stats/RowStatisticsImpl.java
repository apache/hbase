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

import java.util.Map;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.RawCellBuilder;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.utils.RowStatisticsUtil;
import org.apache.hadoop.hbase.regionserver.Shipper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;

/**
 * Holder for accumulating row statistics in {@link RowStatisticsCompactionObserver} Creates various
 * cell, row, and total stats.
 */
@InterfaceAudience.Private
public class RowStatisticsImpl implements RowStatistics {

  private static final Logger LOG = LoggerFactory.getLogger(RowStatisticsImpl.class);
  private static final Gson GSON = GsonUtil.createGson().create();

  //
  // Transient fields which are not included in gson serialization
  //
  private final transient long blockSize;
  private final transient long maxCacheSize;
  private transient int rowCells;
  private transient long rowBytes;
  private transient byte[] largestRow;
  private transient Cell largestCell;
  private final transient boolean isMajor;
  private final transient SizeBucketTracker rowSizeBuckets;
  private final transient SizeBucketTracker valueSizeBuckets;

  // We don't need to clone anything until shipped() is called on scanner.
  // To avoid allocations, we keep a reference until that point
  private transient Cell largestRowRef;
  private transient Cell largestCellRef;
  //
  // Non-transient fields which are included in gson
  //
  private final String table;
  private final String region;
  private final String columnFamily;
  private long largestRowNumBytes;
  private int largestRowCellsCount;
  private long largestCellNumBytes;
  private int cellsLargerThanOneBlockCount;
  private int rowsLargerThanOneBlockCount;
  private int cellsLargerThanMaxCacheSizeCount;
  private int totalDeletesCount;
  private int totalCellsCount;
  private int totalRowsCount;
  private long totalBytesCount;

  RowStatisticsImpl(String table, String encodedRegion, String columnFamily, long blockSize,
    long maxCacheSize, boolean isMajor) {
    this.table = table;
    this.region = encodedRegion;
    this.columnFamily = columnFamily;
    this.blockSize = blockSize;
    this.maxCacheSize = maxCacheSize;
    this.isMajor = isMajor;
    this.rowSizeBuckets = new SizeBucketTracker();
    this.valueSizeBuckets = new SizeBucketTracker();
  }

  public void handleRowChanged(Cell lastCell) {
    if (rowBytes > largestRowNumBytes) {
      largestRowRef = lastCell;
      largestRowNumBytes = rowBytes;
      largestRowCellsCount = rowCells;
    }
    if (rowBytes > blockSize) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("RowTooLarge: rowBytes={}, blockSize={}, table={}, rowKey={}", rowBytes,
          blockSize, table, Bytes.toStringBinary(lastCell.getRowArray(), lastCell.getRowOffset(),
            lastCell.getRowLength()));
      }
      rowsLargerThanOneBlockCount++;
    }
    rowSizeBuckets.add(rowBytes);
    rowBytes = 0;
    rowCells = 0;
    totalRowsCount++;
  }

  public void consumeCell(Cell cell) {
    int cellSize = cell.getSerializedSize();

    rowBytes += cellSize;
    rowCells++;

    boolean tooLarge = false;
    if (cellSize > maxCacheSize) {
      cellsLargerThanMaxCacheSizeCount++;
      tooLarge = true;
    }
    if (cellSize > blockSize) {
      cellsLargerThanOneBlockCount++;
      tooLarge = true;
    }

    if (tooLarge && LOG.isDebugEnabled()) {
      LOG.debug("CellTooLarge: size={}, blockSize={}, maxCacheSize={}, table={}, cell={}", cellSize,
        blockSize, maxCacheSize, table, CellUtil.toString(cell, false));
    }

    if (cellSize > largestCellNumBytes) {
      largestCellRef = cell;
      largestCellNumBytes = cellSize;
    }
    valueSizeBuckets.add(cell.getValueLength());

    totalCellsCount++;
    if (CellUtil.isDelete(cell)) {
      totalDeletesCount++;
    }
    totalBytesCount += cellSize;
  }

  /**
   * Clone the cell refs so they can be cleaned up by {@link Shipper#shipped()}. Doing this lazily
   * here, rather than eagerly in the above two methods can save us on some allocations. We might
   * change the largestCell/largestRow multiple times between shipped() calls.
   */
  public void shipped(RawCellBuilder cellBuilder) {
    if (largestRowRef != null) {
      largestRow = CellUtil.cloneRow(largestRowRef);
      largestRowRef = null;
    }
    if (largestCellRef != null) {
      largestCell = RowStatisticsUtil.cloneWithoutValue(cellBuilder, largestCellRef);
      largestCellRef = null;
    }
  }

  @Override
  public String getTable() {
    return table;
  }

  @Override
  public String getRegion() {
    return region;
  }

  @Override
  public String getColumnFamily() {
    return columnFamily;
  }

  @Override
  public boolean isMajor() {
    return isMajor;
  }

  public byte[] getLargestRow() {
    return largestRow;
  }

  @Override
  public String getLargestRowAsString() {
    return Bytes.toStringBinary(getLargestRow());
  }

  @Override
  public long getLargestRowNumBytes() {
    return largestRowNumBytes;
  }

  @Override
  public int getLargestRowCellsCount() {
    return largestRowCellsCount;
  }

  public Cell getLargestCell() {
    return largestCell;
  }

  @Override
  public String getLargestCellAsString() {
    return CellUtil.toString(getLargestCell(), false);
  }

  @Override
  public long getLargestCellNumBytes() {
    return largestCellNumBytes;
  }

  @Override
  public int getCellsLargerThanOneBlockCount() {
    return cellsLargerThanOneBlockCount;
  }

  @Override
  public int getRowsLargerThanOneBlockCount() {
    return rowsLargerThanOneBlockCount;
  }

  @Override
  public int getCellsLargerThanMaxCacheSizeCount() {
    return cellsLargerThanMaxCacheSizeCount;
  }

  @Override
  public int getTotalDeletesCount() {
    return totalDeletesCount;
  }

  @Override
  public int getTotalCellsCount() {
    return totalCellsCount;
  }

  @Override
  public int getTotalRowsCount() {
    return totalRowsCount;
  }

  @Override
  public long getTotalBytes() {
    return totalBytesCount;
  }

  @Override
  public Map<String, Long> getRowSizeBuckets() {
    return rowSizeBuckets.toMap();
  }

  @Override
  public Map<String, Long> getValueSizeBuckets() {
    return valueSizeBuckets.toMap();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("largestRowAsString", Bytes.toStringBinary(largestRow))
      .append("largestCellAsString", largestCell).append("largestRowNumBytes", largestRowNumBytes)
      .append("largestRowCellsCount", largestRowCellsCount)
      .append("largestCellNumBytes", largestCellNumBytes)
      .append("cellsLargerThanOneBlockCount", cellsLargerThanOneBlockCount)
      .append("rowsLargerThanOneBlockCount", rowsLargerThanOneBlockCount)
      .append("cellsLargerThanMaxCacheSizeCount", cellsLargerThanMaxCacheSizeCount)
      .append("totalDeletesCount", totalDeletesCount).append("totalCellsCount", totalCellsCount)
      .append("totalRowsCount", totalRowsCount).append("totalBytesCount", totalBytesCount)
      .append("rowSizeBuckets", getRowSizeBuckets())
      .append("valueSizeBuckets", getValueSizeBuckets()).append("isMajor", isMajor).toString();
  }

  @Override
  public String getJsonString() {
    JsonObject json = (JsonObject) GSON.toJsonTree(this);
    json.add("largestCellParts", buildLargestCellPartsJson());
    json.addProperty("largestRowAsString", getLargestRowAsString());
    json.add("rowSizeBuckets", rowSizeBuckets.toJsonObject());
    json.add("valueSizeBuckets", valueSizeBuckets.toJsonObject());
    return json.toString();
  }

  private JsonObject buildLargestCellPartsJson() {
    JsonObject cellJson = new JsonObject();
    Cell cell = getLargestCell();
    cellJson.addProperty("rowKey",
      Bytes.toStringBinary(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
    cellJson.addProperty("family",
      Bytes.toStringBinary(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
    cellJson.addProperty("qualifier", Bytes.toStringBinary(cell.getQualifierArray(),
      cell.getQualifierOffset(), cell.getQualifierLength()));
    cellJson.addProperty("timestamp", cell.getTimestamp());
    cellJson.addProperty("type", cell.getType().toString());
    return cellJson;
  }
}
