package org.apache.hadoop.hbase.coprocessor.example.row.stats;

import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.RawCellBuilder;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.utils.RowStatisticsUtil;
import org.apache.hadoop.hbase.regionserver.Shipper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;

/**
 * Holder for accumulating row statistics in {@link RowStatisticsCompactionObserver}
 * Creates various cell, row, and total stats.
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
  private long largestRowBytes;
  private int largestRowCells;
  private long largestCellBytes;
  private int cellsLargerThanOneBlock;
  private int rowsLargerThanOneBlock;
  private int cellsLargerThanMaxCacheSize;
  private int totalDeletes;
  private int totalCells;
  private int totalRows;
  private long totalBytes;

  RowStatisticsImpl(
    String table,
    String encodedRegion,
    String columnFamily,
    long blockSize,
    long maxCacheSize,
    boolean isMajor
  ) {
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
    if (rowBytes > largestRowBytes) {
      largestRowRef = lastCell;
      largestRowBytes = rowBytes;
      largestRowCells = rowCells;
    }
    if (rowBytes > blockSize) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "RowTooLarge: rowBytes={}, blockSize={}, table={}, rowKey={}",
          rowBytes,
          blockSize,
          table,
          Bytes.toStringBinary(
            lastCell.getRowArray(),
            lastCell.getRowOffset(),
            lastCell.getRowLength()
          )
        );
      }
      rowsLargerThanOneBlock++;
    }
    rowSizeBuckets.add(rowBytes);
    rowBytes = 0;
    rowCells = 0;
    totalRows++;
  }

  public void consumeCell(Cell cell) {
    int cellSize = cell.getSerializedSize();

    rowBytes += cellSize;
    rowCells++;

    boolean tooLarge = false;
    if (cellSize > maxCacheSize) {
      cellsLargerThanMaxCacheSize++;
      tooLarge = true;
    }
    if (cellSize > blockSize) {
      cellsLargerThanOneBlock++;
      tooLarge = true;
    }

    if (tooLarge && LOG.isDebugEnabled()) {
      LOG.debug(
        "CellTooLarge: size={}, blockSize={}, maxCacheSize={}, table={}, cell={}",
        cellSize,
        blockSize,
        maxCacheSize,
        table,
        CellUtil.toString(cell, false)
      );
    }

    if (cellSize > largestCellBytes) {
      largestCellRef = cell;
      largestCellBytes = cellSize;
    }
    valueSizeBuckets.add(cell.getValueLength());

    totalCells++;
    if (CellUtil.isDelete(cell)) {
      totalDeletes++;
    }
    totalBytes += cellSize;
  }

  /**
   * Clone the cell refs so they can be cleaned up by {@link Shipper#shipped()}.
   * Doing this lazily here, rather than eagerly in the above two methods can save
   * us on some allocations. We might change the largestCell/largestRow multiple times
   * between shipped() calls.
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

  public String getTable() {
    return table;
  }

  public String getRegion() {
    return region;
  }

  public String getColumnFamily() {
    return columnFamily;
  }

  public byte[] getLargestRow() {
    return largestRow;
  }

  public String getLargestRowAsString() {
    return Bytes.toStringBinary(getLargestRow());
  }

  public long getLargestRowBytes() {
    return largestRowBytes;
  }

  public int getLargestRowCells() {
    return largestRowCells;
  }

  public Cell getLargestCell() {
    return largestCell;
  }

  public String getLargestCellAsString() {
    return CellUtil.toString(getLargestCell(), false);
  }

  public long getLargestCellBytes() {
    return largestCellBytes;
  }

  public int getCellsLargerThanOneBlock() {
    return cellsLargerThanOneBlock;
  }

  public int getRowsLargerThanOneBlock() {
    return rowsLargerThanOneBlock;
  }

  public int getCellsLargerThanMaxCacheSize() {
    return cellsLargerThanMaxCacheSize;
  }

  public int getTotalDeletes() {
    return totalDeletes;
  }

  public int getTotalCells() {
    return totalCells;
  }

  public int getTotalRows() {
    return totalRows;
  }

  public long getTotalBytes() {
    return totalBytes;
  }

  public Map<String, Long> getRowSizeBuckets() {
    return rowSizeBuckets.toMap();
  }

  public Map<String, Long> getValueSizeBuckets() {
    return valueSizeBuckets.toMap();
  }

  @Override
  public String toString() {
    return (
      "RowStatistics{" +
      "largestRow=" +
      Bytes.toStringBinary(largestRow) +
      ", largestRowBytes=" +
      largestRowBytes +
      ", largestRowCells=" +
      largestRowCells +
      ", largestCell=" +
      largestCell +
      ", largestCellBytes=" +
      largestCellBytes +
      ", cellsLargerThanOneBlock=" +
      cellsLargerThanOneBlock +
      ", rowsLargerThanOneBlock=" +
      rowsLargerThanOneBlock +
      ", cellsLargerThanMaxCacheSize=" +
      cellsLargerThanMaxCacheSize +
      ", totalDeletes=" +
      totalDeletes +
      ", totalCells=" +
      totalCells +
      ", totalRows=" +
      totalRows +
      ", totalBytes=" +
      totalBytes +
      ", rowSizeBuckets=" +
      getRowSizeBuckets() +
      ", valueSizeBuckets=" +
      getValueSizeBuckets() +
      ", isMajor=" +
      isMajor +
      '}'
    );
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
    cellJson.addProperty(
      "rowKey",
      Bytes.toStringBinary(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
    );
    cellJson.addProperty(
      "family",
      Bytes.toStringBinary(
        cell.getFamilyArray(),
        cell.getFamilyOffset(),
        cell.getFamilyLength()
      )
    );
    cellJson.addProperty(
      "qualifier",
      Bytes.toStringBinary(
        cell.getQualifierArray(),
        cell.getQualifierOffset(),
        cell.getQualifierLength()
      )
    );
    cellJson.addProperty("timestamp", cell.getTimestamp());
    cellJson.addProperty("type", cell.getType().toString());
    return cellJson;
  }
}
