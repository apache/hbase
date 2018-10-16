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
package org.apache.hadoop.hbase.thrift2;

import static org.apache.hadoop.hbase.util.Bytes.getBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.thrift2.generated.TAppend;
import org.apache.hadoop.hbase.thrift2.generated.TColumn;
import org.apache.hadoop.hbase.thrift2.generated.TColumnIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TCompareOp;
import org.apache.hadoop.hbase.thrift2.generated.TConsistency;
import org.apache.hadoop.hbase.thrift2.generated.TDelete;
import org.apache.hadoop.hbase.thrift2.generated.TDurability;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.THRegionInfo;
import org.apache.hadoop.hbase.thrift2.generated.THRegionLocation;
import org.apache.hadoop.hbase.thrift2.generated.TIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TMutation;
import org.apache.hadoop.hbase.thrift2.generated.TPut;
import org.apache.hadoop.hbase.thrift2.generated.TReadType;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TRowMutations;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.hadoop.hbase.thrift2.generated.TServerName;
import org.apache.hadoop.hbase.thrift2.generated.TTimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.MapUtils;

@InterfaceAudience.Private
public final class ThriftUtilities {

  private ThriftUtilities() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

  /**
   * Creates a {@link Get} (HBase) from a {@link TGet} (Thrift).
   *
   * This ignores any timestamps set on {@link TColumn} objects.
   *
   * @param in the <code>TGet</code> to convert
   *
   * @return <code>Get</code> object
   *
   * @throws IOException if an invalid time range or max version parameter is given
   */
  public static Get getFromThrift(TGet in) throws IOException {
    Get out = new Get(in.getRow());

    // Timestamp overwrites time range if both are set
    if (in.isSetTimestamp()) {
      out.setTimestamp(in.getTimestamp());
    } else if (in.isSetTimeRange()) {
      out.setTimeRange(in.getTimeRange().getMinStamp(), in.getTimeRange().getMaxStamp());
    }

    if (in.isSetMaxVersions()) {
      out.setMaxVersions(in.getMaxVersions());
    }

    if (in.isSetFilterString()) {
      ParseFilter parseFilter = new ParseFilter();
      out.setFilter(parseFilter.parseFilterString(in.getFilterString()));
    }

    if (in.isSetAttributes()) {
      addAttributes(out,in.getAttributes());
    }

    if (in.isSetAuthorizations()) {
      out.setAuthorizations(new Authorizations(in.getAuthorizations().getLabels()));
    }

    if (in.isSetConsistency()) {
      out.setConsistency(consistencyFromThrift(in.getConsistency()));
    }

    if (in.isSetTargetReplicaId()) {
      out.setReplicaId(in.getTargetReplicaId());
    }

    if (!in.isSetColumns()) {
      return out;
    }

    for (TColumn column : in.getColumns()) {
      if (column.isSetQualifier()) {
        out.addColumn(column.getFamily(), column.getQualifier());
      } else {
        out.addFamily(column.getFamily());
      }
    }

    return out;
  }

  /**
   * Converts multiple {@link TGet}s (Thrift) into a list of {@link Get}s (HBase).
   *
   * @param in list of <code>TGet</code>s to convert
   *
   * @return list of <code>Get</code> objects
   *
   * @throws IOException if an invalid time range or max version parameter is given
   * @see #getFromThrift(TGet)
   */
  public static List<Get> getsFromThrift(List<TGet> in) throws IOException {
    List<Get> out = new ArrayList<>(in.size());
    for (TGet get : in) {
      out.add(getFromThrift(get));
    }
    return out;
  }

  /**
   * Creates a {@link TResult} (Thrift) from a {@link Result} (HBase).
   *
   * @param in the <code>Result</code> to convert
   *
   * @return converted result, returns an empty result if the input is <code>null</code>
   */
  public static TResult resultFromHBase(Result in) {
    Cell[] raw = in.rawCells();
    TResult out = new TResult();
    byte[] row = in.getRow();
    if (row != null) {
      out.setRow(in.getRow());
    }
    List<TColumnValue> columnValues = new ArrayList<>(raw.length);
    for (Cell kv : raw) {
      TColumnValue col = new TColumnValue();
      col.setFamily(CellUtil.cloneFamily(kv));
      col.setQualifier(CellUtil.cloneQualifier(kv));
      col.setTimestamp(kv.getTimestamp());
      col.setValue(CellUtil.cloneValue(kv));
      if (kv.getTagsLength() > 0) {
        col.setTags(PrivateCellUtil.cloneTags(kv));
      }
      columnValues.add(col);
    }
    out.setColumnValues(columnValues);

    out.setStale(in.isStale());
    return out;
  }

  /**
   * Converts multiple {@link Result}s (HBase) into a list of {@link TResult}s (Thrift).
   *
   * @param in array of <code>Result</code>s to convert
   *
   * @return list of converted <code>TResult</code>s
   *
   * @see #resultFromHBase(Result)
   */
  public static List<TResult> resultsFromHBase(Result[] in) {
    List<TResult> out = new ArrayList<>(in.length);
    for (Result result : in) {
      out.add(resultFromHBase(result));
    }
    return out;
  }

  /**
   * Creates a {@link Put} (HBase) from a {@link TPut} (Thrift)
   *
   * @param in the <code>TPut</code> to convert
   *
   * @return converted <code>Put</code>
   */
  public static Put putFromThrift(TPut in) {
    Put out;

    if (in.isSetTimestamp()) {
      out = new Put(in.getRow(), in.getTimestamp());
    } else {
      out = new Put(in.getRow());
    }

    if (in.isSetDurability()) {
      out.setDurability(durabilityFromThrift(in.getDurability()));
    }

    for (TColumnValue columnValue : in.getColumnValues()) {
      try {
        if (columnValue.isSetTimestamp()) {
          out.add(CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
              .setRow(out.getRow())
              .setFamily(columnValue.getFamily())
              .setQualifier(columnValue.getQualifier())
              .setTimestamp(columnValue.getTimestamp())
              .setType(Cell.Type.Put)
              .setValue(columnValue.getValue())
              .build());
        } else {
          out.add(CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
              .setRow(out.getRow())
              .setFamily(columnValue.getFamily())
              .setQualifier(columnValue.getQualifier())
              .setTimestamp(out.getTimestamp())
              .setType(Cell.Type.Put)
              .setValue(columnValue.getValue())
              .build());
        }
      } catch (IOException e) {
        throw new IllegalArgumentException((e));
      }
    }

    if (in.isSetAttributes()) {
      addAttributes(out,in.getAttributes());
    }

    if (in.getCellVisibility() != null) {
      out.setCellVisibility(new CellVisibility(in.getCellVisibility().getExpression()));
    }

    return out;
  }

  /**
   * Converts multiple {@link TPut}s (Thrift) into a list of {@link Put}s (HBase).
   *
   * @param in list of <code>TPut</code>s to convert
   *
   * @return list of converted <code>Put</code>s
   *
   * @see #putFromThrift(TPut)
   */
  public static List<Put> putsFromThrift(List<TPut> in) {
    List<Put> out = new ArrayList<>(in.size());
    for (TPut put : in) {
      out.add(putFromThrift(put));
    }
    return out;
  }

  /**
   * Creates a {@link Delete} (HBase) from a {@link TDelete} (Thrift).
   *
   * @param in the <code>TDelete</code> to convert
   *
   * @return converted <code>Delete</code>
   */
  public static Delete deleteFromThrift(TDelete in) {
    Delete out;

    if (in.isSetColumns()) {
      out = new Delete(in.getRow());
      for (TColumn column : in.getColumns()) {
        if (in.isSetDeleteType()) {
          switch (in.getDeleteType()) {
            case DELETE_COLUMN:
              if (column.isSetTimestamp()) {
                out.addColumn(column.getFamily(), column.getQualifier(), column.getTimestamp());
              } else {
                out.addColumn(column.getFamily(), column.getQualifier());
              }
              break;
            case DELETE_COLUMNS:
              if (column.isSetTimestamp()) {
                out.addColumns(column.getFamily(), column.getQualifier(), column.getTimestamp());
              } else {
                out.addColumns(column.getFamily(), column.getQualifier());
              }
              break;
            case DELETE_FAMILY:
              if (column.isSetTimestamp()) {
                out.addFamily(column.getFamily(), column.getTimestamp());
              } else {
                out.addFamily(column.getFamily());
              }
              break;
            case DELETE_FAMILY_VERSION:
              if (column.isSetTimestamp()) {
                out.addFamilyVersion(column.getFamily(), column.getTimestamp());
              } else {
                throw new IllegalArgumentException(
                    "Timestamp is required for TDelete with DeleteFamilyVersion type");
              }
              break;
            default:
              throw new IllegalArgumentException("DeleteType is required for TDelete");
          }
        } else {
          throw new IllegalArgumentException("DeleteType is required for TDelete");
        }
      }
    } else {
      if (in.isSetTimestamp()) {
        out = new Delete(in.getRow(), in.getTimestamp());
      } else {
        out = new Delete(in.getRow());
      }
    }

    if (in.isSetAttributes()) {
      addAttributes(out,in.getAttributes());
    }

    if (in.isSetDurability()) {
      out.setDurability(durabilityFromThrift(in.getDurability()));
    }

    return out;
  }

  /**
   * Converts multiple {@link TDelete}s (Thrift) into a list of {@link Delete}s (HBase).
   *
   * @param in list of <code>TDelete</code>s to convert
   *
   * @return list of converted <code>Delete</code>s
   *
   * @see #deleteFromThrift(TDelete)
   */

  public static List<Delete> deletesFromThrift(List<TDelete> in) {
    List<Delete> out = new ArrayList<>(in.size());
    for (TDelete delete : in) {
      out.add(deleteFromThrift(delete));
    }
    return out;
  }

  public static TDelete deleteFromHBase(Delete in) {
    TDelete out = new TDelete(ByteBuffer.wrap(in.getRow()));

    List<TColumn> columns = new ArrayList<>(in.getFamilyCellMap().entrySet().size());
    long rowTimestamp = in.getTimestamp();
    if (rowTimestamp != HConstants.LATEST_TIMESTAMP) {
      out.setTimestamp(rowTimestamp);
    }

    // Map<family, List<KeyValue>>
    for (Map.Entry<byte[], List<org.apache.hadoop.hbase.Cell>> familyEntry:
        in.getFamilyCellMap().entrySet()) {
      TColumn column = new TColumn(ByteBuffer.wrap(familyEntry.getKey()));
      for (org.apache.hadoop.hbase.Cell cell: familyEntry.getValue()) {
        byte[] family = CellUtil.cloneFamily(cell);
        byte[] qualifier = CellUtil.cloneQualifier(cell);
        long timestamp = cell.getTimestamp();
        if (family != null) {
          column.setFamily(family);
        }
        if (qualifier != null) {
          column.setQualifier(qualifier);
        }
        if (timestamp != HConstants.LATEST_TIMESTAMP) {
          column.setTimestamp(timestamp);
        }
      }
      columns.add(column);
    }
    out.setColumns(columns);

    return out;
  }

  /**
   * Creates a {@link RowMutations} (HBase) from a {@link TRowMutations} (Thrift)
   *
   * @param in the <code>TRowMutations</code> to convert
   *
   * @return converted <code>RowMutations</code>
   */
  public static RowMutations rowMutationsFromThrift(TRowMutations in) throws IOException {
    List<TMutation> mutations = in.getMutations();
    RowMutations out = new RowMutations(in.getRow(), mutations.size());
    for (TMutation mutation : mutations) {
      if (mutation.isSetPut()) {
        out.add(putFromThrift(mutation.getPut()));
      }
      if (mutation.isSetDeleteSingle()) {
        out.add(deleteFromThrift(mutation.getDeleteSingle()));
      }
    }
    return out;
  }

  public static Scan scanFromThrift(TScan in) throws IOException {
    Scan out = new Scan();

    if (in.isSetStartRow()) {
      out.setStartRow(in.getStartRow());
    }
    if (in.isSetStopRow()) {
      out.setStopRow(in.getStopRow());
    }
    if (in.isSetCaching()) {
      out.setCaching(in.getCaching());
    }
    if (in.isSetMaxVersions()) {
      out.setMaxVersions(in.getMaxVersions());
    }

    if (in.isSetColumns()) {
      for (TColumn column : in.getColumns()) {
        if (column.isSetQualifier()) {
          out.addColumn(column.getFamily(), column.getQualifier());
        } else {
          out.addFamily(column.getFamily());
        }
      }
    }

    TTimeRange timeRange = in.getTimeRange();
    if (timeRange != null &&
        timeRange.isSetMinStamp() && timeRange.isSetMaxStamp()) {
      out.setTimeRange(timeRange.getMinStamp(), timeRange.getMaxStamp());
    }

    if (in.isSetBatchSize()) {
      out.setBatch(in.getBatchSize());
    }

    if (in.isSetFilterString()) {
      ParseFilter parseFilter = new ParseFilter();
      out.setFilter(parseFilter.parseFilterString(in.getFilterString()));
    }

    if (in.isSetAttributes()) {
      addAttributes(out,in.getAttributes());
    }

    if (in.isSetAuthorizations()) {
      out.setAuthorizations(new Authorizations(in.getAuthorizations().getLabels()));
    }

    if (in.isSetReversed()) {
      out.setReversed(in.isReversed());
    }

    if (in.isSetCacheBlocks()) {
      out.setCacheBlocks(in.isCacheBlocks());
    }

    if (in.isSetColFamTimeRangeMap()) {
      Map<ByteBuffer, TTimeRange> colFamTimeRangeMap = in.getColFamTimeRangeMap();
      if (MapUtils.isNotEmpty(colFamTimeRangeMap)) {
        for (Map.Entry<ByteBuffer, TTimeRange> entry : colFamTimeRangeMap.entrySet()) {
          out.setColumnFamilyTimeRange(Bytes.toBytes(entry.getKey()),
              entry.getValue().getMinStamp(), entry.getValue().getMaxStamp());
        }
      }
    }

    if (in.isSetReadType()) {
      out.setReadType(readTypeFromThrift(in.getReadType()));
    }

    if (in.isSetLimit()) {
      out.setLimit(in.getLimit());
    }

    if (in.isSetConsistency()) {
      out.setConsistency(consistencyFromThrift(in.getConsistency()));
    }

    if (in.isSetTargetReplicaId()) {
      out.setReplicaId(in.getTargetReplicaId());
    }

    return out;
  }

  public static Increment incrementFromThrift(TIncrement in) throws IOException {
    Increment out = new Increment(in.getRow());
    for (TColumnIncrement column : in.getColumns()) {
      out.addColumn(column.getFamily(), column.getQualifier(), column.getAmount());
    }

    if (in.isSetAttributes()) {
      addAttributes(out,in.getAttributes());
    }

    if (in.isSetDurability()) {
      out.setDurability(durabilityFromThrift(in.getDurability()));
    }

    if(in.getCellVisibility() != null) {
      out.setCellVisibility(new CellVisibility(in.getCellVisibility().getExpression()));
    }

    return out;
  }

  public static Append appendFromThrift(TAppend append) throws IOException {
    Append out = new Append(append.getRow());
    for (TColumnValue column : append.getColumns()) {
      out.addColumn(column.getFamily(), column.getQualifier(), column.getValue());
    }

    if (append.isSetAttributes()) {
      addAttributes(out, append.getAttributes());
    }

    if (append.isSetDurability()) {
      out.setDurability(durabilityFromThrift(append.getDurability()));
    }

    if(append.getCellVisibility() != null) {
      out.setCellVisibility(new CellVisibility(append.getCellVisibility().getExpression()));
    }

    return out;
  }

  public static THRegionLocation regionLocationFromHBase(HRegionLocation hrl) {
    HRegionInfo hri = hrl.getRegionInfo();
    ServerName serverName = hrl.getServerName();

    THRegionInfo thRegionInfo = new THRegionInfo();
    THRegionLocation thRegionLocation = new THRegionLocation();
    TServerName tServerName = new TServerName();

    tServerName.setHostName(serverName.getHostname());
    tServerName.setPort(serverName.getPort());
    tServerName.setStartCode(serverName.getStartcode());

    thRegionInfo.setTableName(hri.getTable().getName());
    thRegionInfo.setEndKey(hri.getEndKey());
    thRegionInfo.setStartKey(hri.getStartKey());
    thRegionInfo.setOffline(hri.isOffline());
    thRegionInfo.setSplit(hri.isSplit());
    thRegionInfo.setReplicaId(hri.getReplicaId());

    thRegionLocation.setRegionInfo(thRegionInfo);
    thRegionLocation.setServerName(tServerName);

    return thRegionLocation;
  }

  public static List<THRegionLocation> regionLocationsFromHBase(List<HRegionLocation> locations) {
    List<THRegionLocation> tlocations = new ArrayList<>(locations.size());
    for (HRegionLocation hrl:locations) {
      tlocations.add(regionLocationFromHBase(hrl));
    }
    return tlocations;
  }

  /**
   * Adds all the attributes into the Operation object
   */
  private static void addAttributes(OperationWithAttributes op,
                                    Map<ByteBuffer, ByteBuffer> attributes) {
    if (attributes == null || attributes.isEmpty()) {
      return;
    }
    for (Map.Entry<ByteBuffer, ByteBuffer> entry : attributes.entrySet()) {
      String name = Bytes.toStringBinary(getBytes(entry.getKey()));
      byte[] value =  getBytes(entry.getValue());
      op.setAttribute(name, value);
    }
  }

  private static Durability durabilityFromThrift(TDurability tDurability) {
    switch (tDurability.getValue()) {
      case 1: return Durability.SKIP_WAL;
      case 2: return Durability.ASYNC_WAL;
      case 3: return Durability.SYNC_WAL;
      case 4: return Durability.FSYNC_WAL;
      default: return null;
    }
  }

  public static CompareOperator compareOpFromThrift(TCompareOp tCompareOp) {
    switch (tCompareOp.getValue()) {
      case 0: return CompareOperator.LESS;
      case 1: return CompareOperator.LESS_OR_EQUAL;
      case 2: return CompareOperator.EQUAL;
      case 3: return CompareOperator.NOT_EQUAL;
      case 4: return CompareOperator.GREATER_OR_EQUAL;
      case 5: return CompareOperator.GREATER;
      case 6: return CompareOperator.NO_OP;
      default: return null;
    }
  }

  private static ReadType readTypeFromThrift(TReadType tReadType) {
    switch (tReadType.getValue()) {
      case 1: return ReadType.DEFAULT;
      case 2: return ReadType.STREAM;
      case 3: return ReadType.PREAD;
      default: return null;
    }
  }

  private static Consistency consistencyFromThrift(TConsistency tConsistency) {
    switch (tConsistency.getValue()) {
      case 1: return Consistency.STRONG;
      case 2: return Consistency.TIMELINE;
      default: return Consistency.STRONG;
    }
  }
}
