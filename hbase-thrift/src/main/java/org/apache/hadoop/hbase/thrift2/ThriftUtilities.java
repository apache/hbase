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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.LogQueryFilter;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.OnlineLogRecord;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.thrift2.generated.TAppend;
import org.apache.hadoop.hbase.thrift2.generated.TAuthorization;
import org.apache.hadoop.hbase.thrift2.generated.TBloomFilterType;
import org.apache.hadoop.hbase.thrift2.generated.TCellVisibility;
import org.apache.hadoop.hbase.thrift2.generated.TColumn;
import org.apache.hadoop.hbase.thrift2.generated.TColumnFamilyDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.TColumnIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TCompareOp;
import org.apache.hadoop.hbase.thrift2.generated.TCompressionAlgorithm;
import org.apache.hadoop.hbase.thrift2.generated.TConsistency;
import org.apache.hadoop.hbase.thrift2.generated.TDataBlockEncoding;
import org.apache.hadoop.hbase.thrift2.generated.TDelete;
import org.apache.hadoop.hbase.thrift2.generated.TDeleteType;
import org.apache.hadoop.hbase.thrift2.generated.TDurability;
import org.apache.hadoop.hbase.thrift2.generated.TFilterByOperator;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.THRegionInfo;
import org.apache.hadoop.hbase.thrift2.generated.THRegionLocation;
import org.apache.hadoop.hbase.thrift2.generated.TIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TKeepDeletedCells;
import org.apache.hadoop.hbase.thrift2.generated.TLogQueryFilter;
import org.apache.hadoop.hbase.thrift2.generated.TLogType;
import org.apache.hadoop.hbase.thrift2.generated.TMutation;
import org.apache.hadoop.hbase.thrift2.generated.TNamespaceDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.TOnlineLogRecord;
import org.apache.hadoop.hbase.thrift2.generated.TPut;
import org.apache.hadoop.hbase.thrift2.generated.TReadType;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TRowMutations;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.hadoop.hbase.thrift2.generated.TServerName;
import org.apache.hadoop.hbase.thrift2.generated.TTableDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.TTableName;
import org.apache.hadoop.hbase.thrift2.generated.TTimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.MapUtils;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;

@InterfaceAudience.Private
public class ThriftUtilities {

  private final static Cell[] EMPTY_CELL_ARRAY = new Cell[]{};
  private final static Result EMPTY_RESULT = Result.create(EMPTY_CELL_ARRAY);
  private final static Result EMPTY_RESULT_STALE = Result.create(EMPTY_CELL_ARRAY, null, true);



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

    if (in.isSetCacheBlocks()) {
      out.setCacheBlocks(in.isCacheBlocks());
    }
    if (in.isSetStoreLimit()) {
      out.setMaxResultsPerColumnFamily(in.getStoreLimit());
    }
    if (in.isSetStoreOffset()) {
      out.setRowOffsetPerColumnFamily(in.getStoreOffset());
    }
    if (in.isSetExistence_only()) {
      out.setCheckExistenceOnly(in.isExistence_only());
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

    if (in.isSetFilterBytes()) {
      out.setFilter(filterFromThrift(in.getFilterBytes()));
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
      col.setType(kv.getType().getCode());
      if (kv.getTagsLength() > 0) {
        col.setTags(PrivateCellUtil.cloneTags(kv));
      }
      columnValues.add(col);
    }
    out.setColumnValues(columnValues);

    out.setStale(in.isStale());

    out.setPartial(in.mayHaveMoreCellsInRow());
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

  public static TDeleteType deleteTypeFromHBase(Cell.Type type) {
    switch (type) {
      case Delete: return TDeleteType.DELETE_COLUMN;
      case DeleteColumn: return TDeleteType.DELETE_COLUMNS;
      case DeleteFamily: return TDeleteType.DELETE_FAMILY;
      case DeleteFamilyVersion: return TDeleteType.DELETE_FAMILY_VERSION;
      default: throw new IllegalArgumentException("Unknow delete type " + type);
    }  }

  public static TDelete deleteFromHBase(Delete in) {
    TDelete out = new TDelete(ByteBuffer.wrap(in.getRow()));

    List<TColumn> columns = new ArrayList<>(in.getFamilyCellMap().entrySet().size());
    long rowTimestamp = in.getTimestamp();
    if (rowTimestamp != HConstants.LATEST_TIMESTAMP) {
      out.setTimestamp(rowTimestamp);
    }

    for (Map.Entry<String, byte[]> attribute : in.getAttributesMap().entrySet()) {
      out.putToAttributes(ByteBuffer.wrap(Bytes.toBytes(attribute.getKey())),
          ByteBuffer.wrap(attribute.getValue()));
    }
    if (in.getDurability() != Durability.USE_DEFAULT)  {
      out.setDurability(durabilityFromHBase(in.getDurability()));
    }
    // Delete the whole row
    if (in.getFamilyCellMap().size() == 0) {
      return out;
    }
    TDeleteType type = null;
    for (Map.Entry<byte[], List<Cell>> familyEntry:
        in.getFamilyCellMap().entrySet()) {
      byte[] family = familyEntry.getKey();
      TColumn column = new TColumn(ByteBuffer.wrap(familyEntry.getKey()));
      for (Cell cell: familyEntry.getValue()) {
        TDeleteType cellDeleteType = deleteTypeFromHBase(cell.getType());
        if (type == null) {
          type = cellDeleteType;
        } else if (type != cellDeleteType){
          throw new RuntimeException("Only the same delete type is supported, but two delete type "
              + "is founded, one is " + type + " the other one is " + cellDeleteType);
        }
        byte[] qualifier = CellUtil.cloneQualifier(cell);
        long timestamp = cell.getTimestamp();
        column.setFamily(family);
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
    out.setDeleteType(type);

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

    if (in.isSetStartRow())
      out.setStartRow(in.getStartRow());
    if (in.isSetStopRow())
      out.setStopRow(in.getStopRow());
    if (in.isSetCaching())
      out.setCaching(in.getCaching());
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

    if (in.isSetFilterBytes()) {
      out.setFilter(filterFromThrift(in.getFilterBytes()));
    }

    return out;
  }

  public static byte[] filterFromHBase(Filter filter) throws IOException {
    FilterProtos.Filter filterPB = ProtobufUtil.toFilter(filter);
    return filterPB.toByteArray();
  }

  public static Filter filterFromThrift(byte[] filterBytes) throws IOException {
    FilterProtos.Filter filterPB  = FilterProtos.Filter.parseFrom(filterBytes);
    return ProtobufUtil.toFilter(filterPB);
  }

  public static TScan scanFromHBase(Scan in) throws IOException {
    TScan out = new TScan();
    out.setStartRow(in.getStartRow());
    out.setStopRow(in.getStopRow());
    out.setCaching(in.getCaching());
    out.setMaxVersions(in.getMaxVersions());
    for (Map.Entry<byte[], NavigableSet<byte[]>> family : in.getFamilyMap().entrySet()) {

      if (family.getValue() != null && !family.getValue().isEmpty()) {
        for (byte[] qualifier : family.getValue()) {
          TColumn column = new TColumn();
          column.setFamily(family.getKey());
          column.setQualifier(qualifier);
          out.addToColumns(column);
        }
      } else {
        TColumn column = new TColumn();
        column.setFamily(family.getKey());
        out.addToColumns(column);
      }
    }
    TTimeRange tTimeRange = new TTimeRange();
    tTimeRange.setMinStamp(in.getTimeRange().getMin()).setMaxStamp(in.getTimeRange().getMax());
    out.setTimeRange(tTimeRange);
    out.setBatchSize(in.getBatch());

    for (Map.Entry<String, byte[]> attribute : in.getAttributesMap().entrySet()) {
      out.putToAttributes(ByteBuffer.wrap(Bytes.toBytes(attribute.getKey())),
          ByteBuffer.wrap(attribute.getValue()));
    }

    try {
      Authorizations authorizations = in.getAuthorizations();
      if (authorizations != null) {
        TAuthorization tAuthorization = new TAuthorization();
        tAuthorization.setLabels(authorizations.getLabels());
        out.setAuthorizations(tAuthorization);
      }
    } catch (DeserializationException e) {
      throw new RuntimeException(e);
    }

    out.setReversed(in.isReversed());
    out.setCacheBlocks(in.getCacheBlocks());
    out.setReadType(readTypeFromHBase(in.getReadType()));
    out.setLimit(in.getLimit());
    out.setConsistency(consistencyFromHBase(in.getConsistency()));
    out.setTargetReplicaId(in.getReplicaId());
    for (Map.Entry<byte[], TimeRange> entry : in.getColumnFamilyTimeRange().entrySet()) {
      if (entry.getValue() != null) {
        TTimeRange timeRange = new TTimeRange();
        timeRange.setMinStamp(entry.getValue().getMin()).setMaxStamp(entry.getValue().getMax());
        out.putToColFamTimeRangeMap(ByteBuffer.wrap(entry.getKey()), timeRange);
      }
    }
    if (in.getFilter() != null) {
      try {
        out.setFilterBytes(filterFromHBase(in.getFilter()));
      } catch (IOException ioE) {
        throw new RuntimeException(ioE);
      }
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

    if (in.isSetReturnResults()) {
      out.setReturnResults(in.isReturnResults());
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

    if (append.isSetReturnResults()) {
      out.setReturnResults(append.isReturnResults());
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
      case 0: return Durability.USE_DEFAULT;
      case 1: return Durability.SKIP_WAL;
      case 2: return Durability.ASYNC_WAL;
      case 3: return Durability.SYNC_WAL;
      case 4: return Durability.FSYNC_WAL;
      default: return Durability.USE_DEFAULT;
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

  private static TReadType readTypeFromHBase(ReadType readType) {
    switch (readType) {
      case DEFAULT: return TReadType.DEFAULT;
      case STREAM: return TReadType.STREAM;
      case PREAD: return TReadType.PREAD;
      default: return TReadType.DEFAULT;
    }
  }

  private static Consistency consistencyFromThrift(TConsistency tConsistency) {
    switch (tConsistency.getValue()) {
      case 1: return Consistency.STRONG;
      case 2: return Consistency.TIMELINE;
      default: return Consistency.STRONG;
    }
  }

  public static TableName tableNameFromThrift(TTableName tableName) {
    return TableName.valueOf(tableName.getNs(), tableName.getQualifier());
  }

  public static TableName[] tableNamesArrayFromThrift(List<TTableName> tableNames) {
    TableName[] out = new TableName[tableNames.size()];
    int index = 0;
    for (TTableName tableName : tableNames) {
      out[index++] = tableNameFromThrift(tableName);
    }
    return out;
  }

  public static List<TableName> tableNamesFromThrift(List<TTableName> tableNames) {
    List<TableName> out = new ArrayList<>(tableNames.size());
    for (TTableName tableName : tableNames) {
      out.add(tableNameFromThrift(tableName));
    }
    return out;
  }

  public static TTableName tableNameFromHBase(TableName table) {
    TTableName tableName = new TTableName();
    tableName.setNs(table.getNamespace());
    tableName.setQualifier(table.getQualifier());
    return tableName;
  }

  public static List<TTableName> tableNamesFromHBase(List<TableName> in) {
    List<TTableName> out = new ArrayList<>(in.size());
    for (TableName tableName : in) {
      out.add(tableNameFromHBase(tableName));
    }
    return out;
  }

  public static List<TTableName> tableNamesFromHBase(TableName[] in) {
    List<TTableName> out = new ArrayList<>(in.length);
    for (TableName tableName : in) {
      out.add(tableNameFromHBase(tableName));
    }
    return out;
  }

  public static byte[][] splitKeyFromThrift(List<ByteBuffer> in) {
    if (in == null || in.size() == 0) {
      return null;
    }
    byte[][] out = new byte[in.size()][];
    int index = 0;
    for (ByteBuffer key : in) {
      out[index++] = key.array();
    }
    return out;
  }

  public static BloomType bloomFilterFromThrift(TBloomFilterType in) {
    switch (in.getValue()) {
      case 0: return BloomType.NONE;
      case 1: return BloomType.ROW;
      case 2: return BloomType.ROWCOL;
      case 3: return BloomType.ROWPREFIX_FIXED_LENGTH;
      default: return BloomType.ROW;
    }
  }

  public static Compression.Algorithm compressionAlgorithmFromThrift(TCompressionAlgorithm in) {
    switch (in.getValue()) {
      case 0: return Compression.Algorithm.LZO;
      case 1: return Compression.Algorithm.GZ;
      case 2: return Compression.Algorithm.NONE;
      case 3: return Compression.Algorithm.SNAPPY;
      case 4: return Compression.Algorithm.LZ4;
      case 5: return Compression.Algorithm.BZIP2;
      case 6: return Compression.Algorithm.ZSTD;
      default: return Compression.Algorithm.NONE;
    }
  }

  public static DataBlockEncoding dataBlockEncodingFromThrift(TDataBlockEncoding in) {
    switch (in.getValue()) {
      case 0: return DataBlockEncoding.NONE;
      case 2: return DataBlockEncoding.PREFIX;
      case 3: return DataBlockEncoding.DIFF;
      case 4: return DataBlockEncoding.FAST_DIFF;
      case 7: return DataBlockEncoding.ROW_INDEX_V1;
      default: return DataBlockEncoding.NONE;
    }
  }

  public static KeepDeletedCells keepDeletedCellsFromThrift(TKeepDeletedCells in) {
    switch (in.getValue()) {
      case 0: return KeepDeletedCells.FALSE;
      case 1: return KeepDeletedCells.TRUE;
      case 2: return KeepDeletedCells.TTL;
      default: return KeepDeletedCells.FALSE;
    }
  }

  public static ColumnFamilyDescriptor columnFamilyDescriptorFromThrift(
      TColumnFamilyDescriptor in) {
    ColumnFamilyDescriptorBuilder builder = ColumnFamilyDescriptorBuilder
        .newBuilder(in.getName());

    if (in.isSetAttributes()) {
      for (Map.Entry<ByteBuffer, ByteBuffer> attribute : in.getAttributes().entrySet()) {
        builder.setValue(attribute.getKey().array(), attribute.getValue().array());
      }
    }
    if (in.isSetConfiguration()) {
      for (Map.Entry<String, String> conf : in.getConfiguration().entrySet()) {
        builder.setConfiguration(conf.getKey(), conf.getValue());
      }
    }
    if (in.isSetBlockSize()) {
      builder.setBlocksize(in.getBlockSize());
    }
    if (in.isSetBloomnFilterType()) {
      builder.setBloomFilterType(bloomFilterFromThrift(in.getBloomnFilterType()));
    }
    if (in.isSetCompressionType()) {
      builder.setCompressionType(compressionAlgorithmFromThrift(in.getCompressionType()));
    }
    if (in.isSetDfsReplication()) {
      builder.setDFSReplication(in.getDfsReplication());
    }
    if (in.isSetDataBlockEncoding()) {
      builder.setDataBlockEncoding(dataBlockEncodingFromThrift(in.getDataBlockEncoding()));
    }
    if (in.isSetKeepDeletedCells()) {
      builder.setKeepDeletedCells(keepDeletedCellsFromThrift(in.getKeepDeletedCells()));
    }
    if (in.isSetMaxVersions()) {
      builder.setMaxVersions(in.getMaxVersions());
    }
    if (in.isSetMinVersions()) {
      builder.setMinVersions(in.getMinVersions());
    }
    if (in.isSetScope()) {
      builder.setScope(in.getScope());
    }
    if (in.isSetTimeToLive()) {
      builder.setTimeToLive(in.getTimeToLive());
    }
    if (in.isSetBlockCacheEnabled()) {
      builder.setBlockCacheEnabled(in.isBlockCacheEnabled());
    }
    if (in.isSetCacheBloomsOnWrite()) {
      builder.setCacheBloomsOnWrite(in.isCacheBloomsOnWrite());
    }
    if (in.isSetCacheDataOnWrite()) {
      builder.setCacheDataOnWrite(in.isCacheDataOnWrite());
    }
    if (in.isSetCacheIndexesOnWrite()) {
      builder.setCacheIndexesOnWrite(in.isCacheIndexesOnWrite());
    }
    if (in.isSetCompressTags()) {
      builder.setCompressTags(in.isCompressTags());
    }
    if (in.isSetEvictBlocksOnClose()) {
      builder.setEvictBlocksOnClose(in.isEvictBlocksOnClose());
    }
    if (in.isSetInMemory()) {
      builder.setInMemory(in.isInMemory());
    }


    return builder.build();
  }

  public static NamespaceDescriptor namespaceDescriptorFromThrift(TNamespaceDescriptor in) {
    NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(in.getName());
    if (in.isSetConfiguration()) {
      for (Map.Entry<String, String> conf : in.getConfiguration().entrySet()) {
        builder.addConfiguration(conf.getKey(), conf.getValue());
      }
    }
    return builder.build();
  }

  public static TNamespaceDescriptor namespaceDescriptorFromHBase(NamespaceDescriptor in) {
    TNamespaceDescriptor out = new TNamespaceDescriptor();
    out.setName(in.getName());
    for (Map.Entry<String, String> conf : in.getConfiguration().entrySet()) {
      out.putToConfiguration(conf.getKey(), conf.getValue());
    }
    return out;
  }

  public static List<TNamespaceDescriptor> namespaceDescriptorsFromHBase(
      NamespaceDescriptor[] in) {
    List<TNamespaceDescriptor> out = new ArrayList<>(in.length);
    for (NamespaceDescriptor descriptor : in) {
      out.add(namespaceDescriptorFromHBase(descriptor));
    }
    return out;
  }

  public static TableDescriptor tableDescriptorFromThrift(TTableDescriptor in) {
    TableDescriptorBuilder builder = TableDescriptorBuilder
        .newBuilder(tableNameFromThrift(in.getTableName()));
    for (TColumnFamilyDescriptor column : in.getColumns()) {
      builder.setColumnFamily(columnFamilyDescriptorFromThrift(column));
    }
    if (in.isSetAttributes()) {
      for (Map.Entry<ByteBuffer, ByteBuffer> attribute : in.getAttributes().entrySet()) {
        builder.setValue(attribute.getKey().array(), attribute.getValue().array());
      }
    }
    if (in.isSetDurability()) {
      builder.setDurability(durabilityFromThrift(in.getDurability()));
    }
    return builder.build();
  }

  public static HTableDescriptor hTableDescriptorFromThrift(TTableDescriptor in) {
    return new HTableDescriptor(tableDescriptorFromThrift(in));
  }

  public static HTableDescriptor[] hTableDescriptorsFromThrift(List<TTableDescriptor> in) {
    HTableDescriptor[] out = new HTableDescriptor[in.size()];
    int index = 0;
    for (TTableDescriptor tTableDescriptor : in) {
      out[index++] = hTableDescriptorFromThrift(tTableDescriptor);
    }
    return out;
  }


  public static List<TableDescriptor> tableDescriptorsFromThrift(List<TTableDescriptor> in) {
    List<TableDescriptor> out = new ArrayList<>();
    for (TTableDescriptor tableDescriptor : in) {
      out.add(tableDescriptorFromThrift(tableDescriptor));
    }
    return out;
  }

  private static TDurability durabilityFromHBase(Durability durability) {
    switch (durability) {
      case USE_DEFAULT: return TDurability.USE_DEFAULT;
      case SKIP_WAL: return TDurability.SKIP_WAL;
      case ASYNC_WAL: return TDurability.ASYNC_WAL;
      case SYNC_WAL: return TDurability.SYNC_WAL;
      case FSYNC_WAL: return TDurability.FSYNC_WAL;
      default: return null;
    }
  }

  public static TTableDescriptor tableDescriptorFromHBase(TableDescriptor in) {
    TTableDescriptor out = new TTableDescriptor();
    out.setTableName(tableNameFromHBase(in.getTableName()));
    Map<Bytes, Bytes> attributes = in.getValues();
    for (Map.Entry<Bytes, Bytes> attribute : attributes.entrySet()) {
      out.putToAttributes(ByteBuffer.wrap(attribute.getKey().get()),
          ByteBuffer.wrap(attribute.getValue().get()));
    }
    for (ColumnFamilyDescriptor column : in.getColumnFamilies()) {
      out.addToColumns(columnFamilyDescriptorFromHBase(column));
    }
    out.setDurability(durabilityFromHBase(in.getDurability()));
    return out;
  }

  public static List<TTableDescriptor> tableDescriptorsFromHBase(List<TableDescriptor> in) {
    List<TTableDescriptor> out = new ArrayList<>(in.size());
    for (TableDescriptor descriptor : in) {
      out.add(tableDescriptorFromHBase(descriptor));
    }
    return out;
  }

  public static List<TTableDescriptor> tableDescriptorsFromHBase(TableDescriptor[] in) {
    List<TTableDescriptor> out = new ArrayList<>(in.length);
    for (TableDescriptor descriptor : in) {
      out.add(tableDescriptorFromHBase(descriptor));
    }
    return out;
  }


  public static TBloomFilterType bloomFilterFromHBase(BloomType in) {
    switch (in) {
      case NONE: return TBloomFilterType.NONE;
      case ROW: return TBloomFilterType.ROW;
      case ROWCOL: return TBloomFilterType.ROWCOL;
      case ROWPREFIX_FIXED_LENGTH: return TBloomFilterType.ROWPREFIX_FIXED_LENGTH;
      default: return TBloomFilterType.ROW;
    }
  }

  public static TCompressionAlgorithm compressionAlgorithmFromHBase(Compression.Algorithm in) {
    switch (in) {
      case LZO: return TCompressionAlgorithm.LZO;
      case GZ: return TCompressionAlgorithm.GZ;
      case NONE: return TCompressionAlgorithm.NONE;
      case SNAPPY: return TCompressionAlgorithm.SNAPPY;
      case LZ4: return TCompressionAlgorithm.LZ4;
      case BZIP2: return TCompressionAlgorithm.BZIP2;
      case ZSTD: return TCompressionAlgorithm.ZSTD;
      default: return TCompressionAlgorithm.NONE;
    }
  }

  public static TDataBlockEncoding dataBlockEncodingFromHBase(DataBlockEncoding in) {
    switch (in) {
      case NONE: return TDataBlockEncoding.NONE;
      case PREFIX: return TDataBlockEncoding.PREFIX;
      case DIFF: return TDataBlockEncoding.DIFF;
      case FAST_DIFF: return TDataBlockEncoding.FAST_DIFF;
      case ROW_INDEX_V1: return TDataBlockEncoding.ROW_INDEX_V1;
      default: return TDataBlockEncoding.NONE;
    }
  }

  public static TKeepDeletedCells keepDeletedCellsFromHBase(KeepDeletedCells in) {
    switch (in) {
      case FALSE: return TKeepDeletedCells.FALSE;
      case TRUE: return TKeepDeletedCells.TRUE;
      case TTL: return TKeepDeletedCells.TTL;
      default: return TKeepDeletedCells.FALSE;
    }
  }

  public static TColumnFamilyDescriptor columnFamilyDescriptorFromHBase(
      ColumnFamilyDescriptor in) {
    TColumnFamilyDescriptor out = new TColumnFamilyDescriptor();
    out.setName(in.getName());
    for (Map.Entry<Bytes, Bytes> attribute : in.getValues().entrySet()) {
      out.putToAttributes(ByteBuffer.wrap(attribute.getKey().get()),
          ByteBuffer.wrap(attribute.getValue().get()));
    }
    for (Map.Entry<String, String> conf : in.getConfiguration().entrySet()) {
      out.putToConfiguration(conf.getKey(), conf.getValue());
    }
    out.setBlockSize(in.getBlocksize());
    out.setBloomnFilterType(bloomFilterFromHBase(in.getBloomFilterType()));
    out.setCompressionType(compressionAlgorithmFromHBase(in.getCompressionType()));
    out.setDfsReplication(in.getDFSReplication());
    out.setDataBlockEncoding(dataBlockEncodingFromHBase(in.getDataBlockEncoding()));
    out.setKeepDeletedCells(keepDeletedCellsFromHBase(in.getKeepDeletedCells()));
    out.setMaxVersions(in.getMaxVersions());
    out.setMinVersions(in.getMinVersions());
    out.setScope(in.getScope());
    out.setTimeToLive(in.getTimeToLive());
    out.setBlockCacheEnabled(in.isBlockCacheEnabled());
    out.setCacheBloomsOnWrite(in.isCacheBloomsOnWrite());
    out.setCacheDataOnWrite(in.isCacheDataOnWrite());
    out.setCacheIndexesOnWrite(in.isCacheIndexesOnWrite());
    out.setCompressTags(in.isCompressTags());
    out.setEvictBlocksOnClose(in.isEvictBlocksOnClose());
    out.setInMemory(in.isInMemory());
    return out;
  }


  private static TConsistency consistencyFromHBase(Consistency consistency) {
    switch (consistency) {
      case STRONG: return TConsistency.STRONG;
      case TIMELINE: return TConsistency.TIMELINE;
      default: return TConsistency.STRONG;
    }
  }

  public static TGet getFromHBase(Get in) {
    TGet out = new TGet();
    out.setRow(in.getRow());

    TTimeRange tTimeRange = new TTimeRange();
    tTimeRange.setMaxStamp(in.getTimeRange().getMax()).setMinStamp(in.getTimeRange().getMin());
    out.setTimeRange(tTimeRange);
    out.setMaxVersions(in.getMaxVersions());

    for (Map.Entry<String, byte[]> attribute : in.getAttributesMap().entrySet()) {
      out.putToAttributes(ByteBuffer.wrap(Bytes.toBytes(attribute.getKey())),
          ByteBuffer.wrap(attribute.getValue()));
    }
    try {
      Authorizations authorizations = in.getAuthorizations();
      if (authorizations != null) {
        TAuthorization tAuthorization = new TAuthorization();
        tAuthorization.setLabels(authorizations.getLabels());
        out.setAuthorizations(tAuthorization);
      }
    } catch (DeserializationException e) {
      throw new RuntimeException(e);
    }
    out.setConsistency(consistencyFromHBase(in.getConsistency()));
    out.setTargetReplicaId(in.getReplicaId());
    out.setCacheBlocks(in.getCacheBlocks());
    out.setStoreLimit(in.getMaxResultsPerColumnFamily());
    out.setStoreOffset(in.getRowOffsetPerColumnFamily());
    out.setExistence_only(in.isCheckExistenceOnly());
    for (Map.Entry<byte[], NavigableSet<byte[]>> family : in.getFamilyMap().entrySet()) {

      if (family.getValue() != null && !family.getValue().isEmpty()) {
        for (byte[] qualifier : family.getValue()) {
          TColumn column = new TColumn();
          column.setFamily(family.getKey());
          column.setQualifier(qualifier);
          out.addToColumns(column);
        }
      } else {
        TColumn column = new TColumn();
        column.setFamily(family.getKey());
        out.addToColumns(column);
      }
    }
    if (in.getFilter() != null) {
      try {
        out.setFilterBytes(filterFromHBase(in.getFilter()));
      } catch (IOException ioE) {
        throw new RuntimeException(ioE);
      }
    }
    return out;
  }

  public static Cell toCell(ExtendedCellBuilder cellBuilder, byte[] row, TColumnValue columnValue) {
    return cellBuilder.clear()
        .setRow(row)
        .setFamily(columnValue.getFamily())
        .setQualifier(columnValue.getQualifier())
        .setTimestamp(columnValue.getTimestamp())
        .setType(columnValue.getType())
        .setValue(columnValue.getValue())
        .setTags(columnValue.getTags())
        .build();
  }







  public static Result resultFromThrift(TResult in) {
    if (in == null) {
      return null;
    }
    if (!in.isSetColumnValues() || in.getColumnValues().isEmpty()){
      return in.isStale() ? EMPTY_RESULT_STALE : EMPTY_RESULT;
    }
    List<Cell> cells = new ArrayList<>(in.getColumnValues().size());
    ExtendedCellBuilder builder = ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
    for (TColumnValue columnValue : in.getColumnValues()) {
      cells.add(toCell(builder, in.getRow(), columnValue));
    }
    return Result.create(cells, null, in.isStale(), in.isPartial());
  }

  public static TPut putFromHBase(Put in) {
    TPut out = new TPut();
    out.setRow(in.getRow());
    if (in.getTimestamp() != HConstants.LATEST_TIMESTAMP) {
      out.setTimestamp(in.getTimestamp());
    }
    if (in.getDurability() != Durability.USE_DEFAULT) {
      out.setDurability(durabilityFromHBase(in.getDurability()));
    }
    for (Map.Entry<byte [], List<Cell>> entry : in.getFamilyCellMap().entrySet()) {
      byte[] family = entry.getKey();
      for (Cell cell : entry.getValue()) {
        TColumnValue columnValue = new TColumnValue();
        columnValue.setFamily(family)
            .setQualifier(CellUtil.cloneQualifier(cell))
            .setType(cell.getType().getCode())
            .setTimestamp(cell.getTimestamp())
            .setValue(CellUtil.cloneValue(cell));
        if (cell.getTagsLength() != 0) {
          columnValue.setTags(CellUtil.cloneTags(cell));
        }
        out.addToColumnValues(columnValue);
      }
    }
    for (Map.Entry<String, byte[]> attribute : in.getAttributesMap().entrySet()) {
      out.putToAttributes(ByteBuffer.wrap(Bytes.toBytes(attribute.getKey())),
          ByteBuffer.wrap(attribute.getValue()));
    }
    try {
      CellVisibility cellVisibility = in.getCellVisibility();
      if (cellVisibility != null) {
        TCellVisibility tCellVisibility = new TCellVisibility();
        tCellVisibility.setExpression(cellVisibility.getExpression());
        out.setCellVisibility(tCellVisibility);
      }
    } catch (DeserializationException e) {
      throw new RuntimeException(e);
    }
    return out;
  }

  public static List<TPut> putsFromHBase(List<Put> in) {
    List<TPut> out = new ArrayList<>(in.size());
    for (Put put : in) {
      out.add(putFromHBase(put));
    }
    return out;
  }

  public static NamespaceDescriptor[] namespaceDescriptorsFromThrift(
      List<TNamespaceDescriptor> in) {
    NamespaceDescriptor[] out = new NamespaceDescriptor[in.size()];
    int index = 0;
    for (TNamespaceDescriptor descriptor : in) {
      out[index++] = namespaceDescriptorFromThrift(descriptor);
    }
    return out;
  }

  public static List<TDelete> deletesFromHBase(List<Delete> in) {
    List<TDelete> out = new ArrayList<>(in.size());
    for (Delete delete : in) {
      out.add(deleteFromHBase(delete));
    }
    return out;
  }

  public static TAppend appendFromHBase(Append in) throws IOException {
    TAppend out = new TAppend();
    out.setRow(in.getRow());

    if (in.getDurability() != Durability.USE_DEFAULT) {
      out.setDurability(durabilityFromHBase(in.getDurability()));
    }
    for (Map.Entry<byte [], List<Cell>> entry : in.getFamilyCellMap().entrySet()) {
      byte[] family = entry.getKey();
      for (Cell cell : entry.getValue()) {
        TColumnValue columnValue = new TColumnValue();
        columnValue.setFamily(family)
            .setQualifier(CellUtil.cloneQualifier(cell))
            .setType(cell.getType().getCode())
            .setTimestamp(cell.getTimestamp())
            .setValue(CellUtil.cloneValue(cell));
        if (cell.getTagsLength() != 0) {
          columnValue.setTags(CellUtil.cloneTags(cell));
        }
        out.addToColumns(columnValue);
      }
    }
    for (Map.Entry<String, byte[]> attribute : in.getAttributesMap().entrySet()) {
      out.putToAttributes(ByteBuffer.wrap(Bytes.toBytes(attribute.getKey())),
          ByteBuffer.wrap(attribute.getValue()));
    }
    try {
      CellVisibility cellVisibility = in.getCellVisibility();
      if (cellVisibility != null) {
        TCellVisibility tCellVisibility = new TCellVisibility();
        tCellVisibility.setExpression(cellVisibility.getExpression());
        out.setCellVisibility(tCellVisibility);
      }
    } catch (DeserializationException e) {
      throw new RuntimeException(e);
    }
    out.setReturnResults(in.isReturnResults());
    return out;
  }

  public static TIncrement incrementFromHBase(Increment in) throws IOException {
    TIncrement out = new TIncrement();
    out.setRow(in.getRow());

    if (in.getDurability() != Durability.USE_DEFAULT) {
      out.setDurability(durabilityFromHBase(in.getDurability()));
    }
    for (Map.Entry<byte [], List<Cell>> entry : in.getFamilyCellMap().entrySet()) {
      byte[] family = entry.getKey();
      for (Cell cell : entry.getValue()) {
        TColumnIncrement columnValue = new TColumnIncrement();
        columnValue.setFamily(family).setQualifier(CellUtil.cloneQualifier(cell));
        columnValue.setAmount(
            Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        out.addToColumns(columnValue);
      }
    }
    for (Map.Entry<String, byte[]> attribute : in.getAttributesMap().entrySet()) {
      out.putToAttributes(ByteBuffer.wrap(Bytes.toBytes(attribute.getKey())),
          ByteBuffer.wrap(attribute.getValue()));
    }
    try {
      CellVisibility cellVisibility = in.getCellVisibility();
      if (cellVisibility != null) {
        TCellVisibility tCellVisibility = new TCellVisibility();
        tCellVisibility.setExpression(cellVisibility.getExpression());
        out.setCellVisibility(tCellVisibility);
      }
    } catch (DeserializationException e) {
      throw new RuntimeException(e);
    }
    out.setReturnResults(in.isReturnResults());
    return out;
  }

  public static TRowMutations rowMutationsFromHBase(RowMutations in) {
    TRowMutations tRowMutations = new TRowMutations();
    tRowMutations.setRow(in.getRow());
    for (Mutation mutation : in.getMutations()) {
      TMutation tMutation = new TMutation();
      if (mutation instanceof Put) {
        tMutation.setPut(ThriftUtilities.putFromHBase((Put)mutation));
      } else if (mutation instanceof Delete) {
        tMutation.setDeleteSingle(ThriftUtilities.deleteFromHBase((Delete)mutation));
      } else {
        throw new IllegalArgumentException(
            "Only Put and Delete is supported in mutateRow, but muation=" + mutation);
      }
      tRowMutations.addToMutations(tMutation);
    }
    return tRowMutations;
  }

  public static TCompareOp compareOpFromHBase(CompareOperator compareOp) {
    switch (compareOp) {
      case LESS: return TCompareOp.LESS;
      case LESS_OR_EQUAL: return TCompareOp.LESS_OR_EQUAL;
      case EQUAL: return TCompareOp.EQUAL;
      case NOT_EQUAL: return TCompareOp.NOT_EQUAL;
      case GREATER_OR_EQUAL: return TCompareOp.GREATER_OR_EQUAL;
      case GREATER: return TCompareOp.GREATER;
      case NO_OP: return TCompareOp.NO_OP;
      default: return null;
    }
  }
  public static List<ByteBuffer> splitKeyFromHBase(byte[][] in) {
    if (in == null || in.length == 0) {
      return null;
    }
    List<ByteBuffer> out = new ArrayList<>(in.length);
    for (byte[] key : in) {
      out.add(ByteBuffer.wrap(key));
    }
    return out;
  }

  public static Result[] resultsFromThrift(List<TResult> in) {
    Result[] out = new Result[in.size()];
    int index = 0;
    for (TResult tResult : in) {
      out[index++] = resultFromThrift(tResult);
    }
    return out;
  }

  public static List<TGet> getsFromHBase(List<Get> in) {
    List<TGet> out = new ArrayList<>(in.size());
    for (Get get : in) {
      out.add(getFromHBase(get));
    }
    return out;
  }

  public static Set<TServerName> getServerNamesFromHBase(Set<ServerName> serverNames) {
    if (CollectionUtils.isEmpty(serverNames)) {
      return Collections.emptySet();
    }
    return serverNames.stream().map(serverName -> {
      TServerName tServerName = new TServerName();
      tServerName.setHostName(serverName.getHostname());
      tServerName.setPort(serverName.getPort());
      tServerName.setStartCode(serverName.getStartcode());
      return tServerName;
    }).collect(Collectors.toSet());
  }

  public static Set<ServerName> getServerNamesFromThrift(Set<TServerName> tServerNames) {
    if (CollectionUtils.isEmpty(tServerNames)) {
      return Collections.emptySet();
    }
    return tServerNames.stream().map(tServerName ->
      ServerName.valueOf(tServerName.getHostName(),
        tServerName.getPort(),
        tServerName.getStartCode()))
      .collect(Collectors.toSet());
  }

  public static TLogQueryFilter getSlowLogQueryFromHBase(
      LogQueryFilter logQueryFilter) {
    TLogQueryFilter tLogQueryFilter = new TLogQueryFilter();
    tLogQueryFilter.setRegionName(logQueryFilter.getRegionName());
    tLogQueryFilter.setClientAddress(logQueryFilter.getClientAddress());
    tLogQueryFilter.setTableName(logQueryFilter.getTableName());
    tLogQueryFilter.setUserName(logQueryFilter.getUserName());
    tLogQueryFilter.setLimit(logQueryFilter.getLimit());
    TLogType tLogType = gettLogTypeFromHBase(logQueryFilter);
    tLogQueryFilter.setLogType(tLogType);
    TFilterByOperator tFilterByOperator = getTFilterByFromHBase(logQueryFilter);
    tLogQueryFilter.setFilterByOperator(tFilterByOperator);
    return tLogQueryFilter;
  }

  private static TLogType gettLogTypeFromHBase(final LogQueryFilter logQueryFilter) {
    TLogType tLogType;
    switch (logQueryFilter.getType()) {
      case SLOW_LOG: {
        tLogType = TLogType.SLOW_LOG;
        break;
      }
      case LARGE_LOG: {
        tLogType = TLogType.LARGE_LOG;
        break;
      }
      default: {
        tLogType = TLogType.SLOW_LOG;
      }
    }
    return tLogType;
  }

  private static TFilterByOperator getTFilterByFromHBase(final LogQueryFilter logQueryFilter) {
    TFilterByOperator tFilterByOperator;
    switch (logQueryFilter.getFilterByOperator()) {
      case AND: {
        tFilterByOperator = TFilterByOperator.AND;
        break;
      }
      case OR: {
        tFilterByOperator = TFilterByOperator.OR;
        break;
      }
      default: {
        tFilterByOperator = TFilterByOperator.OR;
      }
    }
    return tFilterByOperator;
  }

  public static LogQueryFilter getSlowLogQueryFromThrift(
      TLogQueryFilter tLogQueryFilter) {
    LogQueryFilter logQueryFilter = new LogQueryFilter();
    logQueryFilter.setRegionName(tLogQueryFilter.getRegionName());
    logQueryFilter.setClientAddress(tLogQueryFilter.getClientAddress());
    logQueryFilter.setTableName(tLogQueryFilter.getTableName());
    logQueryFilter.setUserName(tLogQueryFilter.getUserName());
    logQueryFilter.setLimit(tLogQueryFilter.getLimit());
    LogQueryFilter.Type type = getLogTypeFromThrift(tLogQueryFilter);
    logQueryFilter.setType(type);
    LogQueryFilter.FilterByOperator filterByOperator = getFilterByFromThrift(tLogQueryFilter);
    logQueryFilter.setFilterByOperator(filterByOperator);
    return logQueryFilter;
  }

  private static LogQueryFilter.Type getLogTypeFromThrift(
      final TLogQueryFilter tSlowLogQueryFilter) {
    LogQueryFilter.Type type;
    switch (tSlowLogQueryFilter.getLogType()) {
      case SLOW_LOG: {
        type = LogQueryFilter.Type.SLOW_LOG;
        break;
      }
      case LARGE_LOG: {
        type = LogQueryFilter.Type.LARGE_LOG;
        break;
      }
      default: {
        type = LogQueryFilter.Type.SLOW_LOG;
      }
    }
    return type;
  }

  private static LogQueryFilter.FilterByOperator getFilterByFromThrift(
      final TLogQueryFilter tLogQueryFilter) {
    LogQueryFilter.FilterByOperator filterByOperator;
    switch (tLogQueryFilter.getFilterByOperator()) {
      case AND: {
        filterByOperator = LogQueryFilter.FilterByOperator.AND;
        break;
      }
      case OR: {
        filterByOperator = LogQueryFilter.FilterByOperator.OR;
        break;
      }
      default: {
        filterByOperator = LogQueryFilter.FilterByOperator.OR;
      }
    }
    return filterByOperator;
  }

  public static List<TOnlineLogRecord> getSlowLogRecordsFromHBase(
      List<OnlineLogRecord> onlineLogRecords) {
    if (CollectionUtils.isEmpty(onlineLogRecords)) {
      return Collections.emptyList();
    }
    return onlineLogRecords.stream()
      .map(slowLogRecord -> {
        TOnlineLogRecord tOnlineLogRecord = new TOnlineLogRecord();
        tOnlineLogRecord.setCallDetails(slowLogRecord.getCallDetails());
        tOnlineLogRecord.setClientAddress(slowLogRecord.getClientAddress());
        tOnlineLogRecord.setMethodName(slowLogRecord.getMethodName());
        tOnlineLogRecord.setMultiGetsCount(slowLogRecord.getMultiGetsCount());
        tOnlineLogRecord.setMultiMutationsCount(slowLogRecord.getMultiMutationsCount());
        tOnlineLogRecord.setMultiServiceCalls(slowLogRecord.getMultiServiceCalls());
        tOnlineLogRecord.setParam(slowLogRecord.getParam());
        tOnlineLogRecord.setProcessingTime(slowLogRecord.getProcessingTime());
        tOnlineLogRecord.setQueueTime(slowLogRecord.getQueueTime());
        tOnlineLogRecord.setRegionName(slowLogRecord.getRegionName());
        tOnlineLogRecord.setResponseSize(slowLogRecord.getResponseSize());
        tOnlineLogRecord.setServerClass(slowLogRecord.getServerClass());
        tOnlineLogRecord.setStartTime(slowLogRecord.getStartTime());
        tOnlineLogRecord.setUserName(slowLogRecord.getUserName());
        return tOnlineLogRecord;
      }).collect(Collectors.toList());
  }

  public static List<OnlineLogRecord> getSlowLogRecordsFromThrift(
    List<TOnlineLogRecord> tOnlineLogRecords) {
    if (CollectionUtils.isEmpty(tOnlineLogRecords)) {
      return Collections.emptyList();
    }
    return tOnlineLogRecords.stream()
      .map(tSlowLogRecord -> new OnlineLogRecord.OnlineLogRecordBuilder()
        .setCallDetails(tSlowLogRecord.getCallDetails())
        .setClientAddress(tSlowLogRecord.getClientAddress())
        .setMethodName(tSlowLogRecord.getMethodName())
        .setMultiGetsCount(tSlowLogRecord.getMultiGetsCount())
        .setMultiMutationsCount(tSlowLogRecord.getMultiMutationsCount())
        .setMultiServiceCalls(tSlowLogRecord.getMultiServiceCalls())
        .setParam(tSlowLogRecord.getParam())
        .setProcessingTime(tSlowLogRecord.getProcessingTime())
        .setQueueTime(tSlowLogRecord.getQueueTime())
        .setRegionName(tSlowLogRecord.getRegionName())
        .setResponseSize(tSlowLogRecord.getResponseSize())
        .setServerClass(tSlowLogRecord.getServerClass())
        .setStartTime(tSlowLogRecord.getStartTime())
        .setUserName(tSlowLogRecord.getUserName())
        .build())
      .collect(Collectors.toList());
  }

}
