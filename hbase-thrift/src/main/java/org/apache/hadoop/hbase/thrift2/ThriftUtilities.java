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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.thrift2.generated.TAppend;
import org.apache.hadoop.hbase.thrift2.generated.TColumn;
import org.apache.hadoop.hbase.thrift2.generated.TColumnIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TDelete;
import org.apache.hadoop.hbase.thrift2.generated.TDeleteType;
import org.apache.hadoop.hbase.thrift2.generated.TDurability;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.TIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TMutation;
import org.apache.hadoop.hbase.thrift2.generated.TPut;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TRowMutations;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.hadoop.hbase.thrift2.generated.TTimeRange;
import org.apache.hadoop.hbase.util.Bytes;

@InterfaceAudience.Private
public class ThriftUtilities {

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
      out.setTimeStamp(in.getTimestamp());
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
    List<Get> out = new ArrayList<Get>(in.size());
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
    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    for (Cell kv : raw) {
      TColumnValue col = new TColumnValue();
      col.setFamily(CellUtil.cloneFamily(kv));
      col.setQualifier(CellUtil.cloneQualifier(kv));
      col.setTimestamp(kv.getTimestamp());
      col.setValue(CellUtil.cloneValue(kv));
      if (kv.getTagsLength() > 0) {
        col.setTags(CellUtil.getTagArray(kv));
      }
      columnValues.add(col);
    }
    out.setColumnValues(columnValues);
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
    List<TResult> out = new ArrayList<TResult>(in.length);
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
      if (columnValue.isSetTimestamp()) {
        out.addImmutable(
            columnValue.getFamily(), columnValue.getQualifier(), columnValue.getTimestamp(),
            columnValue.getValue());
      } else {
        out.addImmutable(
            columnValue.getFamily(), columnValue.getQualifier(), columnValue.getValue());
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
    List<Put> out = new ArrayList<Put>(in.size());
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
        if (column.isSetQualifier()) {
          if (column.isSetTimestamp()) {
            if (in.isSetDeleteType() &&
                in.getDeleteType().equals(TDeleteType.DELETE_COLUMNS))
              out.deleteColumns(column.getFamily(), column.getQualifier(), column.getTimestamp());
            else
              out.deleteColumn(column.getFamily(), column.getQualifier(), column.getTimestamp());
          } else {
            if (in.isSetDeleteType() &&
                in.getDeleteType().equals(TDeleteType.DELETE_COLUMNS))
              out.deleteColumns(column.getFamily(), column.getQualifier());
            else
              out.deleteColumn(column.getFamily(), column.getQualifier());
          }

        } else {
          if (column.isSetTimestamp()) {
            out.deleteFamily(column.getFamily(), column.getTimestamp());
          } else {
            out.deleteFamily(column.getFamily());
          }
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
    List<Delete> out = new ArrayList<Delete>(in.size());
    for (TDelete delete : in) {
      out.add(deleteFromThrift(delete));
    }
    return out;
  }

  public static TDelete deleteFromHBase(Delete in) {
    TDelete out = new TDelete(ByteBuffer.wrap(in.getRow()));

    List<TColumn> columns = new ArrayList<TColumn>();
    long rowTimestamp = in.getTimeStamp();
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
    RowMutations out = new RowMutations(in.getRow());
    List<TMutation> mutations = in.getMutations();
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
      out.add(column.getFamily(), column.getQualifier(), column.getValue());
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

  /**
   * Adds all the attributes into the Operation object
   */
  private static void addAttributes(OperationWithAttributes op,
                                    Map<ByteBuffer, ByteBuffer> attributes) {
    if (attributes == null || attributes.size() == 0) {
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
}
