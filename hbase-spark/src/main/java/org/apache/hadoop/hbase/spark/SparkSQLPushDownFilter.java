/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.spark;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.ByteStringer;
import scala.collection.mutable.MutableList;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This filter will push down all qualifier logic given to us
 * by SparkSQL so that we have make the filters at the region server level
 * and avoid sending the data back to the client to be filtered.
 */
public class SparkSQLPushDownFilter extends FilterBase {
  protected static final Log log = LogFactory.getLog(SparkSQLPushDownFilter.class);

  HashMap<ColumnFamilyQualifierMapKeyWrapper, ColumnFilter> columnFamilyQualifierFilterMap;

  public SparkSQLPushDownFilter(HashMap<ColumnFamilyQualifierMapKeyWrapper,
          ColumnFilter> columnFamilyQualifierFilterMap) {
    this.columnFamilyQualifierFilterMap = columnFamilyQualifierFilterMap;
  }

  /**
   * This method will find the related filter logic for the given
   * column family and qualifier then execute it.  It will also
   * not clone the in coming cell to avoid extra object creation
   *
   * @param c            The Cell to be validated
   * @return             ReturnCode object to determine if skipping is required
   * @throws IOException
   */
  @Override
  public ReturnCode filterKeyValue(Cell c) throws IOException {

    //Get filter if one exist
    ColumnFilter filter =
            columnFamilyQualifierFilterMap.get(new ColumnFamilyQualifierMapKeyWrapper(
            c.getFamilyArray(),
            c.getFamilyOffset(),
            c.getFamilyLength(),
            c.getQualifierArray(),
            c.getQualifierOffset(),
            c.getQualifierLength()));

    if (filter == null) {
      //If no filter then just include values
      return ReturnCode.INCLUDE;
    } else {
      //If there is a filter then run validation
      if (filter.validate(c.getValueArray(), c.getValueOffset(), c.getValueLength())) {
        return ReturnCode.INCLUDE;
      }
    }
    //If validation fails then skip whole row
    return ReturnCode.NEXT_ROW;
  }

  /**
   * @param pbBytes A pb serialized instance
   * @return An instance of SparkSQLPushDownFilter
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   */
  @SuppressWarnings("unused")
  public static SparkSQLPushDownFilter parseFrom(final byte[] pbBytes)
          throws DeserializationException {

    HashMap<ColumnFamilyQualifierMapKeyWrapper,
            ColumnFilter> columnFamilyQualifierFilterMap = new HashMap<>();

    FilterProtos.SQLPredicatePushDownFilter proto;
    try {
      proto = FilterProtos.SQLPredicatePushDownFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    final List<FilterProtos.SQLPredicatePushDownColumnFilter> columnFilterListList =
            proto.getColumnFilterListList();

    for (FilterProtos.SQLPredicatePushDownColumnFilter columnFilter: columnFilterListList) {

      byte[] columnFamily = columnFilter.getColumnFamily().toByteArray();
      byte[] qualifier = columnFilter.getQualifier().toByteArray();
      final ColumnFamilyQualifierMapKeyWrapper columnFamilyQualifierMapKeyWrapper =
              new ColumnFamilyQualifierMapKeyWrapper(columnFamily, 0, columnFamily.length,
              qualifier, 0, qualifier.length);

      final MutableList<byte[]> points = new MutableList<>();
      final MutableList<ScanRange> scanRanges = new MutableList<>();

      for (ByteString byteString: columnFilter.getGetPointListList()) {
        points.$plus$eq(byteString.toByteArray());
      }

      for (FilterProtos.RowRange rowRange: columnFilter.getRangeListList()) {
        ScanRange scanRange = new ScanRange(rowRange.getStopRow().toByteArray(),
                rowRange.getStopRowInclusive(),
                rowRange.getStartRow().toByteArray(),
                rowRange.getStartRowInclusive());
        scanRanges.$plus$eq(scanRange);
      }

      final ColumnFilter columnFilterObj = new ColumnFilter(null, null, points, scanRanges);

      columnFamilyQualifierFilterMap.put(columnFamilyQualifierMapKeyWrapper, columnFilterObj);
    }

    return new SparkSQLPushDownFilter(columnFamilyQualifierFilterMap);
  }

  /**
   * @return The filter serialized using pb
   */
  public byte[] toByteArray() {

    FilterProtos.SQLPredicatePushDownFilter.Builder builder =
            FilterProtos.SQLPredicatePushDownFilter.newBuilder();

    FilterProtos.SQLPredicatePushDownColumnFilter.Builder columnBuilder =
            FilterProtos.SQLPredicatePushDownColumnFilter.newBuilder();

    FilterProtos.RowRange.Builder rowRangeBuilder = FilterProtos.RowRange.newBuilder();

    for (Map.Entry<ColumnFamilyQualifierMapKeyWrapper, ColumnFilter> entry :
            columnFamilyQualifierFilterMap.entrySet()) {

      columnBuilder.setColumnFamily(
              ByteStringer.wrap(entry.getKey().cloneColumnFamily()));
      columnBuilder.setQualifier(
              ByteStringer.wrap(entry.getKey().cloneQualifier()));

      final MutableList<byte[]> points = entry.getValue().points();

      int pointLength = points.length();
      for (int i = 0; i < pointLength; i++) {
        byte[] point = points.get(i).get();
        columnBuilder.addGetPointList(ByteStringer.wrap(point));

      }

      final MutableList<ScanRange> ranges = entry.getValue().ranges();
      int rangeLength = ranges.length();
      for (int i = 0; i < rangeLength; i++) {
        ScanRange scanRange = ranges.get(i).get();
        rowRangeBuilder.clear();
        rowRangeBuilder.setStartRow(ByteStringer.wrap(scanRange.lowerBound()));
        rowRangeBuilder.setStopRow(ByteStringer.wrap(scanRange.upperBound()));
        rowRangeBuilder.setStartRowInclusive(scanRange.isLowerBoundEqualTo());
        rowRangeBuilder.setStopRowInclusive(scanRange.isUpperBoundEqualTo());

        columnBuilder.addRangeList(rowRangeBuilder.build());
      }

      builder.addColumnFilterList(columnBuilder.build());
      columnBuilder.clear();
    }
    return builder.build().toByteArray();
  }
}
