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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.spark.datasources.BytesEncoder;
import org.apache.hadoop.hbase.spark.datasources.JavaBytesEncoder;
import org.apache.hadoop.hbase.spark.protobuf.generated.SparkFilterProtos;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.datasources.hbase.Field;
import scala.collection.mutable.MutableList;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ByteString;

/**
 * This filter will push down all qualifier logic given to us
 * by SparkSQL so that we have make the filters at the region server level
 * and avoid sending the data back to the client to be filtered.
 */
public class SparkSQLPushDownFilter extends FilterBase{
  protected static final Log log = LogFactory.getLog(SparkSQLPushDownFilter.class);

  //The following values are populated with protobuffer
  DynamicLogicExpression dynamicLogicExpression;
  byte[][] valueFromQueryArray;
  HashMap<ByteArrayComparable, HashMap<ByteArrayComparable, String>>
          currentCellToColumnIndexMap;

  //The following values are transient
  HashMap<String, ByteArrayComparable> columnToCurrentRowValueMap = null;

  static final byte[] rowKeyFamily = new byte[0];
  static final byte[] rowKeyQualifier = Bytes.toBytes("key");

  String encoderClassName;

  public SparkSQLPushDownFilter(DynamicLogicExpression dynamicLogicExpression,
                                byte[][] valueFromQueryArray,
                                HashMap<ByteArrayComparable,
                                        HashMap<ByteArrayComparable, String>>
                                        currentCellToColumnIndexMap, String encoderClassName) {
    this.dynamicLogicExpression = dynamicLogicExpression;
    this.valueFromQueryArray = valueFromQueryArray;
    this.currentCellToColumnIndexMap = currentCellToColumnIndexMap;
    this.encoderClassName = encoderClassName;
  }

  public SparkSQLPushDownFilter(DynamicLogicExpression dynamicLogicExpression,
                                byte[][] valueFromQueryArray,
                                MutableList<Field> fields, String encoderClassName) {
    this.dynamicLogicExpression = dynamicLogicExpression;
    this.valueFromQueryArray = valueFromQueryArray;
    this.encoderClassName = encoderClassName;

    //generate family qualifier to index mapping
    this.currentCellToColumnIndexMap =
            new HashMap<>();

    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.apply(i);

      byte[] cfBytes = field.cfBytes();
      ByteArrayComparable familyByteComparable =
          new ByteArrayComparable(cfBytes, 0, cfBytes.length);

      HashMap<ByteArrayComparable, String> qualifierIndexMap =
              currentCellToColumnIndexMap.get(familyByteComparable);

      if (qualifierIndexMap == null) {
        qualifierIndexMap = new HashMap<>();
        currentCellToColumnIndexMap.put(familyByteComparable, qualifierIndexMap);
      }
      byte[] qBytes = field.colBytes();
      ByteArrayComparable qualifierByteComparable =
          new ByteArrayComparable(qBytes, 0, qBytes.length);

      qualifierIndexMap.put(qualifierByteComparable, field.colName());
    }
  }

  @Override
  public ReturnCode filterKeyValue(Cell c) throws IOException {

    //If the map RowValueMap is empty then we need to populate
    // the row key
    if (columnToCurrentRowValueMap == null) {
      columnToCurrentRowValueMap = new HashMap<>();
      HashMap<ByteArrayComparable, String> qualifierColumnMap =
              currentCellToColumnIndexMap.get(
                      new ByteArrayComparable(rowKeyFamily, 0, rowKeyFamily.length));

      if (qualifierColumnMap != null) {
        String rowKeyColumnName =
                qualifierColumnMap.get(
                        new ByteArrayComparable(rowKeyQualifier, 0,
                                rowKeyQualifier.length));
        //Make sure that the rowKey is part of the where clause
        if (rowKeyColumnName != null) {
          columnToCurrentRowValueMap.put(rowKeyColumnName,
                  new ByteArrayComparable(c.getRowArray(),
                          c.getRowOffset(), c.getRowLength()));
        }
      }
    }

    //Always populate the column value into the RowValueMap
    ByteArrayComparable currentFamilyByteComparable =
            new ByteArrayComparable(c.getFamilyArray(),
            c.getFamilyOffset(),
            c.getFamilyLength());

    HashMap<ByteArrayComparable, String> qualifierColumnMap =
            currentCellToColumnIndexMap.get(
                    currentFamilyByteComparable);

    if (qualifierColumnMap != null) {

      String columnName =
              qualifierColumnMap.get(
                      new ByteArrayComparable(c.getQualifierArray(),
                              c.getQualifierOffset(),
                              c.getQualifierLength()));

      if (columnName != null) {
        columnToCurrentRowValueMap.put(columnName,
                new ByteArrayComparable(c.getValueArray(),
                        c.getValueOffset(), c.getValueLength()));
      }
    }

    return ReturnCode.INCLUDE;
  }


  @Override
  public boolean filterRow() throws IOException {

    try {
      boolean result =
              dynamicLogicExpression.execute(columnToCurrentRowValueMap,
                      valueFromQueryArray);
      columnToCurrentRowValueMap = null;
      return !result;
    } catch (Throwable e) {
      log.error("Error running dynamic logic on row", e);
    }
    return false;
  }


  /**
   * @param pbBytes A pb serialized instance
   * @return An instance of SparkSQLPushDownFilter
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   */
  @SuppressWarnings("unused")
  public static SparkSQLPushDownFilter parseFrom(final byte[] pbBytes)
          throws DeserializationException {

    SparkFilterProtos.SQLPredicatePushDownFilter proto;
    try {
      proto = SparkFilterProtos.SQLPredicatePushDownFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }

    String encoder = proto.getEncoderClassName();
    BytesEncoder enc = JavaBytesEncoder.create(encoder);

    //Load DynamicLogicExpression
    DynamicLogicExpression dynamicLogicExpression =
            DynamicLogicExpressionBuilder.build(proto.getDynamicLogicExpression(), enc);

    //Load valuesFromQuery
    final List<ByteString> valueFromQueryArrayList = proto.getValueFromQueryArrayList();
    byte[][] valueFromQueryArray = new byte[valueFromQueryArrayList.size()][];
    for (int i = 0; i < valueFromQueryArrayList.size(); i++) {
      valueFromQueryArray[i] = valueFromQueryArrayList.get(i).toByteArray();
    }

    //Load mapping from HBase family/qualifier to Spark SQL columnName
    HashMap<ByteArrayComparable, HashMap<ByteArrayComparable, String>>
            currentCellToColumnIndexMap = new HashMap<>();

    for (SparkFilterProtos.SQLPredicatePushDownCellToColumnMapping
            sqlPredicatePushDownCellToColumnMapping :
            proto.getCellToColumnMappingList()) {

      byte[] familyArray =
              sqlPredicatePushDownCellToColumnMapping.getColumnFamily().toByteArray();
      ByteArrayComparable familyByteComparable =
              new ByteArrayComparable(familyArray, 0, familyArray.length);
      HashMap<ByteArrayComparable, String> qualifierMap =
              currentCellToColumnIndexMap.get(familyByteComparable);

      if (qualifierMap == null) {
        qualifierMap = new HashMap<>();
        currentCellToColumnIndexMap.put(familyByteComparable, qualifierMap);
      }
      byte[] qualifierArray =
              sqlPredicatePushDownCellToColumnMapping.getQualifier().toByteArray();

      ByteArrayComparable qualifierByteComparable =
              new ByteArrayComparable(qualifierArray, 0 ,qualifierArray.length);

      qualifierMap.put(qualifierByteComparable,
              sqlPredicatePushDownCellToColumnMapping.getColumnName());
    }

    return new SparkSQLPushDownFilter(dynamicLogicExpression,
            valueFromQueryArray, currentCellToColumnIndexMap, encoder);
  }

  /**
   * @return The filter serialized using pb
   */
  public byte[] toByteArray() {

    SparkFilterProtos.SQLPredicatePushDownFilter.Builder builder =
            SparkFilterProtos.SQLPredicatePushDownFilter.newBuilder();

    SparkFilterProtos.SQLPredicatePushDownCellToColumnMapping.Builder columnMappingBuilder =
            SparkFilterProtos.SQLPredicatePushDownCellToColumnMapping.newBuilder();

    builder.setDynamicLogicExpression(dynamicLogicExpression.toExpressionString());
    for (byte[] valueFromQuery: valueFromQueryArray) {
      builder.addValueFromQueryArray(ByteStringer.wrap(valueFromQuery));
    }

    for (Map.Entry<ByteArrayComparable, HashMap<ByteArrayComparable, String>>
            familyEntry : currentCellToColumnIndexMap.entrySet()) {
      for (Map.Entry<ByteArrayComparable, String> qualifierEntry :
              familyEntry.getValue().entrySet()) {
        columnMappingBuilder.setColumnFamily(
                ByteStringer.wrap(familyEntry.getKey().bytes()));
        columnMappingBuilder.setQualifier(
                ByteStringer.wrap(qualifierEntry.getKey().bytes()));
        columnMappingBuilder.setColumnName(qualifierEntry.getValue());
        builder.addCellToColumnMapping(columnMappingBuilder.build());
      }
    }
    builder.setEncoderClassName(encoderClassName);


    return builder.build().toByteArray();
  }
}
