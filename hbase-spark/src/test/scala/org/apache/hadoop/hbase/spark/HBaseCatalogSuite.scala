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

package org.apache.hadoop.hbase.spark

import org.apache.hadoop.hbase.spark.datasources.{DoubleSerDes, SerDes}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.apache.spark.sql.datasources.hbase.{DataTypeParserWrapper, HBaseTableCatalog}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class HBaseCatalogSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll  with Logging {

  val map = s"""MAP<int, struct<varchar:string>>"""
  val array = s"""array<struct<tinYint:tinyint>>"""
  val arrayMap = s"""MAp<int, ARRAY<double>>"""
  val catalog = s"""{
                    |"table":{"namespace":"default", "name":"htable"},
                    |"rowkey":"key1:key2",
                    |"columns":{
                    |"col1":{"cf":"rowkey", "col":"key1", "type":"string"},
                    |"col2":{"cf":"rowkey", "col":"key2", "type":"double"},
                    |"col3":{"cf":"cf1", "col":"col2", "type":"binary"},
                    |"col4":{"cf":"cf1", "col":"col3", "type":"timestamp"},
                    |"col5":{"cf":"cf1", "col":"col4", "type":"double", "serdes":"${classOf[DoubleSerDes].getName}"},
                    |"col6":{"cf":"cf1", "col":"col5", "type":"$map"},
                    |"col7":{"cf":"cf1", "col":"col6", "type":"$array"},
                    |"col8":{"cf":"cf1", "col":"col7", "type":"$arrayMap"}
                    |}
                    |}""".stripMargin
  val parameters = Map(HBaseTableCatalog.tableCatalog->catalog)
  val t = HBaseTableCatalog(parameters)

  def checkDataType(dataTypeString: String, expectedDataType: DataType): Unit = {
    test(s"parse ${dataTypeString.replace("\n", "")}") {
      assert(DataTypeParserWrapper.parse(dataTypeString) === expectedDataType)
    }
  }
  test("basic") {
    assert(t.getField("col1").isRowKey == true)
    assert(t.getPrimaryKey == "key1")
    assert(t.getField("col3").dt == BinaryType)
    assert(t.getField("col4").dt == TimestampType)
    assert(t.getField("col5").dt == DoubleType)
    assert(t.getField("col5").serdes != None)
    assert(t.getField("col4").serdes == None)
    assert(t.getField("col1").isRowKey)
    assert(t.getField("col2").isRowKey)
    assert(!t.getField("col3").isRowKey)
    assert(t.getField("col2").length == Bytes.SIZEOF_DOUBLE)
    assert(t.getField("col1").length == -1)
    assert(t.getField("col8").length == -1)
  }

  checkDataType(
    map,
    t.getField("col6").dt
  )

  checkDataType(
    array,
    t.getField("col7").dt
  )

  checkDataType(
    arrayMap,
    t.getField("col8").dt
  )

  test("convert") {
    val m = Map("hbase.columns.mapping" ->
      "KEY_FIELD STRING :key, A_FIELD STRING c:a, B_FIELD DOUBLE c:b, C_FIELD BINARY c:c,",
      "hbase.table" -> "t1")
    val map = HBaseTableCatalog.convert(m)
    val json = map.get(HBaseTableCatalog.tableCatalog).get
    val parameters = Map(HBaseTableCatalog.tableCatalog->json)
    val t = HBaseTableCatalog(parameters)
    assert(t.getField("KEY_FIELD").isRowKey)
    assert(DataTypeParserWrapper.parse("STRING") === t.getField("A_FIELD").dt)
    assert(!t.getField("A_FIELD").isRowKey)
    assert(DataTypeParserWrapper.parse("DOUBLE") === t.getField("B_FIELD").dt)
    assert(DataTypeParserWrapper.parse("BINARY") === t.getField("C_FIELD").dt)
  }

  test("compatiblity") {
    val m = Map("hbase.columns.mapping" ->
      "KEY_FIELD STRING :key, A_FIELD STRING c:a, B_FIELD DOUBLE c:b, C_FIELD BINARY c:c,",
      "hbase.table" -> "t1")
    val t = HBaseTableCatalog(m)
    assert(t.getField("KEY_FIELD").isRowKey)
    assert(DataTypeParserWrapper.parse("STRING") === t.getField("A_FIELD").dt)
    assert(!t.getField("A_FIELD").isRowKey)
    assert(DataTypeParserWrapper.parse("DOUBLE") === t.getField("B_FIELD").dt)
    assert(DataTypeParserWrapper.parse("BINARY") === t.getField("C_FIELD").dt)
  }
}
