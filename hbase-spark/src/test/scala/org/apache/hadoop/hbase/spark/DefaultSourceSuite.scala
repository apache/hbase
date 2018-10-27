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

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.spark.datasources.{HBaseSparkConf, HBaseTableCatalog}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseTestingUtility, TableName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.xml.sax.SAXParseException

case class HBaseRecord(
  col0: String,
  col1: Boolean,
  col2: Double,
  col3: Float,
  col4: Int,
  col5: Long,
  col6: Short,
  col7: String,
  col8: Byte)

object HBaseRecord {
  def apply(i: Int, t: String): HBaseRecord = {
    val s = s"""row${"%03d".format(i)}"""
    HBaseRecord(s,
      i % 2 == 0,
      i.toDouble,
      i.toFloat,
      i,
      i.toLong,
      i.toShort,
      s"String$i: $t",
      i.toByte)
  }
}


case class AvroHBaseKeyRecord(col0: Array[Byte],
                              col1: Array[Byte])

object AvroHBaseKeyRecord {
  val schemaString =
    s"""{"namespace": "example.avro",
        |   "type": "record",      "name": "User",
        |    "fields": [      {"name": "name", "type": "string"},
        |      {"name": "favorite_number",  "type": ["int", "null"]},
        |        {"name": "favorite_color", "type": ["string", "null"]}      ]    }""".stripMargin

  val avroSchema: Schema = {
    val p = new Schema.Parser
    p.parse(schemaString)
  }

  def apply(i: Int): AvroHBaseKeyRecord = {
    val user = new GenericData.Record(avroSchema);
    user.put("name", s"name${"%03d".format(i)}")
    user.put("favorite_number", i)
    user.put("favorite_color", s"color${"%03d".format(i)}")
    val avroByte = AvroSerdes.serialize(user, avroSchema)
    AvroHBaseKeyRecord(avroByte, avroByte)
  }
}

class DefaultSourceSuite extends FunSuite with
BeforeAndAfterEach with BeforeAndAfterAll with Logging {
  @transient var sc: SparkContext = null
  var TEST_UTIL: HBaseTestingUtility = new HBaseTestingUtility

  val t1TableName = "t1"
  val t2TableName = "t2"
  val columnFamily = "c"

  var sqlContext:SQLContext = null
  var df:DataFrame = null

  override def beforeAll() {

    TEST_UTIL.startMiniCluster

    logInfo(" - minicluster started")
    try
      TEST_UTIL.deleteTable(TableName.valueOf(t1TableName))
    catch {
      case e: Exception => logInfo(" - no table " + t1TableName + " found")
    }
    try
      TEST_UTIL.deleteTable(TableName.valueOf(t2TableName))
    catch {
      case e: Exception => logInfo(" - no table " + t2TableName + " found")
    }
    logInfo(" - creating table " + t1TableName)
    TEST_UTIL.createTable(TableName.valueOf(t1TableName), Bytes.toBytes(columnFamily))
    logInfo(" - created table")
    logInfo(" - creating table " + t2TableName)
    TEST_UTIL.createTable(TableName.valueOf(t2TableName), Bytes.toBytes(columnFamily))
    logInfo(" - created table")
    val sparkConf = new SparkConf
    sparkConf.set(HBaseSparkConf.QUERY_CACHEBLOCKS, "true")
    sparkConf.set(HBaseSparkConf.QUERY_BATCHSIZE, "100")
    sparkConf.set(HBaseSparkConf.QUERY_CACHEDROWS, "100")

    sc  = new SparkContext("local", "test", sparkConf)

    val connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration)
    try {
      val t1Table = connection.getTable(TableName.valueOf("t1"))

      try {
        var put = new Put(Bytes.toBytes("get1"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo1"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("1"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(1))
        t1Table.put(put)
        put = new Put(Bytes.toBytes("get2"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo2"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("4"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(4))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("z"), Bytes.toBytes("FOO"))
        t1Table.put(put)
        put = new Put(Bytes.toBytes("get3"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo3"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("8"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(8))
        t1Table.put(put)
        put = new Put(Bytes.toBytes("get4"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo4"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("10"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(10))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("z"), Bytes.toBytes("BAR"))
        t1Table.put(put)
        put = new Put(Bytes.toBytes("get5"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo5"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("8"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(8))
        t1Table.put(put)
      } finally {
        t1Table.close()
      }

      val t2Table = connection.getTable(TableName.valueOf("t2"))

      try {
        var put = new Put(Bytes.toBytes(1))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo1"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("1"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(1))
        t2Table.put(put)
        put = new Put(Bytes.toBytes(2))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo2"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("4"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(4))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("z"), Bytes.toBytes("FOO"))
        t2Table.put(put)
        put = new Put(Bytes.toBytes(3))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo3"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("8"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(8))
        t2Table.put(put)
        put = new Put(Bytes.toBytes(4))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo4"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("10"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(10))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("z"), Bytes.toBytes("BAR"))
        t2Table.put(put)
        put = new Put(Bytes.toBytes(5))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo5"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("8"))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(8))
        t2Table.put(put)
      } finally {
        t2Table.close()
      }
    } finally {
      connection.close()
    }

    def hbaseTable1Catalog = s"""{
            |"table":{"namespace":"default", "name":"t1"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"B_FIELD":{"cf":"c", "col":"b", "type":"string"}
            |}
          |}""".stripMargin

    new HBaseContext(sc, TEST_UTIL.getConfiguration)
    sqlContext = new SQLContext(sc)

    df = sqlContext.load("org.apache.hadoop.hbase.spark",
      Map(HBaseTableCatalog.tableCatalog->hbaseTable1Catalog))

    df.registerTempTable("hbaseTable1")

    def hbaseTable2Catalog = s"""{
            |"table":{"namespace":"default", "name":"t2"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"int"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"B_FIELD":{"cf":"c", "col":"b", "type":"string"}
            |}
          |}""".stripMargin


    df = sqlContext.load("org.apache.hadoop.hbase.spark",
      Map(HBaseTableCatalog.tableCatalog->hbaseTable2Catalog))

    df.registerTempTable("hbaseTable2")
  }

  override def afterAll() {
    TEST_UTIL.deleteTable(TableName.valueOf(t1TableName))
    logInfo("shuting down minicluster")
    TEST_UTIL.shutdownMiniCluster()

    sc.stop()
  }

  override def beforeEach(): Unit = {
    DefaultSourceStaticUtils.lastFiveExecutionRules.clear()
  }


  /**
   * A example of query three fields and also only using rowkey points for the filter
   */
  test("Test rowKey point only rowKey query") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
      "WHERE " +
      "(KEY_FIELD = 'get1' or KEY_FIELD = 'get2' or KEY_FIELD = 'get3')").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 3)

    assert(executionRules.dynamicLogicExpression.toExpressionString.
      equals("( ( KEY_FIELD == 0 OR KEY_FIELD == 1 ) OR KEY_FIELD == 2 )"))

    assert(executionRules.rowKeyFilter.points.size == 3)
    assert(executionRules.rowKeyFilter.ranges.size == 0)
  }

  /**
   * A example of query three fields and also only using cell points for the filter
   */
  test("Test cell point only rowKey query") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
      "WHERE " +
      "(B_FIELD = '4' or B_FIELD = '10' or A_FIELD = 'foo1')").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 3)

    assert(executionRules.dynamicLogicExpression.toExpressionString.
      equals("( ( B_FIELD == 0 OR B_FIELD == 1 ) OR A_FIELD == 2 )"))
  }

  /**
   * A example of a OR merge between to ranges the result is one range
   * Also an example of less then and greater then
   */
  test("Test two range rowKey query") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
      "WHERE " +
      "( KEY_FIELD < 'get2' or KEY_FIELD > 'get3')").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 3)

    assert(executionRules.dynamicLogicExpression.toExpressionString.
      equals("( KEY_FIELD < 0 OR KEY_FIELD > 1 )"))

    assert(executionRules.rowKeyFilter.points.size == 0)
    assert(executionRules.rowKeyFilter.ranges.size == 2)

    val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
    assert(Bytes.equals(scanRange1.lowerBound,Bytes.toBytes("")))
    assert(Bytes.equals(scanRange1.upperBound,Bytes.toBytes("get2")))
    assert(scanRange1.isLowerBoundEqualTo)
    assert(!scanRange1.isUpperBoundEqualTo)

    val scanRange2 = executionRules.rowKeyFilter.ranges.get(1).get
    assert(Bytes.equals(scanRange2.lowerBound,Bytes.toBytes("get3")))
    assert(scanRange2.upperBound == null)
    assert(!scanRange2.isLowerBoundEqualTo)
    assert(scanRange2.isUpperBoundEqualTo)
  }

  /**
   * A example of a OR merge between to ranges the result is one range
   * Also an example of less then and greater then
   *
   * This example makes sure the code works for a int rowKey
   */
  test("Test two range rowKey query where the rowKey is Int and there is a range over lap") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable2 " +
      "WHERE " +
      "( KEY_FIELD < 4 or KEY_FIELD > 2)").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()



    assert(executionRules.dynamicLogicExpression.toExpressionString.
      equals("( KEY_FIELD < 0 OR KEY_FIELD > 1 )"))

    assert(executionRules.rowKeyFilter.points.size == 0)
    assert(executionRules.rowKeyFilter.ranges.size == 2)
    assert(results.length == 5)
  }

  /**
   * A example of a OR merge between to ranges the result is two ranges
   * Also an example of less then and greater then
   *
   * This example makes sure the code works for a int rowKey
   */
  test("Test two range rowKey query where the rowKey is Int and the ranges don't over lap") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable2 " +
      "WHERE " +
      "( KEY_FIELD < 2 or KEY_FIELD > 4)").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(executionRules.dynamicLogicExpression.toExpressionString.
      equals("( KEY_FIELD < 0 OR KEY_FIELD > 1 )"))

    assert(executionRules.rowKeyFilter.points.size == 0)

    assert(executionRules.rowKeyFilter.ranges.size == 3)

    val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
    assert(Bytes.equals(scanRange1.upperBound, Bytes.toBytes(2)))
    assert(scanRange1.isLowerBoundEqualTo)
    assert(!scanRange1.isUpperBoundEqualTo)

    val scanRange2 = executionRules.rowKeyFilter.ranges.get(1).get
    assert(scanRange2.isUpperBoundEqualTo)

    assert(results.length == 2)
  }

  /**
   * A example of a AND merge between to ranges the result is one range
   * Also an example of less then and equal to and greater then and equal to
   */
  test("Test one combined range rowKey query") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
      "WHERE " +
      "(KEY_FIELD <= 'get3' and KEY_FIELD >= 'get2')").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 2)

    val expr = executionRules.dynamicLogicExpression.toExpressionString
    assert(expr.equals("( ( KEY_FIELD isNotNull AND KEY_FIELD <= 0 ) AND KEY_FIELD >= 1 )"), expr)

    assert(executionRules.rowKeyFilter.points.size == 0)
    assert(executionRules.rowKeyFilter.ranges.size == 1)

    val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
    assert(Bytes.equals(scanRange1.lowerBound,Bytes.toBytes("get2")))
    assert(Bytes.equals(scanRange1.upperBound, Bytes.toBytes("get3")))
    assert(scanRange1.isLowerBoundEqualTo)
    assert(scanRange1.isUpperBoundEqualTo)

  }

  /**
   * Do a select with no filters
   */
  test("Test select only query") {

    val results = df.select("KEY_FIELD").take(10)
    assert(results.length == 5)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(executionRules.dynamicLogicExpression == null)

  }

  /**
   * A complex query with one point and one range for both the
   * rowKey and the a column
   */
  test("Test SQL point and range combo") {
    val results = sqlContext.sql("SELECT KEY_FIELD FROM hbaseTable1 " +
      "WHERE " +
      "(KEY_FIELD = 'get1' and B_FIELD < '3') or " +
      "(KEY_FIELD >= 'get3' and B_FIELD = '8')").take(5)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(executionRules.dynamicLogicExpression.toExpressionString.
      equals("( ( KEY_FIELD == 0 AND B_FIELD < 1 ) OR " +
      "( KEY_FIELD >= 2 AND B_FIELD == 3 ) )"))

    assert(executionRules.rowKeyFilter.points.size == 1)
    assert(executionRules.rowKeyFilter.ranges.size == 1)

    val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
    assert(Bytes.equals(scanRange1.lowerBound,Bytes.toBytes("get3")))
    assert(scanRange1.upperBound == null)
    assert(scanRange1.isLowerBoundEqualTo)
    assert(scanRange1.isUpperBoundEqualTo)


    assert(results.length == 3)
  }

  /**
   * A complex query with two complex ranges that doesn't merge into one
   */
  test("Test two complete range non merge rowKey query") {

    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable2 " +
      "WHERE " +
      "( KEY_FIELD >= 1 and KEY_FIELD <= 2) or" +
      "( KEY_FIELD > 3 and KEY_FIELD <= 5)").take(10)


    assert(results.length == 4)
    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()
    assert(executionRules.dynamicLogicExpression.toExpressionString.
      equals("( ( KEY_FIELD >= 0 AND KEY_FIELD <= 1 ) OR " +
      "( KEY_FIELD > 2 AND KEY_FIELD <= 3 ) )"))

    assert(executionRules.rowKeyFilter.points.size == 0)
    assert(executionRules.rowKeyFilter.ranges.size == 2)

    val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
    assert(Bytes.equals(scanRange1.lowerBound,Bytes.toBytes(1)))
    assert(Bytes.equals(scanRange1.upperBound, Bytes.toBytes(2)))
    assert(scanRange1.isLowerBoundEqualTo)
    assert(scanRange1.isUpperBoundEqualTo)

    val scanRange2 = executionRules.rowKeyFilter.ranges.get(1).get
    assert(Bytes.equals(scanRange2.lowerBound,Bytes.toBytes(3)))
    assert(Bytes.equals(scanRange2.upperBound, Bytes.toBytes(5)))
    assert(!scanRange2.isLowerBoundEqualTo)
    assert(scanRange2.isUpperBoundEqualTo)

  }

  /**
   * A complex query with two complex ranges that does merge into one
   */
  test("Test two complete range merge rowKey query") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
      "WHERE " +
      "( KEY_FIELD >= 'get1' and KEY_FIELD <= 'get2') or" +
      "( KEY_FIELD > 'get3' and KEY_FIELD <= 'get5')").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 4)

    assert(executionRules.dynamicLogicExpression.toExpressionString.
      equals("( ( KEY_FIELD >= 0 AND KEY_FIELD <= 1 ) OR " +
      "( KEY_FIELD > 2 AND KEY_FIELD <= 3 ) )"))

    assert(executionRules.rowKeyFilter.points.size == 0)
    assert(executionRules.rowKeyFilter.ranges.size == 2)

    val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
    assert(Bytes.equals(scanRange1.lowerBound,Bytes.toBytes("get1")))
    assert(Bytes.equals(scanRange1.upperBound, Bytes.toBytes("get2")))
    assert(scanRange1.isLowerBoundEqualTo)
    assert(scanRange1.isUpperBoundEqualTo)

    val scanRange2 = executionRules.rowKeyFilter.ranges.get(1).get
    assert(Bytes.equals(scanRange2.lowerBound, Bytes.toBytes("get3")))
    assert(Bytes.equals(scanRange2.upperBound, Bytes.toBytes("get5")))
    assert(!scanRange2.isLowerBoundEqualTo)
    assert(scanRange2.isUpperBoundEqualTo)
  }

  test("Test OR logic with a one RowKey and One column") {

    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
      "WHERE " +
      "( KEY_FIELD >= 'get1' or A_FIELD <= 'foo2') or" +
      "( KEY_FIELD > 'get3' or B_FIELD <= '4')").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 5)

    assert(executionRules.dynamicLogicExpression.toExpressionString.
      equals("( ( KEY_FIELD >= 0 OR A_FIELD <= 1 ) OR " +
      "( KEY_FIELD > 2 OR B_FIELD <= 3 ) )"))

    assert(executionRules.rowKeyFilter.points.size == 0)
    assert(executionRules.rowKeyFilter.ranges.size == 1)

    val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
    //This is the main test for 14406
    //Because the key is joined through a or with a qualifier
    //There is no filter on the rowKey
    assert(Bytes.equals(scanRange1.lowerBound,Bytes.toBytes("")))
    assert(scanRange1.upperBound == null)
    assert(scanRange1.isLowerBoundEqualTo)
    assert(scanRange1.isUpperBoundEqualTo)
  }

  test("Test OR logic with a two columns") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
      "WHERE " +
      "( B_FIELD > '4' or A_FIELD <= 'foo2') or" +
      "( A_FIELD > 'foo2' or B_FIELD < '4')").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 5)

    assert(executionRules.dynamicLogicExpression.toExpressionString.
      equals("( ( B_FIELD > 0 OR A_FIELD <= 1 ) OR " +
      "( A_FIELD > 2 OR B_FIELD < 3 ) )"))

    assert(executionRules.rowKeyFilter.points.size == 0)
    assert(executionRules.rowKeyFilter.ranges.size == 1)

    val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
    assert(Bytes.equals(scanRange1.lowerBound,Bytes.toBytes("")))
    assert(scanRange1.upperBound == null)
    assert(scanRange1.isLowerBoundEqualTo)
    assert(scanRange1.isUpperBoundEqualTo)

  }

  test("Test single RowKey Or Column logic") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
      "WHERE " +
      "( KEY_FIELD >= 'get4' or A_FIELD <= 'foo2' )").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 4)

    assert(executionRules.dynamicLogicExpression.toExpressionString.
      equals("( KEY_FIELD >= 0 OR A_FIELD <= 1 )"))

    assert(executionRules.rowKeyFilter.points.size == 0)
    assert(executionRules.rowKeyFilter.ranges.size == 1)

    val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
    assert(Bytes.equals(scanRange1.lowerBound,Bytes.toBytes("")))
    assert(scanRange1.upperBound == null)
    assert(scanRange1.isLowerBoundEqualTo)
    assert(scanRange1.isUpperBoundEqualTo)
  }

  test("Test table that doesn't exist") {
    val catalog = s"""{
            |"table":{"namespace":"default", "name":"t1NotThere"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"B_FIELD":{"cf":"c", "col":"c", "type":"string"}
            |}
          |}""".stripMargin

    intercept[Exception] {
      df = sqlContext.load("org.apache.hadoop.hbase.spark",
        Map(HBaseTableCatalog.tableCatalog->catalog))

      df.registerTempTable("hbaseNonExistingTmp")

      sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseNonExistingTmp " +
        "WHERE " +
        "( KEY_FIELD >= 'get1' and KEY_FIELD <= 'get3') or" +
        "( KEY_FIELD > 'get3' and KEY_FIELD <= 'get5')").count()
    }
    DefaultSourceStaticUtils.lastFiveExecutionRules.poll()
  }


  test("Test table with column that doesn't exist") {
    val catalog = s"""{
            |"table":{"namespace":"default", "name":"t1"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"B_FIELD":{"cf":"c", "col":"b", "type":"string"},
              |"C_FIELD":{"cf":"c", "col":"c", "type":"string"}
            |}
          |}""".stripMargin
    df = sqlContext.load("org.apache.hadoop.hbase.spark",
      Map(HBaseTableCatalog.tableCatalog->catalog))

    df.registerTempTable("hbaseFactColumnTmp")

    val result = sqlContext.sql("SELECT KEY_FIELD, " +
      "B_FIELD, A_FIELD FROM hbaseFactColumnTmp")

    assert(result.count() == 5)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()
    assert(executionRules.dynamicLogicExpression == null)

  }

  test("Test table with INT column") {
    val catalog = s"""{
            |"table":{"namespace":"default", "name":"t1"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"B_FIELD":{"cf":"c", "col":"b", "type":"string"},
              |"I_FIELD":{"cf":"c", "col":"i", "type":"int"}
            |}
          |}""".stripMargin
    df = sqlContext.load("org.apache.hadoop.hbase.spark",
      Map(HBaseTableCatalog.tableCatalog->catalog))

    df.registerTempTable("hbaseIntTmp")

    val result = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, I_FIELD FROM hbaseIntTmp"+
    " where I_FIELD > 4 and I_FIELD < 10")

    val localResult = result.take(5)

    assert(localResult.length == 2)
    assert(localResult(0).getInt(2) == 8)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()
    val expr = executionRules.dynamicLogicExpression.toExpressionString
    logInfo(expr)
    assert(expr.equals("( ( I_FIELD isNotNull AND I_FIELD > 0 ) AND I_FIELD < 1 )"), expr)

  }

  test("Test table with INT column defined at wrong type") {
    val catalog = s"""{
            |"table":{"namespace":"default", "name":"t1"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"B_FIELD":{"cf":"c", "col":"b", "type":"string"},
              |"I_FIELD":{"cf":"c", "col":"i", "type":"string"}
            |}
          |}""".stripMargin
    df = sqlContext.load("org.apache.hadoop.hbase.spark",
      Map(HBaseTableCatalog.tableCatalog->catalog))

    df.registerTempTable("hbaseIntWrongTypeTmp")

    val result = sqlContext.sql("SELECT KEY_FIELD, " +
      "B_FIELD, I_FIELD FROM hbaseIntWrongTypeTmp")

    val localResult = result.take(10)
    assert(localResult.length == 5)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()
    assert(executionRules.dynamicLogicExpression == null)

    assert(localResult(0).getString(2).length == 4)
    assert(localResult(0).getString(2).charAt(0).toByte == 0)
    assert(localResult(0).getString(2).charAt(1).toByte == 0)
    assert(localResult(0).getString(2).charAt(2).toByte == 0)
    assert(localResult(0).getString(2).charAt(3).toByte == 1)
  }

  test("Test bad column type") {
    val catalog = s"""{
            |"table":{"namespace":"default", "name":"t1"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"FOOBAR"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"I_FIELD":{"cf":"c", "col":"i", "type":"string"}
            |}
          |}""".stripMargin
    intercept[Exception] {
      df = sqlContext.load("org.apache.hadoop.hbase.spark",
        Map(HBaseTableCatalog.tableCatalog->catalog))

      df.registerTempTable("hbaseIntWrongTypeTmp")

      val result = sqlContext.sql("SELECT KEY_FIELD, " +
        "B_FIELD, I_FIELD FROM hbaseIntWrongTypeTmp")

      val localResult = result.take(10)
      assert(localResult.length == 5)

      val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()
      assert(executionRules.dynamicLogicExpression == null)

    }
  }

  test("Test HBaseSparkConf matching") {
    val df = sqlContext.load("org.apache.hadoop.hbase.spark.HBaseTestSource",
      Map("cacheSize" -> "100",
        "batchNum" -> "100",
        "blockCacheingEnable" -> "true", "rowNum" -> "10"))
    assert(df.count() == 10)

    val df1 = sqlContext.load("org.apache.hadoop.hbase.spark.HBaseTestSource",
      Map("cacheSize" -> "1000",
        "batchNum" -> "100", "blockCacheingEnable" -> "true", "rowNum" -> "10"))
    intercept[Exception] {
      assert(df1.count() == 10)
    }

    val df2 = sqlContext.load("org.apache.hadoop.hbase.spark.HBaseTestSource",
      Map("cacheSize" -> "100",
        "batchNum" -> "1000", "blockCacheingEnable" -> "true", "rowNum" -> "10"))
    intercept[Exception] {
      assert(df2.count() == 10)
    }

    val df3 = sqlContext.load("org.apache.hadoop.hbase.spark.HBaseTestSource",
      Map("cacheSize" -> "100",
        "batchNum" -> "100", "blockCacheingEnable" -> "false", "rowNum" -> "10"))
    intercept[Exception] {
      assert(df3.count() == 10)
    }
  }

  test("Test table with sparse column") {
    val catalog = s"""{
            |"table":{"namespace":"default", "name":"t1"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"B_FIELD":{"cf":"c", "col":"b", "type":"string"},
              |"Z_FIELD":{"cf":"c", "col":"z", "type":"string"}
            |}
          |}""".stripMargin
    df = sqlContext.load("org.apache.hadoop.hbase.spark",
      Map(HBaseTableCatalog.tableCatalog->catalog))

    df.registerTempTable("hbaseZTmp")

    val result = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, Z_FIELD FROM hbaseZTmp")

    val localResult = result.take(10)
    assert(localResult.length == 5)

    assert(localResult(0).getString(2) == null)
    assert(localResult(1).getString(2) == "FOO")
    assert(localResult(2).getString(2) == null)
    assert(localResult(3).getString(2) == "BAR")
    assert(localResult(4).getString(2) == null)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()
    assert(executionRules.dynamicLogicExpression == null)
  }

  test("Test with column logic disabled") {
    val catalog = s"""{
            |"table":{"namespace":"default", "name":"t1"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"B_FIELD":{"cf":"c", "col":"b", "type":"string"},
              |"Z_FIELD":{"cf":"c", "col":"z", "type":"string"}
            |}
          |}""".stripMargin
    df = sqlContext.load("org.apache.hadoop.hbase.spark",
      Map(HBaseTableCatalog.tableCatalog->catalog,
        HBaseSparkConf.PUSHDOWN_COLUMN_FILTER -> "false"))

    df.registerTempTable("hbaseNoPushDownTmp")

    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseNoPushDownTmp " +
      "WHERE " +
      "(KEY_FIELD <= 'get3' and KEY_FIELD >= 'get2')").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 2)

    assert(executionRules.dynamicLogicExpression == null)
  }

  def writeCatalog = s"""{
                    |"table":{"namespace":"default", "name":"table1"},
                    |"rowkey":"key",
                    |"columns":{
                    |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                    |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
                    |"col2":{"cf":"cf1", "col":"col2", "type":"double"},
                    |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
                    |"col4":{"cf":"cf3", "col":"col4", "type":"int"},
                    |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
                    |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
                    |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
                    |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"}
                    |}
                    |}""".stripMargin

  def withCatalog(cat: String): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.hadoop.hbase.spark")
      .load()
  }

  test("populate table") {
    val sql = sqlContext
    import sql.implicits._
    val data = (0 to 255).map { i =>
      HBaseRecord(i, "extra")
    }
    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> writeCatalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.hadoop.hbase.spark")
      .save()
  }

  test("empty column") {
    val df = withCatalog(writeCatalog)
    df.registerTempTable("table0")
    val c = sqlContext.sql("select count(1) from table0").rdd.collect()(0)(0).asInstanceOf[Long]
    assert(c == 256)
  }

  test("full query") {
    val df = withCatalog(writeCatalog)
    df.show()
    assert(df.count() == 256)
  }

  test("filtered query0") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(writeCatalog)
    val s = df.filter($"col0" <= "row005")
      .select("col0", "col1")
    s.show()
    assert(s.count() == 6)
  }

  test("Timestamp semantics") {
    val sql = sqlContext
    import sql.implicits._

    // There's already some data in here from recently. Let's throw something in
    // from 1993 which we can include/exclude and add some data with the implicit (now) timestamp.
    // Then we should be able to cross-section it and only get points in between, get the most recent view
    // and get an old view.
    val oldMs = 754869600000L
    val startMs = System.currentTimeMillis()
    val oldData = (0 to 100).map { i =>
      HBaseRecord(i, "old")
    }
    val newData = (200 to 255).map { i =>
      HBaseRecord(i, "new")
    }

    sc.parallelize(oldData).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> writeCatalog, HBaseTableCatalog.tableName -> "5",
        HBaseSparkConf.TIMESTAMP -> oldMs.toString))
      .format("org.apache.hadoop.hbase.spark")
      .save()
    sc.parallelize(newData).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> writeCatalog, HBaseTableCatalog.tableName -> "5"))
      .format("org.apache.hadoop.hbase.spark")
      .save()

    // Test specific timestamp -- Full scan, Timestamp
    val individualTimestamp = sqlContext.read
      .options(Map(HBaseTableCatalog.tableCatalog -> writeCatalog, HBaseSparkConf.TIMESTAMP -> oldMs.toString))
      .format("org.apache.hadoop.hbase.spark")
      .load()
    assert(individualTimestamp.count() == 101)

    // Test getting everything -- Full Scan, No range
    val everything = sqlContext.read
      .options(Map(HBaseTableCatalog.tableCatalog -> writeCatalog))
      .format("org.apache.hadoop.hbase.spark")
      .load()
    assert(everything.count() == 256)
    // Test getting everything -- Pruned Scan, TimeRange
    val element50 = everything.where(col("col0") === lit("row050")).select("col7").collect()(0)(0)
    assert(element50 == "String50: extra")
    val element200 = everything.where(col("col0") === lit("row200")).select("col7").collect()(0)(0)
    assert(element200 == "String200: new")

    // Test Getting old stuff -- Full Scan, TimeRange
    val oldRange = sqlContext.read
      .options(Map(HBaseTableCatalog.tableCatalog -> writeCatalog, HBaseSparkConf.TIMERANGE_START -> "0",
        HBaseSparkConf.TIMERANGE_END -> (oldMs + 100).toString))
      .format("org.apache.hadoop.hbase.spark")
      .load()
    assert(oldRange.count() == 101)
    // Test Getting old stuff -- Pruned Scan, TimeRange
    val oldElement50 = oldRange.where(col("col0") === lit("row050")).select("col7").collect()(0)(0)
    assert(oldElement50 == "String50: old")

    // Test Getting middle stuff -- Full Scan, TimeRange
    val middleRange = sqlContext.read
      .options(Map(HBaseTableCatalog.tableCatalog -> writeCatalog, HBaseSparkConf.TIMERANGE_START -> "0",
        HBaseSparkConf.TIMERANGE_END -> (startMs + 100).toString))
      .format("org.apache.hadoop.hbase.spark")
      .load()
    assert(middleRange.count() == 256)
    // Test Getting middle stuff -- Pruned Scan, TimeRange
    val middleElement200 = middleRange.where(col("col0") === lit("row200")).select("col7").collect()(0)(0)
    assert(middleElement200 == "String200: extra")
  }


  // catalog for insertion
  def avroWriteCatalog = s"""{
                             |"table":{"namespace":"default", "name":"avrotable"},
                             |"rowkey":"key",
                             |"columns":{
                             |"col0":{"cf":"rowkey", "col":"key", "type":"binary"},
                             |"col1":{"cf":"cf1", "col":"col1", "type":"binary"}
                             |}
                             |}""".stripMargin

  // catalog for read
  def avroCatalog = s"""{
                        |"table":{"namespace":"default", "name":"avrotable"},
                        |"rowkey":"key",
                        |"columns":{
                        |"col0":{"cf":"rowkey", "col":"key",  "avro":"avroSchema"},
                        |"col1":{"cf":"cf1", "col":"col1", "avro":"avroSchema"}
                        |}
                        |}""".stripMargin

  // for insert to another table
  def avroCatalogInsert = s"""{
                              |"table":{"namespace":"default", "name":"avrotableInsert"},
                              |"rowkey":"key",
                              |"columns":{
                              |"col0":{"cf":"rowkey", "col":"key", "avro":"avroSchema"},
                              |"col1":{"cf":"cf1", "col":"col1", "avro":"avroSchema"}
                              |}
                              |}""".stripMargin

  def withAvroCatalog(cat: String): DataFrame = {
    sqlContext
      .read
      .options(Map("avroSchema"->AvroHBaseKeyRecord.schemaString,
        HBaseTableCatalog.tableCatalog->avroCatalog))
      .format("org.apache.hadoop.hbase.spark")
      .load()
  }


  test("populate avro table") {
    val sql = sqlContext
    import sql.implicits._

    val data = (0 to 255).map { i =>
      AvroHBaseKeyRecord(i)
    }
    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> avroWriteCatalog,
        HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.hadoop.hbase.spark")
      .save()
  }

  test("avro empty column") {
    val df = withAvroCatalog(avroCatalog)
    df.registerTempTable("avrotable")
    val c = sqlContext.sql("select count(1) from avrotable")
      .rdd.collect()(0)(0).asInstanceOf[Long]
    assert(c == 256)
  }

  test("avro full query") {
    val df = withAvroCatalog(avroCatalog)
    df.show()
    df.printSchema()
    assert(df.count() == 256)
  }

  test("avro serialization and deserialization query") {
    val df = withAvroCatalog(avroCatalog)
    df.write.options(
      Map("avroSchema"->AvroHBaseKeyRecord.schemaString,
        HBaseTableCatalog.tableCatalog->avroCatalogInsert,
        HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.hadoop.hbase.spark")
      .save()
    val newDF = withAvroCatalog(avroCatalogInsert)
    newDF.show()
    newDF.printSchema()
    assert(newDF.count() == 256)
  }

  test("avro filtered query") {
    val sql = sqlContext
    import sql.implicits._
    val df = withAvroCatalog(avroCatalog)
    val r = df.filter($"col1.name" === "name005" || $"col1.name" <= "name005")
      .select("col0", "col1.favorite_color", "col1.favorite_number")
    r.show()
    assert(r.count() == 6)
  }

  test("avro Or filter") {
    val sql = sqlContext
    import sql.implicits._
    val df = withAvroCatalog(avroCatalog)
    val s = df.filter($"col1.name" <= "name005" || $"col1.name".contains("name007"))
      .select("col0", "col1.favorite_color", "col1.favorite_number")
    s.show()
    assert(s.count() == 7)
  }

  test("test create HBaseRelation with new context throws SAXParseException") {
    val catalog = s"""{
                     |"table":{"namespace":"default", "name":"t1NotThere"},
                     |"rowkey":"key",
                     |"columns":{
                     |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
                     |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
                     |"B_FIELD":{"cf":"c", "col":"c", "type":"string"}
                     |}
                     |}""".stripMargin
    try {
      HBaseRelation(Map(HBaseTableCatalog.tableCatalog -> catalog,
        HBaseSparkConf.USE_HBASECONTEXT -> "false"), None)(sqlContext)
    } catch {
        case e: Throwable => if(e.getCause.isInstanceOf[SAXParseException]) {
          fail("SAXParseException due to configuration loading empty resource")
        } else {
          println("Failed due to some other exception, ignore " + e.getMessage)
        }
    }
  }
}
