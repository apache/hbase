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

import org.apache.hadoop.hbase.client.{Put, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableNotFoundException, TableName, HBaseTestingUtility}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, Logging}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class DefaultSourceSuite extends FunSuite with
BeforeAndAfterEach with BeforeAndAfterAll with Logging {
  @transient var sc: SparkContext = null
  var TEST_UTIL: HBaseTestingUtility = new HBaseTestingUtility

  val tableName = "t1"
  val columnFamily = "c"

  var sqlContext:SQLContext = null
  var df:DataFrame = null

  override def beforeAll() {

    TEST_UTIL.startMiniCluster

    logInfo(" - minicluster started")
    try
      TEST_UTIL.deleteTable(TableName.valueOf(tableName))
    catch {
      case e: Exception => logInfo(" - no table " + tableName + " found")

    }
    logInfo(" - creating table " + tableName)
    TEST_UTIL.createTable(TableName.valueOf(tableName), Bytes.toBytes(columnFamily))
    logInfo(" - created table")

    sc = new SparkContext("local", "test")

    val connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration)
    val table = connection.getTable(TableName.valueOf("t1"))

    try {
      var put = new Put(Bytes.toBytes("get1"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo1"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("1"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(1))
      table.put(put)
      put = new Put(Bytes.toBytes("get2"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo2"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("4"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(4))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("z"), Bytes.toBytes("FOO"))
      table.put(put)
      put = new Put(Bytes.toBytes("get3"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo3"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("8"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(8))
      table.put(put)
      put = new Put(Bytes.toBytes("get4"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo4"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("10"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(10))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("z"), Bytes.toBytes("BAR"))
      table.put(put)
      put = new Put(Bytes.toBytes("get5"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo5"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("8"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("i"), Bytes.toBytes(8))
      table.put(put)
    } finally {
      table.close()
      connection.close()
    }

    new HBaseContext(sc, TEST_UTIL.getConfiguration)
    sqlContext = new SQLContext(sc)

    df = sqlContext.load("org.apache.hadoop.hbase.spark",
      Map("hbase.columns.mapping" ->
        "KEY_FIELD STRING :key, A_FIELD STRING c:a, B_FIELD STRING c:b,",
        "hbase.table" -> "t1",
        "hbase.batching.num" -> "100",
        "cachingNum" -> "100"))

    df.registerTempTable("hbaseTmp")
  }

  override def afterAll() {
    TEST_UTIL.deleteTable(TableName.valueOf(tableName))
    logInfo("shuting down minicluster")
    TEST_UTIL.shutdownMiniCluster()

    sc.stop()
  }


  /**
   * A example of query three fields and also only using rowkey points for the filter
   */
  test("Test rowKey point only rowKey query") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTmp " +
      "WHERE " +
      "(KEY_FIELD = 'get1' or KEY_FIELD = 'get2' or KEY_FIELD = 'get3')").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 3)

    assert(executionRules.columnFilterCollection.columnFilterMap.size == 1)
    val keyFieldFilter =
      executionRules.columnFilterCollection.columnFilterMap.get("KEY_FIELD").get
    assert(keyFieldFilter.ranges.length == 0)
    assert(keyFieldFilter.points.length == 3)
    assert(Bytes.toString(keyFieldFilter.points.head).equals("get1"))
    assert(Bytes.toString(keyFieldFilter.points(1)).equals("get2"))
    assert(Bytes.toString(keyFieldFilter.points(2)).equals("get3"))

    assert(executionRules.requiredQualifierDefinitionArray.length == 2)
  }

  /**
   * A example of query three fields and also only using cell points for the filter
   */
  test("Test cell point only rowKey query") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTmp " +
      "WHERE " +
      "(B_FIELD = '4' or B_FIELD = '10' or A_FIELD = 'foo1')").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 3)

    assert(executionRules.columnFilterCollection.columnFilterMap.size == 2)
    val bFieldFilter =
      executionRules.columnFilterCollection.columnFilterMap.get("B_FIELD").get
    assert(bFieldFilter.ranges.length == 0)
    assert(bFieldFilter.points.length == 2)
    val aFieldFilter =
      executionRules.columnFilterCollection.columnFilterMap.get("A_FIELD").get
    assert(aFieldFilter.ranges.length == 0)
    assert(aFieldFilter.points.length == 1)

    assert(executionRules.requiredQualifierDefinitionArray.length == 2)
  }

  /**
   * A example of a OR merge between to ranges the result is one range
   * Also an example of less then and greater then
   */
  test("Test two range rowKey query") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTmp " +
      "WHERE " +
      "( KEY_FIELD < 'get2' or KEY_FIELD > 'get3')").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 3)

    assert(executionRules.columnFilterCollection.columnFilterMap.size == 1)
    val keyFieldFilter =
      executionRules.columnFilterCollection.columnFilterMap.get("KEY_FIELD").get
    assert(keyFieldFilter.ranges.length == 2)
    assert(keyFieldFilter.points.length == 0)

    assert(executionRules.requiredQualifierDefinitionArray.length == 2)
  }

  /**
   * A example of a AND merge between to ranges the result is one range
   * Also an example of less then and equal to and greater then and equal to
   */
  test("Test one combined range rowKey query") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTmp " +
      "WHERE " +
      "(KEY_FIELD <= 'get3' and KEY_FIELD >= 'get2')").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 2)

    assert(executionRules.columnFilterCollection.columnFilterMap.size == 1)
    val keyFieldFilter =
      executionRules.columnFilterCollection.columnFilterMap.get("KEY_FIELD").get
    assert(keyFieldFilter.ranges.length == 1)
    assert(keyFieldFilter.points.length == 0)

    assert(executionRules.requiredQualifierDefinitionArray.length == 2)
  }

  /**
   * Do a select with no filters
   */
  test("Test select only query") {

    val results = df.select("KEY_FIELD").take(10)
    assert(results.length == 5)


    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()
    assert(executionRules.columnFilterCollection == null)
    assert(executionRules.requiredQualifierDefinitionArray.length == 0)

  }

  /**
   * A complex query with one point and one range for both the
   * rowKey and the a column
   */
  test("Test SQL point and range combo") {
    val results = sqlContext.sql("SELECT KEY_FIELD FROM hbaseTmp " +
      "WHERE " +
      "(KEY_FIELD = 'get1' and B_FIELD < '3') or " +
      "(KEY_FIELD >= 'get3' and B_FIELD = '8')").take(5)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 3)

    assert(executionRules.columnFilterCollection.columnFilterMap.size == 2)
    val keyFieldFilter =
      executionRules.columnFilterCollection.columnFilterMap.get("KEY_FIELD").get
    assert(keyFieldFilter.ranges.length == 1)
    assert(keyFieldFilter.ranges.head.upperBound == null)
    assert(Bytes.toString(keyFieldFilter.ranges.head.lowerBound).equals("get3"))
    assert(keyFieldFilter.ranges.head.isLowerBoundEqualTo)
    assert(keyFieldFilter.points.length == 1)
    assert(Bytes.toString(keyFieldFilter.points.head).equals("get1"))

    val bFieldFilter =
      executionRules.columnFilterCollection.columnFilterMap.get("B_FIELD").get
    assert(bFieldFilter.ranges.length == 1)
    assert(bFieldFilter.ranges.head.lowerBound.length == 0)
    assert(Bytes.toString(bFieldFilter.ranges.head.upperBound).equals("3"))
    assert(!bFieldFilter.ranges.head.isUpperBoundEqualTo)
    assert(bFieldFilter.points.length == 1)
    assert(Bytes.toString(bFieldFilter.points.head).equals("8"))

    assert(executionRules.requiredQualifierDefinitionArray.length == 1)
    assert(executionRules.requiredQualifierDefinitionArray.head.columnName.equals("B_FIELD"))
    assert(executionRules.requiredQualifierDefinitionArray.head.columnFamily.equals("c"))
    assert(executionRules.requiredQualifierDefinitionArray.head.qualifier.equals("b"))
  }

  /**
   * A complex query with two complex ranges that doesn't merge into one
   */
  test("Test two complete range non merge rowKey query") {

    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTmp " +
      "WHERE " +
      "( KEY_FIELD >= 'get1' and KEY_FIELD <= 'get2') or" +
      "( KEY_FIELD > 'get3' and KEY_FIELD <= 'get5')").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()
    assert(results.length == 4)

    assert(executionRules.columnFilterCollection.columnFilterMap.size == 1)
    val keyFieldFilter =
      executionRules.columnFilterCollection.columnFilterMap.get("KEY_FIELD").get
    assert(keyFieldFilter.ranges.length == 2)
    assert(keyFieldFilter.points.length == 0)

    assert(executionRules.requiredQualifierDefinitionArray.length == 2)
  }

  /**
   * A complex query with two complex ranges that does merge into one
   */
  test("Test two complete range merge rowKey query") {
    val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTmp " +
      "WHERE " +
      "( KEY_FIELD >= 'get1' and KEY_FIELD <= 'get3') or" +
      "( KEY_FIELD > 'get3' and KEY_FIELD <= 'get5')").take(10)

    val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

    assert(results.length == 5)

    assert(executionRules.columnFilterCollection.columnFilterMap.size == 1)
    val keyFieldFilter =
      executionRules.columnFilterCollection.columnFilterMap.get("KEY_FIELD").get
    assert(keyFieldFilter.ranges.length == 1)
    assert(keyFieldFilter.points.length == 0)

    assert(executionRules.requiredQualifierDefinitionArray.length == 2)
  }

  test("test table that doesn't exist") {
    intercept[TableNotFoundException] {
      df = sqlContext.load("org.apache.hadoop.hbase.spark",
        Map("hbase.columns.mapping" ->
          "KEY_FIELD STRING :key, A_FIELD STRING c:a, B_FIELD STRING c:b,",
          "hbase.table" -> "t1NotThere"))

      df.registerTempTable("hbaseNonExistingTmp")

      sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseNonExistingTmp " +
        "WHERE " +
        "( KEY_FIELD >= 'get1' and KEY_FIELD <= 'get3') or" +
        "( KEY_FIELD > 'get3' and KEY_FIELD <= 'get5')").count()
    }
  }

  test("Test table with column that doesn't exist") {
    df = sqlContext.load("org.apache.hadoop.hbase.spark",
      Map("hbase.columns.mapping" ->
        "KEY_FIELD STRING :key, A_FIELD STRING c:a, B_FIELD STRING c:b, C_FIELD STRING c:c,",
        "hbase.table" -> "t1"))

    df.registerTempTable("hbaseFactColumnTmp")

    val result = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseFactColumnTmp")

    assert(result.count() == 5)

    val localResult = result.take(5)
  }

  test("Test table with INT column") {
    df = sqlContext.load("org.apache.hadoop.hbase.spark",
      Map("hbase.columns.mapping" ->
        "KEY_FIELD STRING :key, A_FIELD STRING c:a, B_FIELD STRING c:b, I_FIELD INT c:i,",
        "hbase.table" -> "t1"))

    df.registerTempTable("hbaseIntTmp")

    val result = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, I_FIELD FROM hbaseIntTmp"+
    " where I_FIELD > 4 and I_FIELD < 10")

    assert(result.count() == 2)

    val localResult = result.take(3)

    assert(localResult(0).getInt(2) == 8)
  }

  test("Test table with INT column defined at wrong type") {
    df = sqlContext.load("org.apache.hadoop.hbase.spark",
      Map("hbase.columns.mapping" ->
        "KEY_FIELD STRING :key, A_FIELD STRING c:a, B_FIELD STRING c:b, I_FIELD STRING c:i,",
        "hbase.table" -> "t1"))

    df.registerTempTable("hbaseIntWrongTypeTmp")

    val result = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, I_FIELD FROM hbaseIntWrongTypeTmp")

    assert(result.count() == 5)

    val localResult = result.take(5)

    assert(localResult(0).getString(2).length == 4)
    assert(localResult(0).getString(2).charAt(0).toByte == 0)
    assert(localResult(0).getString(2).charAt(1).toByte == 0)
    assert(localResult(0).getString(2).charAt(2).toByte == 0)
    assert(localResult(0).getString(2).charAt(3).toByte == 1)
  }

  test("Test improperly formatted column mapping") {
    intercept[IllegalArgumentException] {
      df = sqlContext.load("org.apache.hadoop.hbase.spark",
        Map("hbase.columns.mapping" ->
          "KEY_FIELD,STRING,:key, A_FIELD,STRING,c:a, B_FIELD,STRING,c:b, I_FIELD,STRING,c:i,",
          "hbase.table" -> "t1"))

      df.registerTempTable("hbaseBadTmp")

      val result = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, I_FIELD FROM hbaseBadTmp")

      val localResult = result.take(5)
    }
  }


  test("Test bad column type") {
    intercept[IllegalArgumentException] {
      df = sqlContext.load("org.apache.hadoop.hbase.spark",
        Map("hbase.columns.mapping" ->
          "KEY_FIELD FOOBAR :key, A_FIELD STRING c:a, B_FIELD STRING c:b, I_FIELD STRING c:i,",
          "hbase.table" -> "t1"))

      df.registerTempTable("hbaseIntWrongTypeTmp")

      val result = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, I_FIELD FROM hbaseIntWrongTypeTmp")

      assert(result.count() == 5)

      val localResult = result.take(5)
    }
  }

  test("Test bad hbase.batching.num type") {
    intercept[IllegalArgumentException] {
      df = sqlContext.load("org.apache.hadoop.hbase.spark",
        Map("hbase.columns.mapping" ->
          "KEY_FIELD FOOBAR :key, A_FIELD STRING c:a, B_FIELD STRING c:b, I_FIELD STRING c:i,",
          "hbase.table" -> "t1", "hbase.batching.num" -> "foo"))

      df.registerTempTable("hbaseIntWrongTypeTmp")

      val result = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, I_FIELD FROM hbaseIntWrongTypeTmp")

      assert(result.count() == 5)

      val localResult = result.take(5)
    }
  }

  test("Test bad hbase.caching.num type") {
    intercept[IllegalArgumentException] {
      df = sqlContext.load("org.apache.hadoop.hbase.spark",
        Map("hbase.columns.mapping" ->
          "KEY_FIELD FOOBAR :key, A_FIELD STRING c:a, B_FIELD STRING c:b, I_FIELD STRING c:i,",
          "hbase.table" -> "t1", "hbase.caching.num" -> "foo"))

      df.registerTempTable("hbaseIntWrongTypeTmp")

      val result = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, I_FIELD FROM hbaseIntWrongTypeTmp")

      assert(result.count() == 5)

      val localResult = result.take(5)
    }
  }

  test("Test table with sparse column") {
    df = sqlContext.load("org.apache.hadoop.hbase.spark",
      Map("hbase.columns.mapping" ->
        "KEY_FIELD STRING :key, A_FIELD STRING c:a, B_FIELD STRING c:b, Z_FIELD STRING c:z,",
        "hbase.table" -> "t1"))

    df.registerTempTable("hbaseZTmp")

    val result = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, Z_FIELD FROM hbaseZTmp")

    assert(result.count() == 5)

    val localResult = result.take(5)

    assert(localResult(0).getString(2) == null)
    assert(localResult(1).getString(2) == "FOO")
    assert(localResult(2).getString(2) == null)
    assert(localResult(3).getString(2) == "BAR")
    assert(localResult(4).getString(2) == null)

  }
}
