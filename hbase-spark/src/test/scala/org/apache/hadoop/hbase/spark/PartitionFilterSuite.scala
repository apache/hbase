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

import org.apache.hadoop.hbase.spark.datasources.HBaseSparkConf
import org.apache.hadoop.hbase.{TableName, HBaseTestingUtility}
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

case class FilterRangeRecord(
                              intCol0: Int,
                              boolCol1: Boolean,
                              doubleCol2: Double,
                              floatCol3: Float,
                              intCol4: Int,
                              longCol5: Long,
                              shortCol6: Short,
                              stringCol7: String,
                              byteCol8: Byte)

object FilterRangeRecord {
  def apply(i: Int): FilterRangeRecord = {
    FilterRangeRecord(if (i % 2 == 0) i else -i,
      i % 2 == 0,
      if (i % 2 == 0) i.toDouble else -i.toDouble,
      i.toFloat,
      if (i % 2 == 0) i else -i,
      i.toLong,
      i.toShort,
      s"String$i extra",
      i.toByte)
  }
}

class PartitionFilterSuite extends FunSuite with
  BeforeAndAfterEach with BeforeAndAfterAll with Logging {
  @transient var sc: SparkContext = null
  var TEST_UTIL: HBaseTestingUtility = new HBaseTestingUtility

  var sqlContext: SQLContext = null
  var df: DataFrame = null

  def withCatalog(cat: String): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog -> cat))
      .format("org.apache.hadoop.hbase.spark")
      .load()
  }

  override def beforeAll() {

    TEST_UTIL.startMiniCluster
    val sparkConf = new SparkConf
    sparkConf.set(HBaseSparkConf.BLOCK_CACHE_ENABLE, "true")
    sparkConf.set(HBaseSparkConf.BATCH_NUM, "100")
    sparkConf.set(HBaseSparkConf.CACHE_SIZE, "100")

    sc = new SparkContext("local", "test", sparkConf)
    new HBaseContext(sc, TEST_UTIL.getConfiguration)
    sqlContext = new SQLContext(sc)
  }

  override def afterAll() {
    logInfo("shutting down minicluster")
    TEST_UTIL.shutdownMiniCluster()

    sc.stop()
  }

  override def beforeEach(): Unit = {
    DefaultSourceStaticUtils.lastFiveExecutionRules.clear()
  }

  // The original raw data used for construct result set without going through
  // data frame logic. It is used to verify the result set retrieved from data frame logic.
  val rawResult = (0 until 32).map { i =>
    FilterRangeRecord(i)
  }

  def collectToSet[T](df: DataFrame): Set[T] = {
    df.collect().map(_.getAs[T](0)).toSet
  }
  val catalog = s"""{
                    |"table":{"namespace":"default", "name":"rangeTable"},
                    |"rowkey":"key",
                    |"columns":{
                    |"intCol0":{"cf":"rowkey", "col":"key", "type":"int"},
                    |"boolCol1":{"cf":"cf1", "col":"boolCol1", "type":"boolean"},
                    |"doubleCol2":{"cf":"cf2", "col":"doubleCol2", "type":"double"},
                    |"floatCol3":{"cf":"cf3", "col":"floatCol3", "type":"float"},
                    |"intCol4":{"cf":"cf4", "col":"intCol4", "type":"int"},
                    |"longCol5":{"cf":"cf5", "col":"longCol5", "type":"bigint"},
                    |"shortCol6":{"cf":"cf6", "col":"shortCol6", "type":"smallint"},
                    |"stringCol7":{"cf":"cf7", "col":"stringCol7", "type":"string"},
                    |"byteCol8":{"cf":"cf8", "col":"byteCol8", "type":"tinyint"}
                    |}
                    |}""".stripMargin

  test("populate rangeTable") {
    val sql = sqlContext
    import sql.implicits._

    sc.parallelize(rawResult).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.hadoop.hbase.spark")
      .save()
  }
  test("rangeTable full query") {
    val df = withCatalog(catalog)
    df.show
    assert(df.count() === 32)
  }

  /**
    *expected result: only showing top 20 rows
    *+-------+
    *|intCol0|
    *+-------+
    *| -31   |
    *| -29   |
    *| -27   |
    *| -25   |
    *| -23   |
    *| -21   |
    *| -19   |
    *| -17   |
    *| -15   |
    *| -13   |
    *| -11   |
    *|  -9   |
    *|  -7   |
    *|  -5   |
    *|  -3   |
    *|  -1   |
    *+----   +
    */
  test("rangeTable rowkey less than 0") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"intCol0" < 0).select($"intCol0")
    s.show
    // filter results without going through dataframe
    val expected = rawResult.filter(_.intCol0 < 0).map(_.intCol0).toSet
    // filter results going through dataframe
    val result = collectToSet[Int](s)
    assert(expected === result)
  }

  /**
    *expected result: only showing top 20 rows
    *+-------+
    *|intCol4|
    *+-------+
    *| -31   |
    *| -29   |
    *| -27   |
    *| -25   |
    *| -23   |
    *| -21   |
    *| -19   |
    *| -17   |
    *| -15   |
    *| -13   |
    *| -11   |
    *|  -9   |
    *|  -7   |
    *|  -5   |
    *|  -3   |
    *|  -1   |
    *+-------+
    */
  test("rangeTable int col less than 0") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"intCol4" < 0).select($"intCol4")
    s.show
    // filter results without going through dataframe
    val expected = rawResult.filter(_.intCol4 < 0).map(_.intCol4).toSet
    // filter results going through dataframe
    val result = collectToSet[Int](s)
    assert(expected === result)
  }

  /**
    *expected result: only showing top 20 rows
    *+-----------+
    *| doubleCol2|
    *+-----------+
    *|  0.0      |
    *|  2.0      |
    *|-31.0      |
    *|-29.0      |
    *|-27.0      |
    *|-25.0      |
    *|-23.0      |
    *|-21.0      |
    *|-19.0      |
    *|-17.0      |
    *|-15.0      |
    *|-13.0      |
    *|-11.0      |
    *| -9.0      |
    *| -7.0      |
    *| -5.0      |
    *| -3.0      |
    *| -1.0      |
    *+-----------+
    */
  test("rangeTable double col less than 0") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"doubleCol2" < 3.0).select($"doubleCol2")
    s.show
    // filter results without going through dataframe
    val expected = rawResult.filter(_.doubleCol2 < 3.0).map(_.doubleCol2).toSet
    // filter results going through dataframe
    val result = collectToSet[Double](s)
    assert(expected === result)
  }

  /**
    * expected result: only showing top 20 rows
    *+-------+
    *|intCol0|
    *+-------+
    *| -31   |
    *| -29   |
    *| -27   |
    *| -25   |
    *| -23   |
    *| -21   |
    *| -19   |
    *| -17   |
    *| -15   |
    *| -13   |
    *| -11   |
    *+-------+
    *
    */
  test("rangeTable lessequal than -10") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"intCol0" <= -10).select($"intCol0")
    s.show
    // filter results without going through dataframe
    val expected = rawResult.filter(_.intCol0 <= -10).map(_.intCol0).toSet
    // filter results going through dataframe
    val result = collectToSet[Int](s)
    assert(expected === result)
  }

  /**
    *expected result: only showing top 20 rows
    *+-------+
    *|intCol0|
    *+----+
    *| -31   |
    *| -29   |
    *| -27   |
    *| -25   |
    *| -23   |
    *| -21   |
    *| -19   |
    *| -17   |
    *| -15   |
    *| -13   |
    *| -11   |
    *|  -9   |
    *+-------+
    */
  test("rangeTable lessequal than -9") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"intCol0" <= -9).select($"intCol0")
    s.show
    // filter results without going through dataframe
    val expected = rawResult.filter(_.intCol0 <= -9).map(_.intCol0).toSet
    // filter results going through dataframe
    val result = collectToSet[Int](s)
    assert(expected === result)
  }

  /**
    *expected result: only showing top 20 rows
    *+-------+
    *|intCol0|
    *+-------+
    *|   0   |
    *|   2   |
    *|   4   |
    *|   6   |
    *|   8   |
    *|  10   |
    *|  12   |
    *|  14   |
    *|  16   |
    *|  18   |
    *|  20   |
    *|  22   |
    *|  24   |
    *|  26   |
    *|  28   |
    *|  30   |
    *|  -9   |
    *|  -7   |
    *|  -5   |
    *|  -3   |
    *+-------+
    */
  test("rangeTable greaterequal than -9") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"intCol0" >= -9).select($"intCol0")
    s.show
    // filter results without going through dataframe
    val expected = rawResult.filter(_.intCol0 >= -9).map(_.intCol0).toSet
    // filter results going through dataframe
    val result = collectToSet[Int](s)
    assert(expected === result)
  }

  /**
    *expected result: only showing top 20 rows
    *+-------+
    *|intCol0|
    *+-------+
    *|   0   |
    *|   2   |
    *|   4   |
    *|   6   |
    *|   8   |
    *|  10   |
    *|  12   |
    *|  14   |
    *|  16   |
    *|  18   |
    *|  20   |
    *|  22   |
    *|  24   |
    *|  26   |
    *|  28   |
    *|  30   |
    *+-------+
    */
  test("rangeTable greaterequal  than 0") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"intCol0" >= 0).select($"intCol0")
    s.show
    // filter results without going through dataframe
    val expected = rawResult.filter(_.intCol0 >= 0).map(_.intCol0).toSet
    // filter results going through dataframe
    val result = collectToSet[Int](s)
    assert(expected === result)
  }

  /**
    *expected result: only showing top 20 rows
    *+-------+
    *|intCol0|
    *+-------+
    *|  12   |
    *|  14   |
    *|  16   |
    *|  18   |
    *|  20   |
    *|  22   |
    *|  24   |
    *|  26   |
    *|  28   |
    *|  30   |
    *+-------+
    */
  test("rangeTable greater than 10") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"intCol0" > 10).select($"intCol0")
    s.show
    // filter results without going through dataframe
    val expected = rawResult.filter(_.intCol0 > 10).map(_.intCol0).toSet
    // filter results going through dataframe
    val result = collectToSet[Int](s)
    assert(expected === result)
  }

  /**
    *expected result: only showing top 20 rows
    *+-------+
    *|intCol0|
    *+-------+
    *|   0   |
    *|   2   |
    *|   4   |
    *|   6   |
    *|   8   |
    *|  10   |
    *|  -9   |
    *|  -7   |
    *|  -5   |
    *|  -3   |
    *|  -1   |
    *+-------+
    */
  test("rangeTable and") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"intCol0" > -10 && $"intCol0" <= 10).select($"intCol0")
    s.show
    // filter results without going through dataframe
    val expected = rawResult.filter(x => x.intCol0 > -10 && x.intCol0 <= 10 ).map(_.intCol0).toSet
    // filter results going through dataframe
    val result = collectToSet[Int](s)
    assert(expected === result)
  }

  /**
    *expected result: only showing top 20 rows
    *+-------+
    *|intCol0|
    *+-------+
    *|  12   |
    *|  14   |
    *|  16   |
    *|  18   |
    *|  20   |
    *|  22   |
    *|  24   |
    *|  26   |
    *|  28   |
    *|  30   |
    *| -31   |
    *| -29   |
    *| -27   |
    *| -25   |
    *| -23   |
    *| -21   |
    *| -19   |
    *| -17   |
    *| -15   |
    *| -13   |
    *+-------+
    */

  test("or") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"intCol0" <= -10 || $"intCol0" > 10).select($"intCol0")
    s.show
    // filter results without going through dataframe
    val expected = rawResult.filter(x => x.intCol0 <= -10 || x.intCol0 > 10).map(_.intCol0).toSet
    // filter results going through dataframe
    val result = collectToSet[Int](s)
    assert(expected === result)
  }

  /**
    *expected result: only showing top 20 rows
    *+-------+
    *|intCol0|
    *+-------+
    *|   0   |
    *|   2   |
    *|   4   |
    *|   6   |
    *|   8   |
    *|  10   |
    *|  12   |
    *|  14   |
    *|  16   |
    *|  18   |
    *|  20   |
    *|  22   |
    *|  24   |
    *|  26   |
    *|  28   |
    *|  30   |
    *| -31   |
    *| -29   |
    *| -27   |
    *| -25   |
    *+-------+
    */
  test("rangeTable all") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(catalog)
    val s = df.filter($"intCol0" >= -100).select($"intCol0")
    s.show
    // filter results without going through dataframe
    val expected = rawResult.filter(_.intCol0 >= -100).map(_.intCol0).toSet
    // filter results going through dataframe
    val result = collectToSet[Int](s)
    assert(expected === result)
  }
}
