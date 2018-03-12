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

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, TableName, HBaseTestingUtility}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.spark.HBaseDStreamFunctions._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import scala.collection.mutable

class HBaseDStreamFunctionsSuite  extends FunSuite with
BeforeAndAfterEach with BeforeAndAfterAll with Logging {
  @transient var sc: SparkContext = null

  var TEST_UTIL: HBaseTestingUtility = new HBaseTestingUtility

  val tableName = "t1"
  val columnFamily = "c"

  override def beforeAll() {

    TEST_UTIL.startMiniCluster()

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
  }

  override def afterAll() {
    TEST_UTIL.deleteTable(TableName.valueOf(tableName))
    TEST_UTIL.shutdownMiniCluster()
    sc.stop()
  }

  test("bulkput to test HBase client") {
    val config = TEST_UTIL.getConfiguration
    val rdd1 = sc.parallelize(Array(
      (Bytes.toBytes("1"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo1")))),
      (Bytes.toBytes("2"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("foo2")))),
      (Bytes.toBytes("3"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("c"), Bytes.toBytes("foo3"))))))

    val rdd2 = sc.parallelize(Array(
      (Bytes.toBytes("4"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("d"), Bytes.toBytes("foo")))),
      (Bytes.toBytes("5"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("e"), Bytes.toBytes("bar"))))))

    var isFinished = false

    val hbaseContext = new HBaseContext(sc, config)
    val ssc = new StreamingContext(sc, Milliseconds(200))

    val queue = mutable.Queue[RDD[(Array[Byte], Array[(Array[Byte],
      Array[Byte], Array[Byte])])]]()
    queue += rdd1
    queue += rdd2
    val dStream = ssc.queueStream(queue)

    dStream.hbaseBulkPut(
      hbaseContext,
      TableName.valueOf(tableName),
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, putValue._3))
        put
      })

    dStream.foreachRDD(rdd => {
      if (rdd.count() == 0) {
        isFinished = true
      }
    })

    ssc.start()

    while (!isFinished) {
      Thread.sleep(100)
    }

    ssc.stop(true, true)

    val connection = ConnectionFactory.createConnection(config)
    val table = connection.getTable(TableName.valueOf("t1"))

    try {
      val foo1 = Bytes.toString(CellUtil.cloneValue(table.get(new Get(Bytes.toBytes("1"))).
        getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("a"))))
      assert(foo1 == "foo1")

      val foo2 = Bytes.toString(CellUtil.cloneValue(table.get(new Get(Bytes.toBytes("2"))).
        getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("b"))))
      assert(foo2 == "foo2")

      val foo3 = Bytes.toString(CellUtil.cloneValue(table.get(new Get(Bytes.toBytes("3"))).
        getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("c"))))
      assert(foo3 == "foo3")

      val foo4 = Bytes.toString(CellUtil.cloneValue(table.get(new Get(Bytes.toBytes("4"))).
        getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("d"))))
      assert(foo4 == "foo")

      val foo5 = Bytes.toString(CellUtil.cloneValue(table.get(new Get(Bytes.toBytes("5"))).
        getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("e"))))
      assert(foo5 == "bar")
    } finally {
      table.close()
      connection.close()
    }
  }

}
