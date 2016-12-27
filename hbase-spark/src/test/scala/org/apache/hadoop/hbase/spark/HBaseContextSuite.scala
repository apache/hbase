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
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ CellUtil, TableName, HBaseTestingUtility}
import org.apache.spark.{SparkException, Logging, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class HBaseContextSuite extends FunSuite with
BeforeAndAfterEach with BeforeAndAfterAll  with Logging {

  @transient var sc: SparkContext = null
  var TEST_UTIL = new HBaseTestingUtility

  val tableName = "t1"
  val columnFamily = "c"

  override def beforeAll() {
    TEST_UTIL.startMiniCluster()
    logInfo(" - minicluster started")

    try {
      TEST_UTIL.deleteTable(TableName.valueOf(tableName))
    } catch {
      case e: Exception =>
        logInfo(" - no table " + tableName + " found")
    }
    logInfo(" - creating table " + tableName)
    TEST_UTIL.createTable(TableName.valueOf(tableName), Bytes.toBytes(columnFamily))
    logInfo(" - created table")

    val envMap = Map[String,String](("Xmx", "512m"))

    sc = new SparkContext("local", "test", null, Nil, envMap)
  }

  override def afterAll() {
    logInfo("shuting down minicluster")
    TEST_UTIL.shutdownMiniCluster()
    logInfo(" - minicluster shut down")
    TEST_UTIL.cleanupTestDir()
    sc.stop()
  }

  test("bulkput to test HBase client") {
    val config = TEST_UTIL.getConfiguration
    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("1"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo1")))),
      (Bytes.toBytes("2"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("foo2")))),
      (Bytes.toBytes("3"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("c"), Bytes.toBytes("foo3")))),
      (Bytes.toBytes("4"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("d"), Bytes.toBytes("foo")))),
      (Bytes.toBytes("5"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("e"), Bytes.toBytes("bar"))))))

    val hbaseContext = new HBaseContext(sc, config)
    hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,
      TableName.valueOf(tableName),
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, putValue._3))
        put
      })

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

  test("bulkDelete to test HBase client") {
    val config = TEST_UTIL.getConfiguration
    val connection = ConnectionFactory.createConnection(config)
    val table = connection.getTable(TableName.valueOf("t1"))

    try {
      var put = new Put(Bytes.toBytes("delete1"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo1"))
      table.put(put)
      put = new Put(Bytes.toBytes("delete2"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo2"))
      table.put(put)
      put = new Put(Bytes.toBytes("delete3"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo3"))
      table.put(put)

      val rdd = sc.parallelize(Array(
        Bytes.toBytes("delete1"),
        Bytes.toBytes("delete3")))

      val hbaseContext = new HBaseContext(sc, config)
      hbaseContext.bulkDelete[Array[Byte]](rdd,
        TableName.valueOf(tableName),
        putRecord => new Delete(putRecord),
        4)

      assert(table.get(new Get(Bytes.toBytes("delete1"))).
        getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("a")) == null)
      assert(table.get(new Get(Bytes.toBytes("delete3"))).
        getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("a")) == null)
      assert(Bytes.toString(CellUtil.cloneValue(table.get(new Get(Bytes.toBytes("delete2"))).
        getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("a")))).equals("foo2"))
    } finally {
      table.close()
      connection.close()
    }
  }

  test("bulkGet to test HBase client") {
    val config = TEST_UTIL.getConfiguration
    val connection = ConnectionFactory.createConnection(config)
    val table = connection.getTable(TableName.valueOf("t1"))

    try {
      var put = new Put(Bytes.toBytes("get1"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo1"))
      table.put(put)
      put = new Put(Bytes.toBytes("get2"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo2"))
      table.put(put)
      put = new Put(Bytes.toBytes("get3"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo3"))
      table.put(put)
    } finally {
      table.close()
      connection.close()
    }
    val rdd = sc.parallelize(Array(
      Bytes.toBytes("get1"),
      Bytes.toBytes("get2"),
      Bytes.toBytes("get3"),
      Bytes.toBytes("get4")))
    val hbaseContext = new HBaseContext(sc, config)

    val getRdd = hbaseContext.bulkGet[Array[Byte], String](
      TableName.valueOf(tableName),
      2,
      rdd,
      record => {
        new Get(record)
      },
      (result: Result) => {
        if (result.listCells() != null) {
          val it = result.listCells().iterator()
          val B = new StringBuilder

          B.append(Bytes.toString(result.getRow) + ":")

          while (it.hasNext) {
            val cell = it.next()
            val q = Bytes.toString(CellUtil.cloneQualifier(cell))
            if (q.equals("counter")) {
              B.append("(" + q + "," + Bytes.toLong(CellUtil.cloneValue(cell)) + ")")
            } else {
              B.append("(" + q + "," + Bytes.toString(CellUtil.cloneValue(cell)) + ")")
            }
          }
          "" + B.toString
        } else {
          ""
        }
      })
    val getArray = getRdd.collect()

    assert(getArray.length == 4)
    assert(getArray.contains("get1:(a,foo1)"))
    assert(getArray.contains("get2:(a,foo2)"))
    assert(getArray.contains("get3:(a,foo3)"))

  }

  test("BulkGet failure test: bad table") {
    val config = TEST_UTIL.getConfiguration

    val rdd = sc.parallelize(Array(
      Bytes.toBytes("get1"),
      Bytes.toBytes("get2"),
      Bytes.toBytes("get3"),
      Bytes.toBytes("get4")))
    val hbaseContext = new HBaseContext(sc, config)

    intercept[SparkException] {
      try {
        val getRdd = hbaseContext.bulkGet[Array[Byte], String](
          TableName.valueOf("badTableName"),
          2,
          rdd,
          record => {
            new Get(record)
          },
          (result: Result) => "1")

        getRdd.collect()

        fail("We should have failed and not reached this line")
      } catch {
        case ex: SparkException => {
          assert(
            ex.getMessage.contains(
              "org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException"))
          throw ex
        }
      }
    }
  }

  test("BulkGet failure test: bad column") {

    val config = TEST_UTIL.getConfiguration
    val connection = ConnectionFactory.createConnection(config)
    val table = connection.getTable(TableName.valueOf("t1"))

    try {
      var put = new Put(Bytes.toBytes("get1"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo1"))
      table.put(put)
      put = new Put(Bytes.toBytes("get2"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo2"))
      table.put(put)
      put = new Put(Bytes.toBytes("get3"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo3"))
      table.put(put)
    } finally {
      table.close()
      connection.close()
    }

    val rdd = sc.parallelize(Array(
      Bytes.toBytes("get1"),
      Bytes.toBytes("get2"),
      Bytes.toBytes("get3"),
      Bytes.toBytes("get4")))
    val hbaseContext = new HBaseContext(sc, config)

    val getRdd = hbaseContext.bulkGet[Array[Byte], String](
      TableName.valueOf(tableName),
      2,
      rdd,
      record => {
        new Get(record)
      },
      (result: Result) => {
        if (result.listCells() != null) {
          val cellValue = result.getColumnLatestCell(
            Bytes.toBytes("c"), Bytes.toBytes("bad_column"))
          if (cellValue == null) "null" else "bad"
        } else "noValue"
      })
    var nullCounter = 0
    var noValueCounter = 0
    getRdd.collect().foreach(r => {
      if ("null".equals(r)) nullCounter += 1
      else if ("noValue".equals(r)) noValueCounter += 1
    })
    assert(nullCounter == 3)
    assert(noValueCounter == 1)
  }

  test("distributedScan to test HBase client") {
    val config = TEST_UTIL.getConfiguration
    val connection = ConnectionFactory.createConnection(config)
    val table = connection.getTable(TableName.valueOf("t1"))

    try {
      var put = new Put(Bytes.toBytes("scan1"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo1"))
      table.put(put)
      put = new Put(Bytes.toBytes("scan2"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo2"))
      table.put(put)
      put = new Put(Bytes.toBytes("scan2"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("foo-2"))
      table.put(put)
      put = new Put(Bytes.toBytes("scan3"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo3"))
      table.put(put)
      put = new Put(Bytes.toBytes("scan4"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo3"))
      table.put(put)
      put = new Put(Bytes.toBytes("scan5"))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo3"))
      table.put(put)
    } finally {
      table.close()
      connection.close()
    }

    val hbaseContext = new HBaseContext(sc, config)

    val scan = new Scan()
    val filter = new FirstKeyOnlyFilter()
    scan.setCaching(100)
    scan.setStartRow(Bytes.toBytes("scan2"))
    scan.setStopRow(Bytes.toBytes("scan4_"))
    scan.setFilter(filter)

    val scanRdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan)

    try {
      val scanList = scanRdd.map(r => r._1.copyBytes()).collect()
      assert(scanList.length == 3)
      var cnt = 0
      scanRdd.map(r => r._2.listCells().size()).collect().foreach(l => {
        cnt += l
      })
      // the number of cells returned would be 4 without the Filter
      assert(cnt == 3);
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }
}
