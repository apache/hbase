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

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Get, ConnectionFactory}
import org.apache.hadoop.hbase.io.hfile.{CacheConfig, HFile}
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.{HConstants, CellUtil, HBaseTestingUtility, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.spark.{SparkContext, Logging}
import org.junit.rules.TemporaryFolder
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class BulkLoadSuite extends FunSuite with
BeforeAndAfterEach with BeforeAndAfterAll  with Logging {
  @transient var sc: SparkContext = null
  var TEST_UTIL = new HBaseTestingUtility

  val tableName = "t1"
  val columnFamily1 = "f1"
  val columnFamily2 = "f2"
  val testFolder = new TemporaryFolder()


  override def beforeAll() {
    TEST_UTIL.startMiniCluster()
    logInfo(" - minicluster started")

    try {
      TEST_UTIL.deleteTable(TableName.valueOf(tableName))
    } catch {
      case e: Exception =>
        logInfo(" - no table " + tableName + " found")
    }

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

  test("Basic Test multi family and multi column tests " +
    "with all default HFile Configs") {
    val config = TEST_UTIL.getConfiguration

    logInfo(" - creating table " + tableName)
    TEST_UTIL.createTable(TableName.valueOf(tableName),
      Array(Bytes.toBytes(columnFamily1), Bytes.toBytes(columnFamily2)))

    //There are a number of tests in here.
    // 1. Row keys are not in order
    // 2. Qualifiers are not in order
    // 3. Column Families are not in order
    // 4. There are tests for records in one column family and some in two column families
    // 5. There are records will a single qualifier and some with two
    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("1"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo1")))),
      (Bytes.toBytes("3"),
        Array((Bytes.toBytes(columnFamily2), Bytes.toBytes("b"), Bytes.toBytes("foo2.a")))),
      (Bytes.toBytes("3"),
        Array((Bytes.toBytes(columnFamily2), Bytes.toBytes("a"), Bytes.toBytes("foo2.b")))),
      (Bytes.toBytes("3"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo2.c")))),
      (Bytes.toBytes("5"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo3")))),
      (Bytes.toBytes("4"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo.1")))),
      (Bytes.toBytes("4"),
        Array((Bytes.toBytes(columnFamily2), Bytes.toBytes("b"), Bytes.toBytes("foo.2")))),
      (Bytes.toBytes("2"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("bar.1")))),
      (Bytes.toBytes("2"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("b"), Bytes.toBytes("bar.2"))))))

    val hbaseContext = new HBaseContext(sc, config)

    testFolder.create()
    val stagingFolder = testFolder.newFolder()

    hbaseContext.bulkLoad[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,
      TableName.valueOf(tableName),
      t => {
        val rowKey = t._1
        val family:Array[Byte] = t._2(0)._1
        val qualifier = t._2(0)._2
        val value = t._2(0)._3

        val keyFamilyQualifier= new KeyFamilyQualifier(rowKey, family, qualifier)

        Seq((keyFamilyQualifier, value)).iterator
      },
      stagingFolder.getPath)

    val fs = FileSystem.get(config)
    assert(fs.listStatus(new Path(stagingFolder.getPath)).length == 2)

    val conn = ConnectionFactory.createConnection(config)

    val load = new LoadIncrementalHFiles(config)
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      load.doBulkLoad(new Path(stagingFolder.getPath), conn.getAdmin, table,
        conn.getRegionLocator(TableName.valueOf(tableName)))

      val cells5 = table.get(new Get(Bytes.toBytes("5"))).listCells()
      assert(cells5.size == 1)
      assert(Bytes.toString(CellUtil.cloneValue(cells5.get(0))).equals("foo3"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells5.get(0))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells5.get(0))).equals("a"))

      val cells4 = table.get(new Get(Bytes.toBytes("4"))).listCells()
      assert(cells4.size == 2)
      assert(Bytes.toString(CellUtil.cloneValue(cells4.get(0))).equals("foo.1"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells4.get(0))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells4.get(0))).equals("a"))
      assert(Bytes.toString(CellUtil.cloneValue(cells4.get(1))).equals("foo.2"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells4.get(1))).equals("f2"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells4.get(1))).equals("b"))

      val cells3 = table.get(new Get(Bytes.toBytes("3"))).listCells()
      assert(cells3.size == 3)
      assert(Bytes.toString(CellUtil.cloneValue(cells3.get(0))).equals("foo2.c"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells3.get(0))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells3.get(0))).equals("a"))
      assert(Bytes.toString(CellUtil.cloneValue(cells3.get(1))).equals("foo2.b"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells3.get(1))).equals("f2"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells3.get(1))).equals("a"))
      assert(Bytes.toString(CellUtil.cloneValue(cells3.get(2))).equals("foo2.a"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells3.get(2))).equals("f2"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells3.get(2))).equals("b"))


      val cells2 = table.get(new Get(Bytes.toBytes("2"))).listCells()
      assert(cells2.size == 2)
      assert(Bytes.toString(CellUtil.cloneValue(cells2.get(0))).equals("bar.1"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells2.get(0))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells2.get(0))).equals("a"))
      assert(Bytes.toString(CellUtil.cloneValue(cells2.get(1))).equals("bar.2"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells2.get(1))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells2.get(1))).equals("b"))

      val cells1 = table.get(new Get(Bytes.toBytes("1"))).listCells()
      assert(cells1.size == 1)
      assert(Bytes.toString(CellUtil.cloneValue(cells1.get(0))).equals("foo1"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells1.get(0))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells1.get(0))).equals("a"))

    } finally {
      table.close()
      val admin = ConnectionFactory.createConnection(config).getAdmin
      try {
        admin.disableTable(TableName.valueOf(tableName))
        admin.deleteTable(TableName.valueOf(tableName))
      } finally {
        admin.close()
      }
      fs.delete(new Path(stagingFolder.getPath), true)

      testFolder.delete()

    }
  }

  test("bulkLoad to test HBase client: Test Roll Over and " +
    "using an implicit call to bulk load") {
    val config = TEST_UTIL.getConfiguration

    logInfo(" - creating table " + tableName)
    TEST_UTIL.createTable(TableName.valueOf(tableName),
      Array(Bytes.toBytes(columnFamily1), Bytes.toBytes(columnFamily2)))

    //There are a number of tests in here.
    // 1. Row keys are not in order
    // 2. Qualifiers are not in order
    // 3. Column Families are not in order
    // 4. There are tests for records in one column family and some in two column families
    // 5. There are records will a single qualifier and some with two
    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("1"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo1")))),
      (Bytes.toBytes("3"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("b"), Bytes.toBytes("foo2.b")))),
      (Bytes.toBytes("3"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo2.a")))),
      (Bytes.toBytes("3"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("c"), Bytes.toBytes("foo2.c")))),
      (Bytes.toBytes("5"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo3")))),
      (Bytes.toBytes("4"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo.1")))),
      (Bytes.toBytes("4"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("b"), Bytes.toBytes("foo.2")))),
      (Bytes.toBytes("2"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("bar.1")))),
      (Bytes.toBytes("2"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("b"), Bytes.toBytes("bar.2"))))))

    val hbaseContext = new HBaseContext(sc, config)

    testFolder.create()
    val stagingFolder = testFolder.newFolder()

    rdd.hbaseBulkLoad(hbaseContext,
      TableName.valueOf(tableName),
      t => {
        val rowKey = t._1
        val family:Array[Byte] = t._2(0)._1
        val qualifier = t._2(0)._2
        val value = t._2(0)._3

        val keyFamilyQualifier= new KeyFamilyQualifier(rowKey, family, qualifier)

        Seq((keyFamilyQualifier, value)).iterator
      },
      stagingFolder.getPath,
      new java.util.HashMap[Array[Byte], FamilyHFileWriteOptions],
      compactionExclude = false,
      20)

    val fs = FileSystem.get(config)
    assert(fs.listStatus(new Path(stagingFolder.getPath)).length == 1)

    assert(fs.listStatus(new Path(stagingFolder.getPath+ "/f1")).length == 5)

    val conn = ConnectionFactory.createConnection(config)

    val load = new LoadIncrementalHFiles(config)
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      load.doBulkLoad(new Path(stagingFolder.getPath),
        conn.getAdmin, table, conn.getRegionLocator(TableName.valueOf(tableName)))

      val cells5 = table.get(new Get(Bytes.toBytes("5"))).listCells()
      assert(cells5.size == 1)
      assert(Bytes.toString(CellUtil.cloneValue(cells5.get(0))).equals("foo3"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells5.get(0))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells5.get(0))).equals("a"))

      val cells4 = table.get(new Get(Bytes.toBytes("4"))).listCells()
      assert(cells4.size == 2)
      assert(Bytes.toString(CellUtil.cloneValue(cells4.get(0))).equals("foo.1"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells4.get(0))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells4.get(0))).equals("a"))
      assert(Bytes.toString(CellUtil.cloneValue(cells4.get(1))).equals("foo.2"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells4.get(1))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells4.get(1))).equals("b"))

      val cells3 = table.get(new Get(Bytes.toBytes("3"))).listCells()
      assert(cells3.size == 3)
      assert(Bytes.toString(CellUtil.cloneValue(cells3.get(0))).equals("foo2.a"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells3.get(0))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells3.get(0))).equals("a"))
      assert(Bytes.toString(CellUtil.cloneValue(cells3.get(1))).equals("foo2.b"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells3.get(1))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells3.get(1))).equals("b"))
      assert(Bytes.toString(CellUtil.cloneValue(cells3.get(2))).equals("foo2.c"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells3.get(2))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells3.get(2))).equals("c"))

      val cells2 = table.get(new Get(Bytes.toBytes("2"))).listCells()
      assert(cells2.size == 2)
      assert(Bytes.toString(CellUtil.cloneValue(cells2.get(0))).equals("bar.1"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells2.get(0))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells2.get(0))).equals("a"))
      assert(Bytes.toString(CellUtil.cloneValue(cells2.get(1))).equals("bar.2"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells2.get(1))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells2.get(1))).equals("b"))

      val cells1 = table.get(new Get(Bytes.toBytes("1"))).listCells()
      assert(cells1.size == 1)
      assert(Bytes.toString(CellUtil.cloneValue(cells1.get(0))).equals("foo1"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells1.get(0))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells1.get(0))).equals("a"))

    } finally {
      table.close()
      val admin = ConnectionFactory.createConnection(config).getAdmin
      try {
        admin.disableTable(TableName.valueOf(tableName))
        admin.deleteTable(TableName.valueOf(tableName))
      } finally {
        admin.close()
      }
      fs.delete(new Path(stagingFolder.getPath), true)

      testFolder.delete()
    }
  }

  test("Basic Test multi family and multi column tests" +
    " with one column family with custom configs plus multi region") {
    val config = TEST_UTIL.getConfiguration

    val splitKeys:Array[Array[Byte]] = new Array[Array[Byte]](2)
    splitKeys(0) = Bytes.toBytes("2")
    splitKeys(1) = Bytes.toBytes("4")

    logInfo(" - creating table " + tableName)
    TEST_UTIL.createTable(TableName.valueOf(tableName),
      Array(Bytes.toBytes(columnFamily1), Bytes.toBytes(columnFamily2)),
      splitKeys)

    //There are a number of tests in here.
    // 1. Row keys are not in order
    // 2. Qualifiers are not in order
    // 3. Column Families are not in order
    // 4. There are tests for records in one column family and some in two column families
    // 5. There are records will a single qualifier and some with two
    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("1"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo1")))),
      (Bytes.toBytes("3"),
        Array((Bytes.toBytes(columnFamily2), Bytes.toBytes("b"), Bytes.toBytes("foo2.a")))),
      (Bytes.toBytes("3"),
        Array((Bytes.toBytes(columnFamily2), Bytes.toBytes("a"), Bytes.toBytes("foo2.b")))),
      (Bytes.toBytes("3"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo2.c")))),
      (Bytes.toBytes("5"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo3")))),
      (Bytes.toBytes("4"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo.1")))),
      (Bytes.toBytes("4"),
        Array((Bytes.toBytes(columnFamily2), Bytes.toBytes("b"), Bytes.toBytes("foo.2")))),
      (Bytes.toBytes("2"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("bar.1")))),
      (Bytes.toBytes("2"),
        Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("b"), Bytes.toBytes("bar.2"))))))

    val hbaseContext = new HBaseContext(sc, config)

    testFolder.create()
    val stagingFolder = testFolder.newFolder()

    val familyHBaseWriterOptions = new java.util.HashMap[Array[Byte], FamilyHFileWriteOptions]

    val f1Options = new FamilyHFileWriteOptions("GZ", "ROW", 128,
      "PREFIX")

    familyHBaseWriterOptions.put(Bytes.toBytes(columnFamily1), f1Options)

    hbaseContext.bulkLoad[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,
      TableName.valueOf(tableName),
      t => {
        val rowKey = t._1
        val family:Array[Byte] = t._2(0)._1
        val qualifier = t._2(0)._2
        val value = t._2(0)._3

        val keyFamilyQualifier= new KeyFamilyQualifier(rowKey, family, qualifier)

        Seq((keyFamilyQualifier, value)).iterator
      },
      stagingFolder.getPath,
      familyHBaseWriterOptions,
      compactionExclude = false,
      HConstants.DEFAULT_MAX_FILE_SIZE)

    val fs = FileSystem.get(config)
    assert(fs.listStatus(new Path(stagingFolder.getPath)).length == 2)

    val f1FileList = fs.listStatus(new Path(stagingFolder.getPath +"/f1"))
    for ( i <- 0 until f1FileList.length) {
      val reader = HFile.createReader(fs, f1FileList(i).getPath,
        new CacheConfig(config), config)
      assert(reader.getCompressionAlgorithm.getName.equals("gz"))
      assert(reader.getDataBlockEncoding.name().equals("PREFIX"))
    }

    assert( 3 ==  f1FileList.length)

    val f2FileList = fs.listStatus(new Path(stagingFolder.getPath +"/f2"))
    for ( i <- 0 until f2FileList.length) {
      val reader = HFile.createReader(fs, f2FileList(i).getPath,
        new CacheConfig(config), config)
      assert(reader.getCompressionAlgorithm.getName.equals("none"))
      assert(reader.getDataBlockEncoding.name().equals("NONE"))
    }

    assert( 2 ==  f2FileList.length)


    val conn = ConnectionFactory.createConnection(config)

    val load = new LoadIncrementalHFiles(config)
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      load.doBulkLoad(new Path(stagingFolder.getPath),
        conn.getAdmin, table, conn.getRegionLocator(TableName.valueOf(tableName)))

      val cells5 = table.get(new Get(Bytes.toBytes("5"))).listCells()
      assert(cells5.size == 1)
      assert(Bytes.toString(CellUtil.cloneValue(cells5.get(0))).equals("foo3"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells5.get(0))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells5.get(0))).equals("a"))

      val cells4 = table.get(new Get(Bytes.toBytes("4"))).listCells()
      assert(cells4.size == 2)
      assert(Bytes.toString(CellUtil.cloneValue(cells4.get(0))).equals("foo.1"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells4.get(0))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells4.get(0))).equals("a"))
      assert(Bytes.toString(CellUtil.cloneValue(cells4.get(1))).equals("foo.2"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells4.get(1))).equals("f2"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells4.get(1))).equals("b"))

      val cells3 = table.get(new Get(Bytes.toBytes("3"))).listCells()
      assert(cells3.size == 3)
      assert(Bytes.toString(CellUtil.cloneValue(cells3.get(0))).equals("foo2.c"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells3.get(0))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells3.get(0))).equals("a"))
      assert(Bytes.toString(CellUtil.cloneValue(cells3.get(1))).equals("foo2.b"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells3.get(1))).equals("f2"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells3.get(1))).equals("a"))
      assert(Bytes.toString(CellUtil.cloneValue(cells3.get(2))).equals("foo2.a"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells3.get(2))).equals("f2"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells3.get(2))).equals("b"))


      val cells2 = table.get(new Get(Bytes.toBytes("2"))).listCells()
      assert(cells2.size == 2)
      assert(Bytes.toString(CellUtil.cloneValue(cells2.get(0))).equals("bar.1"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells2.get(0))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells2.get(0))).equals("a"))
      assert(Bytes.toString(CellUtil.cloneValue(cells2.get(1))).equals("bar.2"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells2.get(1))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells2.get(1))).equals("b"))

      val cells1 = table.get(new Get(Bytes.toBytes("1"))).listCells()
      assert(cells1.size == 1)
      assert(Bytes.toString(CellUtil.cloneValue(cells1.get(0))).equals("foo1"))
      assert(Bytes.toString(CellUtil.cloneFamily(cells1.get(0))).equals("f1"))
      assert(Bytes.toString(CellUtil.cloneQualifier(cells1.get(0))).equals("a"))

    } finally {
      table.close()
      val admin = ConnectionFactory.createConnection(config).getAdmin
      try {
        admin.disableTable(TableName.valueOf(tableName))
        admin.deleteTable(TableName.valueOf(tableName))
      } finally {
        admin.close()
      }
      fs.delete(new Path(stagingFolder.getPath), true)

      testFolder.delete()

    }
  }

  test("bulkLoad partitioner tests") {

    var splitKeys:Array[Array[Byte]] = new Array[Array[Byte]](3)
    splitKeys(0) = Bytes.toBytes("")
    splitKeys(1) = Bytes.toBytes("3")
    splitKeys(2) = Bytes.toBytes("7")

    var partitioner = new BulkLoadPartitioner(splitKeys)

    assert(0 == partitioner.getPartition(Bytes.toBytes("")))
    assert(0 == partitioner.getPartition(Bytes.toBytes("1")))
    assert(0 == partitioner.getPartition(Bytes.toBytes("2")))
    assert(1 == partitioner.getPartition(Bytes.toBytes("3")))
    assert(1 == partitioner.getPartition(Bytes.toBytes("4")))
    assert(1 == partitioner.getPartition(Bytes.toBytes("6")))
    assert(2 == partitioner.getPartition(Bytes.toBytes("7")))
    assert(2 == partitioner.getPartition(Bytes.toBytes("8")))


    splitKeys = new Array[Array[Byte]](1)
    splitKeys(0) = Bytes.toBytes("")

    partitioner = new BulkLoadPartitioner(splitKeys)

    assert(0 == partitioner.getPartition(Bytes.toBytes("")))
    assert(0 == partitioner.getPartition(Bytes.toBytes("1")))
    assert(0 == partitioner.getPartition(Bytes.toBytes("2")))
    assert(0 == partitioner.getPartition(Bytes.toBytes("3")))
    assert(0 == partitioner.getPartition(Bytes.toBytes("4")))
    assert(0 == partitioner.getPartition(Bytes.toBytes("6")))
    assert(0 == partitioner.getPartition(Bytes.toBytes("7")))

    splitKeys = new Array[Array[Byte]](7)
    splitKeys(0) = Bytes.toBytes("")
    splitKeys(1) = Bytes.toBytes("02")
    splitKeys(2) = Bytes.toBytes("04")
    splitKeys(3) = Bytes.toBytes("06")
    splitKeys(4) = Bytes.toBytes("08")
    splitKeys(5) = Bytes.toBytes("10")
    splitKeys(6) = Bytes.toBytes("12")

    partitioner = new BulkLoadPartitioner(splitKeys)

    assert(0 == partitioner.getPartition(Bytes.toBytes("")))
    assert(0 == partitioner.getPartition(Bytes.toBytes("01")))
    assert(1 == partitioner.getPartition(Bytes.toBytes("02")))
    assert(1 == partitioner.getPartition(Bytes.toBytes("03")))
    assert(2 == partitioner.getPartition(Bytes.toBytes("04")))
    assert(2 == partitioner.getPartition(Bytes.toBytes("05")))
    assert(3 == partitioner.getPartition(Bytes.toBytes("06")))
    assert(3 == partitioner.getPartition(Bytes.toBytes("07")))
    assert(4 == partitioner.getPartition(Bytes.toBytes("08")))
    assert(4 == partitioner.getPartition(Bytes.toBytes("09")))
    assert(5 == partitioner.getPartition(Bytes.toBytes("10")))
    assert(5 == partitioner.getPartition(Bytes.toBytes("11")))
    assert(6 == partitioner.getPartition(Bytes.toBytes("12")))
    assert(6 == partitioner.getPartition(Bytes.toBytes("13")))

  }


}
