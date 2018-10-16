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


import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.{HBaseTestingUtility, TableName, TableNotFoundException}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.{Job, TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import scala.util.{Failure, Success, Try}


// Unit tests for HBASE-20521: change get configuration(TableOutputFormat.conf) object first sequence from jobContext to getConf
// this suite contains two tests, one for normal case(getConf return null, use jobContext), create new TableOutputformat object without init TableOutputFormat.conf object,
// configuration object inside checkOutputSpecs came from jobContext.
// The other one(getConf return conf object) we manually call "setConf" to init TableOutputFormat.conf, for making it more straight forward, we specify a nonexistent table
// name in conf object, checkOutputSpecs will then throw TableNotFoundException exception
class TableOutputFormatSuite extends FunSuite with
  BeforeAndAfterEach with BeforeAndAfterAll with Logging{
  @transient var sc: SparkContext = null
  var TEST_UTIL = new HBaseTestingUtility

  val tableName = "TableOutputFormatTest"
  val tableNameTest = "NonExistentTable"
  val columnFamily = "cf"

  override protected def beforeAll(): Unit = {
    TEST_UTIL.startMiniCluster

    logInfo(" - minicluster started")
    try {
      TEST_UTIL.deleteTable(TableName.valueOf(tableName))
    }
    catch {
      case e: Exception => logInfo(" - no table " + tableName + " found")
    }

    TEST_UTIL.createTable(TableName.valueOf(tableName), Bytes.toBytes(columnFamily))
    logInfo(" - created table")

    //set "validateOutputSpecs" true anyway, force to validate output spec
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("test")

    sc = new SparkContext(sparkConf)
  }

  override protected def afterAll(): Unit = {
    logInfo(" - delete table: " + tableName)
    TEST_UTIL.deleteTable(TableName.valueOf(tableName))
    logInfo(" - shutting down minicluster")
    TEST_UTIL.shutdownMiniCluster()

    TEST_UTIL.cleanupTestDir()
    sc.stop()
  }

  def getJobContext() = {
    val hConf = TEST_UTIL.getConfiguration
    hConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = Job.getInstance(hConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])

    val jobTrackerId = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(new Date())
    val jobAttemptId = new TaskAttemptID(jobTrackerId, 1, TaskType.MAP, 0, 0)
    new TaskAttemptContextImpl(job.getConfiguration, jobAttemptId)
  }

  // Mock up jobContext object and execute actions in "write" function
  // from "org.apache.spark.internal.io.SparkHadoopMapReduceWriter"
  // this case should run normally without any exceptions
  test("TableOutputFormat.checkOutputSpecs test without setConf called, should return true and without exceptions") {
    val jobContext = getJobContext()
    val format = jobContext.getOutputFormatClass
    val jobFormat = format.newInstance
    Try {
      jobFormat.checkOutputSpecs(jobContext)
    } match {
      case Success(_) => assert(true)
      case Failure(_) => assert(false)
    }
  }

  // Set configuration externally, checkOutputSpec should use configuration object set by "SetConf" method
  // rather than jobContext, this case should throw "TableNotFoundException" exception
  test("TableOutputFormat.checkOutputSpecs test without setConf called, should throw TableNotFoundException") {
    val jobContext = getJobContext()
    val format = jobContext.getOutputFormatClass
    val jobFormat = format.newInstance

    val hConf = TEST_UTIL.getConfiguration
    hConf.set(TableOutputFormat.OUTPUT_TABLE, tableNameTest)
    jobFormat.asInstanceOf[TableOutputFormat[String]].setConf(hConf)
    Try {
      jobFormat.checkOutputSpecs(jobContext)
    } match {
      case Success(_) => assert(false)
      case Failure(e: Exception) => {
        if(e.isInstanceOf[TableNotFoundException])
          assert(true)
        else
          assert(false)
      }
     case _ => None
    }
  }

}
