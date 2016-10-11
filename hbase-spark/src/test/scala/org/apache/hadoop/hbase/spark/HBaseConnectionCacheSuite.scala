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

import java.util.concurrent.ExecutorService
import scala.util.Random

import org.apache.hadoop.hbase.client.{BufferedMutator, Table, RegionLocator,
  Connection, BufferedMutatorParams, Admin}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.spark.Logging
import org.scalatest.FunSuite

case class HBaseConnectionKeyMocker (confId: Int) extends HBaseConnectionKey (null) {
  override def hashCode: Int = {
    confId
  }

  override def equals(obj: Any): Boolean = {
    if(!obj.isInstanceOf[HBaseConnectionKeyMocker])
      false
    else
      confId == obj.asInstanceOf[HBaseConnectionKeyMocker].confId
  }
}

class ConnectionMocker extends Connection {
  var isClosed: Boolean = false

  def getRegionLocator (tableName: TableName): RegionLocator = null
  def getConfiguration: Configuration = null
  def getTable (tableName: TableName): Table = null
  def getTable(tableName: TableName, pool: ExecutorService): Table = null
  def getBufferedMutator (params: BufferedMutatorParams): BufferedMutator = null
  def getBufferedMutator (tableName: TableName): BufferedMutator = null
  def getAdmin: Admin = null

  def close(): Unit = {
    if (isClosed)
      throw new IllegalStateException()
    isClosed = true
  }

  def isAborted: Boolean = true
  def abort(why: String, e: Throwable) = {}
}

class HBaseConnectionCacheSuite extends FunSuite with Logging {
  /*
   * These tests must be performed sequentially as they operate with an
   * unique running thread and resource.
   *
   * It looks there's no way to tell FunSuite to do so, so making those
   * test cases normal functions which are called sequentially in a single
   * test case.
   */
  test("all test cases") {
    testBasic()
    testWithPressureWithoutClose()
    testWithPressureWithClose()
  }

  def testBasic() {
    HBaseConnectionCache.setTimeout(1 * 1000)

    val connKeyMocker1 = new HBaseConnectionKeyMocker(1)
    val connKeyMocker1a = new HBaseConnectionKeyMocker(1)
    val connKeyMocker2 = new HBaseConnectionKeyMocker(2)

    val c1 = HBaseConnectionCache
      .getConnection(connKeyMocker1, new ConnectionMocker)
    val c1a = HBaseConnectionCache
      .getConnection(connKeyMocker1a, new ConnectionMocker)

    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 1)
    }

    val c2 = HBaseConnectionCache
      .getConnection(connKeyMocker2, new ConnectionMocker)

    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 2)
    }

    c1.close()
    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 2)
    }

    c1a.close()
    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 2)
    }

    Thread.sleep(3 * 1000) // Leave housekeeping thread enough time
    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 1)
      assert(HBaseConnectionCache.connectionMap.iterator.next()._1
        .asInstanceOf[HBaseConnectionKeyMocker].confId === 2)
    }

    c2.close()
  }

  def testWithPressureWithoutClose() {
    class TestThread extends Runnable {
      override def run() {
        for (i <- 0 to 999) {
          val c = HBaseConnectionCache.getConnection(
            new HBaseConnectionKeyMocker(Random.nextInt(10)), new ConnectionMocker)
        }
      }
    }

    HBaseConnectionCache.setTimeout(500)
    val threads: Array[Thread] = new Array[Thread](100)
    for (i <- 0 to 99) {
      threads.update(i, new Thread(new TestThread()))
      threads(i).run()
    }
    try {
      threads.foreach { x => x.join() }
    } catch {
      case e: InterruptedException => println(e.getMessage)
    }

    Thread.sleep(1000)
    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 10)
      var totalRc : Int = 0
      HBaseConnectionCache.connectionMap.foreach {
        x => totalRc += x._2.refCount
      }
      assert(totalRc === 100 * 1000)
      HBaseConnectionCache.connectionMap.foreach {
        x => {
          x._2.refCount = 0
          x._2.timestamp = System.currentTimeMillis() - 1000
        }
      }
    }
    Thread.sleep(1000)
    assert(HBaseConnectionCache.connectionMap.size === 0)
  }

  def testWithPressureWithClose() {
    class TestThread extends Runnable {
      override def run() {
        for (i <- 0 to 999) {
          val c = HBaseConnectionCache.getConnection(
            new HBaseConnectionKeyMocker(Random.nextInt(10)), new ConnectionMocker)
          Thread.`yield`()
          c.close()
        }
      }
    }

    HBaseConnectionCache.setTimeout(3 * 1000)
    val threads: Array[Thread] = new Array[Thread](100)
    for (i <- threads.indices) {
      threads.update(i, new Thread(new TestThread()))
      threads(i).run()
    }
    try {
      threads.foreach { x => x.join() }
    } catch {
      case e: InterruptedException => println(e.getMessage)
    }

    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 10)
    }

    Thread.sleep(6 * 1000)
    HBaseConnectionCache.connectionMap.synchronized {
      assert(HBaseConnectionCache.connectionMap.size === 0)
    }
  }
}
