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

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, RegionLocator, Table}
import org.apache.hadoop.hbase.ipc.RpcControllerFactory
import org.apache.hadoop.hbase.security.{User, UserProvider}
import org.apache.hadoop.hbase.spark.datasources.HBaseSparkConf
import org.apache.hadoop.hbase.{HConstants, TableName}
import org.apache.spark.Logging

import scala.collection.mutable

private[spark] object HBaseConnectionCache extends Logging {

  // A hashmap of Spark-HBase connections. Key is HBaseConnectionKey.
  val connectionMap = new mutable.HashMap[HBaseConnectionKey, SmartConnection]()

  // in milliseconds
  private final val DEFAULT_TIME_OUT: Long = HBaseSparkConf.connectionCloseDelay
  private var timeout = DEFAULT_TIME_OUT
  private var closed: Boolean = false

  var housekeepingThread = new Thread(new Runnable {
    override def run() {
      while (true) {
        try {
          Thread.sleep(timeout)
        } catch {
          case e: InterruptedException =>
            // setTimeout() and close() may interrupt the sleep and it's safe
            // to ignore the exception
        }
        if (closed)
          return
        performHousekeeping(false)
      }
    }
  })
  housekeepingThread.setDaemon(true)
  housekeepingThread.start()

  def close(): Unit = {
    try {
      connectionMap.synchronized {
        if (closed)
          return
        closed = true
        housekeepingThread.interrupt()
        housekeepingThread = null
        HBaseConnectionCache.performHousekeeping(true)
      }
    } catch {
      case e: Exception => logWarning("Error in finalHouseKeeping", e)
    }
  }

  def performHousekeeping(forceClean: Boolean) = {
    val tsNow: Long = System.currentTimeMillis()
    connectionMap.synchronized {
      connectionMap.foreach {
        x => {
          if(x._2.refCount < 0) {
            logError(s"Bug to be fixed: negative refCount of connection ${x._2}")
          }

          if(forceClean || ((x._2.refCount <= 0) && (tsNow - x._2.timestamp > timeout))) {
            try{
              x._2.connection.close()
            } catch {
              case e: IOException => logWarning(s"Fail to close connection ${x._2}", e)
            }
            connectionMap.remove(x._1)
          }
        }
      }
    }
  }

  // For testing purpose only
  def getConnection(key: HBaseConnectionKey, conn: => Connection): SmartConnection = {
    connectionMap.synchronized {
      if (closed)
        return null
      val sc = connectionMap.getOrElseUpdate(key, new SmartConnection(conn))
      sc.refCount += 1
      sc
    }
  }

  def getConnection(conf: Configuration): SmartConnection =
    getConnection(new HBaseConnectionKey(conf), ConnectionFactory.createConnection(conf))

  // For testing purpose only
  def setTimeout(to: Long): Unit  = {
    connectionMap.synchronized {
      if (closed)
        return
      timeout = to
      housekeepingThread.interrupt()
    }
  }
}

private[hbase] case class SmartConnection (
    connection: Connection, var refCount: Int = 0, var timestamp: Long = 0) {
  def getTable(tableName: TableName): Table = connection.getTable(tableName)
  def getRegionLocator(tableName: TableName): RegionLocator = connection.getRegionLocator(tableName)
  def isClosed: Boolean = connection.isClosed
  def getAdmin: Admin = connection.getAdmin
  def close() = {
    HBaseConnectionCache.connectionMap.synchronized {
      refCount -= 1
      if(refCount <= 0)
        timestamp = System.currentTimeMillis()
    }
  }
}

/**
  * Denotes a unique key to an HBase Connection instance.
  * Please refer to 'org.apache.hadoop.hbase.client.HConnectionKey'.
  *
  * In essence, this class captures the properties in Configuration
  * that may be used in the process of establishing a connection.
  *
  */
class HBaseConnectionKey(c: Configuration) extends Logging {
  val conf: Configuration = c
  val CONNECTION_PROPERTIES: Array[String] = Array[String](
    HConstants.ZOOKEEPER_QUORUM,
    HConstants.ZOOKEEPER_ZNODE_PARENT,
    HConstants.ZOOKEEPER_CLIENT_PORT,
    HConstants.ZOOKEEPER_RECOVERABLE_WAITTIME,
    HConstants.HBASE_CLIENT_PAUSE,
    HConstants.HBASE_CLIENT_RETRIES_NUMBER,
    HConstants.HBASE_RPC_TIMEOUT_KEY,
    HConstants.HBASE_META_SCANNER_CACHING,
    HConstants.HBASE_CLIENT_INSTANCE_ID,
    HConstants.RPC_CODEC_CONF_KEY,
    HConstants.USE_META_REPLICAS,
    RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY)

  var username: String = _
  var m_properties = mutable.HashMap.empty[String, String]
  if (conf != null) {
    for (property <- CONNECTION_PROPERTIES) {
      val value: String = conf.get(property)
      if (value != null) {
        m_properties.+=((property, value))
      }
    }
    try {
      val provider: UserProvider = UserProvider.instantiate(conf)
      val currentUser: User = provider.getCurrent
      if (currentUser != null) {
        username = currentUser.getName
      }
    }
    catch {
      case e: IOException => {
        logWarning("Error obtaining current user, skipping username in HBaseConnectionKey", e)
      }
    }
  }

  // make 'properties' immutable
  val properties = m_properties.toMap

  override def hashCode: Int = {
    val prime: Int = 31
    var result: Int = 1
    if (username != null) {
      result = username.hashCode
    }
    for (property <- CONNECTION_PROPERTIES) {
      val value: Option[String] = properties.get(property)
      if (value.isDefined) {
        result = prime * result + value.hashCode
      }
    }
    result
  }

  override def equals(obj: Any): Boolean = {
    if (obj == null) return false
    if (getClass ne obj.getClass) return false
    val that: HBaseConnectionKey = obj.asInstanceOf[HBaseConnectionKey]
    if (this.username != null && !(this.username == that.username)) {
      return false
    }
    else if (this.username == null && that.username != null) {
      return false
    }
    if (this.properties == null) {
      if (that.properties != null) {
        return false
      }
    }
    else {
      if (that.properties == null) {
        return false
      }
      var flag: Boolean = true
      for (property <- CONNECTION_PROPERTIES) {
        val thisValue: Option[String] = this.properties.get(property)
        val thatValue: Option[String] = that.properties.get(property)
        flag = true
        if (thisValue eq thatValue) {
          flag = false //continue, so make flag be false
        }
        if (flag && (thisValue == null || !(thisValue == thatValue))) {
          return false
        }
      }
    }
    true
  }

  override def toString: String = {
    "HBaseConnectionKey{" + "properties=" + properties + ", username='" + username + '\'' + '}'
  }
}


