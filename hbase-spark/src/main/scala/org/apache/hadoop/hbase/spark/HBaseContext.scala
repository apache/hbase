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

import java.net.InetSocketAddress
import java.util
import javax.management.openmbean.KeyAlreadyExistsException

import org.apache.hadoop.hbase.fs.HFileSystem
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.io.hfile.{CacheConfig, HFileContextBuilder, HFileWriterImpl}
import org.apache.hadoop.hbase.regionserver.{HStore, StoreFile, BloomType}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.client._
import scala.reflect.ClassTag
import org.apache.spark.{Logging, SerializableWritable, SparkContext}
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil,
TableInputFormat, IdentityTableMapper}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.streaming.dstream.DStream
import java.io._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.hadoop.fs.{Path, FileSystem}
import scala.collection.mutable

/**
  * HBaseContext is a façade for HBase operations
  * like bulk put, get, increment, delete, and scan
  *
  * HBaseContext will take the responsibilities
  * of disseminating the configuration information
  * to the working and managing the life cycle of HConnections.
 */
class HBaseContext(@transient sc: SparkContext,
                   @transient val config: Configuration,
                   val tmpHdfsConfgFile: String = null)
  extends Serializable with Logging {

  @transient var credentials = SparkHadoopUtil.get.getCurrentUserCredentials()
  @transient var tmpHdfsConfiguration:Configuration = config
  @transient var appliedCredentials = false
  @transient val job = Job.getInstance(config)
  TableMapReduceUtil.initCredentials(job)
  val broadcastedConf = sc.broadcast(new SerializableWritable(config))
  val credentialsConf = sc.broadcast(new SerializableWritable(job.getCredentials))

  LatestHBaseContextCache.latest = this

  if (tmpHdfsConfgFile != null && config != null) {
    val fs = FileSystem.newInstance(config)
    val tmpPath = new Path(tmpHdfsConfgFile)
    if (!fs.exists(tmpPath)) {
      val outputStream = fs.create(tmpPath)
      config.write(outputStream)
      outputStream.close()
    } else {
      logWarning("tmpHdfsConfigDir " + tmpHdfsConfgFile + " exist!!")
    }
  }

  /**
   * A simple enrichment of the traditional Spark RDD foreachPartition.
   * This function differs from the original in that it offers the
   * developer access to a already connected HConnection object
   *
   * Note: Do not close the HConnection object.  All HConnection
   * management is handled outside this method
   *
   * @param rdd  Original RDD with data to iterate over
   * @param f    Function to be given a iterator to iterate through
   *             the RDD values and a HConnection object to interact
   *             with HBase
   */
  def foreachPartition[T](rdd: RDD[T],
                          f: (Iterator[T], Connection) => Unit):Unit = {
    rdd.foreachPartition(
      it => hbaseForeachPartition(broadcastedConf, it, f))
  }

  /**
   * A simple enrichment of the traditional Spark Streaming dStream foreach
   * This function differs from the original in that it offers the
   * developer access to a already connected HConnection object
   *
   * Note: Do not close the HConnection object.  All HConnection
   * management is handled outside this method
   *
   * @param dstream  Original DStream with data to iterate over
   * @param f        Function to be given a iterator to iterate through
   *                 the DStream values and a HConnection object to
   *                 interact with HBase
   */
  def foreachPartition[T](dstream: DStream[T],
                    f: (Iterator[T], Connection) => Unit):Unit = {
    dstream.foreachRDD((rdd, time) => {
      foreachPartition(rdd, f)
    })
  }

  /**
   * A simple enrichment of the traditional Spark RDD mapPartition.
   * This function differs from the original in that it offers the
   * developer access to a already connected HConnection object
   *
   * Note: Do not close the HConnection object.  All HConnection
   * management is handled outside this method
   *
   * @param rdd  Original RDD with data to iterate over
   * @param mp   Function to be given a iterator to iterate through
   *             the RDD values and a HConnection object to interact
   *             with HBase
   * @return     Returns a new RDD generated by the user definition
   *             function just like normal mapPartition
   */
  def mapPartitions[T, R: ClassTag](rdd: RDD[T],
                                   mp: (Iterator[T], Connection) => Iterator[R]): RDD[R] = {

    rdd.mapPartitions[R](it => hbaseMapPartition[T, R](broadcastedConf,
      it,
      mp))

  }

  /**
   * A simple enrichment of the traditional Spark Streaming DStream
   * foreachPartition.
   *
   * This function differs from the original in that it offers the
   * developer access to a already connected HConnection object
   *
   * Note: Do not close the HConnection object.  All HConnection
   * management is handled outside this method
   *
   * Note: Make sure to partition correctly to avoid memory issue when
   *       getting data from HBase
   *
   * @param dstream  Original DStream with data to iterate over
   * @param f       Function to be given a iterator to iterate through
   *                 the DStream values and a HConnection object to
   *                 interact with HBase
   * @return         Returns a new DStream generated by the user
   *                 definition function just like normal mapPartition
   */
  def streamForeachPartition[T](dstream: DStream[T],
                                f: (Iterator[T], Connection) => Unit): Unit = {

    dstream.foreachRDD(rdd => this.foreachPartition(rdd, f))
  }

  /**
   * A simple enrichment of the traditional Spark Streaming DStream
   * mapPartition.
   *
   * This function differs from the original in that it offers the
   * developer access to a already connected HConnection object
   *
   * Note: Do not close the HConnection object.  All HConnection
   * management is handled outside this method
   *
   * Note: Make sure to partition correctly to avoid memory issue when
   *       getting data from HBase
   *
   * @param dstream  Original DStream with data to iterate over
   * @param f       Function to be given a iterator to iterate through
   *                 the DStream values and a HConnection object to
   *                 interact with HBase
   * @return         Returns a new DStream generated by the user
   *                 definition function just like normal mapPartition
   */
  def streamMapPartitions[T, U: ClassTag](dstream: DStream[T],
                                f: (Iterator[T], Connection) => Iterator[U]):
  DStream[U] = {
    dstream.mapPartitions(it => hbaseMapPartition[T, U](
      broadcastedConf,
      it,
      f))
  }

  /**
   * A simple abstraction over the HBaseContext.foreachPartition method.
   *
   * It allow addition support for a user to take RDD
   * and generate puts and send them to HBase.
   * The complexity of managing the HConnection is
   * removed from the developer
   *
   * @param rdd       Original RDD with data to iterate over
   * @param tableName The name of the table to put into
   * @param f         Function to convert a value in the RDD to a HBase Put
   */
  def bulkPut[T](rdd: RDD[T], tableName: TableName, f: (T) => Put) {

    val tName = tableName.getName
    rdd.foreachPartition(
      it => hbaseForeachPartition[T](
        broadcastedConf,
        it,
        (iterator, connection) => {
          val m = connection.getBufferedMutator(TableName.valueOf(tName))
          iterator.foreach(T => m.mutate(f(T)))
          m.flush()
          m.close()
        }))
  }

  def applyCreds[T] (configBroadcast: Broadcast[SerializableWritable[Configuration]]){
    credentials = SparkHadoopUtil.get.getCurrentUserCredentials()

    logDebug("appliedCredentials:" + appliedCredentials + ",credentials:" + credentials)

    if (!appliedCredentials && credentials != null) {
      appliedCredentials = true

      @transient val ugi = UserGroupInformation.getCurrentUser
      ugi.addCredentials(credentials)
      // specify that this is a proxy user
      ugi.setAuthenticationMethod(AuthenticationMethod.PROXY)

      ugi.addCredentials(credentialsConf.value.value)
    }
  }

  /**
   * A simple abstraction over the HBaseContext.streamMapPartition method.
   *
   * It allow addition support for a user to take a DStream and
   * generate puts and send them to HBase.
   *
   * The complexity of managing the HConnection is
   * removed from the developer
   *
   * @param dstream    Original DStream with data to iterate over
   * @param tableName  The name of the table to put into
   * @param f          Function to convert a value in
   *                   the DStream to a HBase Put
   */
  def streamBulkPut[T](dstream: DStream[T],
                       tableName: TableName,
                       f: (T) => Put) = {
    val tName = tableName.getName
    dstream.foreachRDD((rdd, time) => {
      bulkPut(rdd, TableName.valueOf(tName), f)
    })
  }

  /**
   * A simple abstraction over the HBaseContext.foreachPartition method.
   *
   * It allow addition support for a user to take a RDD and generate delete
   * and send them to HBase.  The complexity of managing the HConnection is
   * removed from the developer
   *
   * @param rdd       Original RDD with data to iterate over
   * @param tableName The name of the table to delete from
   * @param f         Function to convert a value in the RDD to a
   *                  HBase Deletes
   * @param batchSize       The number of delete to batch before sending to HBase
   */
  def bulkDelete[T](rdd: RDD[T], tableName: TableName,
                    f: (T) => Delete, batchSize: Integer) {
    bulkMutation(rdd, tableName, f, batchSize)
  }

  /**
   * A simple abstraction over the HBaseContext.streamBulkMutation method.
   *
   * It allow addition support for a user to take a DStream and
   * generate Delete and send them to HBase.
   *
   * The complexity of managing the HConnection is
   * removed from the developer
   *
   * @param dstream    Original DStream with data to iterate over
   * @param tableName  The name of the table to delete from
   * @param f          function to convert a value in the DStream to a
   *                   HBase Delete
   * @param batchSize        The number of deletes to batch before sending to HBase
   */
  def streamBulkDelete[T](dstream: DStream[T],
                          tableName: TableName,
                          f: (T) => Delete,
                          batchSize: Integer) = {
    streamBulkMutation(dstream, tableName, f, batchSize)
  }

  /**
   *  Under lining function to support all bulk mutations
   *
   *  May be opened up if requested
   */
  private def bulkMutation[T](rdd: RDD[T], tableName: TableName,
                              f: (T) => Mutation, batchSize: Integer) {

    val tName = tableName.getName
    rdd.foreachPartition(
      it => hbaseForeachPartition[T](
        broadcastedConf,
        it,
        (iterator, connection) => {
          val table = connection.getTable(TableName.valueOf(tName))
          val mutationList = new java.util.ArrayList[Mutation]
          iterator.foreach(T => {
            mutationList.add(f(T))
            if (mutationList.size >= batchSize) {
              table.batch(mutationList, null)
              mutationList.clear()
            }
          })
          if (mutationList.size() > 0) {
            table.batch(mutationList, null)
            mutationList.clear()
          }
          table.close()
        }))
  }

  /**
   *  Under lining function to support all bulk streaming mutations
   *
   *  May be opened up if requested
   */
  private def streamBulkMutation[T](dstream: DStream[T],
                                    tableName: TableName,
                                    f: (T) => Mutation,
                                    batchSize: Integer) = {
    val tName = tableName.getName
    dstream.foreachRDD((rdd, time) => {
      bulkMutation(rdd, TableName.valueOf(tName), f, batchSize)
    })
  }

  /**
   * A simple abstraction over the HBaseContext.mapPartition method.
   *
   * It allow addition support for a user to take a RDD and generates a
   * new RDD based on Gets and the results they bring back from HBase
   *
   * @param rdd     Original RDD with data to iterate over
   * @param tableName        The name of the table to get from
   * @param makeGet    function to convert a value in the RDD to a
   *                   HBase Get
   * @param convertResult This will convert the HBase Result object to
   *                   what ever the user wants to put in the resulting
   *                   RDD
   * return            new RDD that is created by the Get to HBase
   */
  def bulkGet[T, U: ClassTag](tableName: TableName,
                    batchSize: Integer,
                    rdd: RDD[T],
                    makeGet: (T) => Get,
                    convertResult: (Result) => U): RDD[U] = {

    val getMapPartition = new GetMapPartition(tableName,
      batchSize,
      makeGet,
      convertResult)

    rdd.mapPartitions[U](it =>
      hbaseMapPartition[T, U](
        broadcastedConf,
        it,
        getMapPartition.run))
  }

  /**
   * A simple abstraction over the HBaseContext.streamMap method.
   *
   * It allow addition support for a user to take a DStream and
   * generates a new DStream based on Gets and the results
   * they bring back from HBase
   *
   * @param tableName     The name of the table to get from
   * @param batchSize     The number of Gets to be sent in a single batch
   * @param dStream       Original DStream with data to iterate over
   * @param makeGet       Function to convert a value in the DStream to a
   *                      HBase Get
   * @param convertResult This will convert the HBase Result object to
   *                      what ever the user wants to put in the resulting
   *                      DStream
   * @return              A new DStream that is created by the Get to HBase
   */
  def streamBulkGet[T, U: ClassTag](tableName: TableName,
                                    batchSize: Integer,
                                    dStream: DStream[T],
                                    makeGet: (T) => Get,
                                    convertResult: (Result) => U): DStream[U] = {

    val getMapPartition = new GetMapPartition(tableName,
      batchSize,
      makeGet,
      convertResult)

    dStream.mapPartitions[U](it => hbaseMapPartition[T, U](
      broadcastedConf,
      it,
      getMapPartition.run))
  }

  /**
   * This function will use the native HBase TableInputFormat with the
   * given scan object to generate a new RDD
   *
   *  @param tableName the name of the table to scan
   *  @param scan      the HBase scan object to use to read data from HBase
   *  @param f         function to convert a Result object from HBase into
   *                   what the user wants in the final generated RDD
   *  @return          new RDD with results from scan
   */
  def hbaseRDD[U: ClassTag](tableName: TableName, scan: Scan,
                            f: ((ImmutableBytesWritable, Result)) => U): RDD[U] = {

    val job: Job = Job.getInstance(getConf(broadcastedConf))

    TableMapReduceUtil.initCredentials(job)
    TableMapReduceUtil.initTableMapperJob(tableName, scan,
      classOf[IdentityTableMapper], null, null, job)

    sc.newAPIHadoopRDD(job.getConfiguration,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).map(f)
  }

  /**
   * A overloaded version of HBaseContext hbaseRDD that defines the
   * type of the resulting RDD
   *
   *  @param tableName the name of the table to scan
   *  @param scans     the HBase scan object to use to read data from HBase
   *  @return          New RDD with results from scan
   *
   */
  def hbaseRDD(tableName: TableName, scans: Scan):
  RDD[(ImmutableBytesWritable, Result)] = {

    hbaseRDD[(ImmutableBytesWritable, Result)](
      tableName,
      scans,
      (r: (ImmutableBytesWritable, Result)) => r)
  }

  /**
   *  underlining wrapper all foreach functions in HBaseContext
   */
  private def hbaseForeachPartition[T](configBroadcast:
                                       Broadcast[SerializableWritable[Configuration]],
                                        it: Iterator[T],
                                        f: (Iterator[T], Connection) => Unit) = {

    val config = getConf(configBroadcast)

    applyCreds(configBroadcast)
    // specify that this is a proxy user
    val connection = ConnectionFactory.createConnection(config)
    f(it, connection)
    connection.close()
  }

  private def getConf(configBroadcast: Broadcast[SerializableWritable[Configuration]]):
  Configuration = {

    if (tmpHdfsConfiguration == null && tmpHdfsConfgFile != null) {
      val fs = FileSystem.newInstance(SparkHadoopUtil.get.conf)
      val inputStream = fs.open(new Path(tmpHdfsConfgFile))
      tmpHdfsConfiguration = new Configuration(false)
      tmpHdfsConfiguration.readFields(inputStream)
      inputStream.close()
    }

    if (tmpHdfsConfiguration == null) {
      try {
        tmpHdfsConfiguration = configBroadcast.value.value
      } catch {
        case ex: Exception => logError("Unable to getConfig from broadcast", ex)
      }
    }
    tmpHdfsConfiguration
  }

  /**
   *  underlining wrapper all mapPartition functions in HBaseContext
   *
   */
  private def hbaseMapPartition[K, U](
                                       configBroadcast:
                                       Broadcast[SerializableWritable[Configuration]],
                                       it: Iterator[K],
                                       mp: (Iterator[K], Connection) =>
                                         Iterator[U]): Iterator[U] = {

    val config = getConf(configBroadcast)
    applyCreds(configBroadcast)

    val connection = ConnectionFactory.createConnection(config)
    val res = mp(it, connection)
    connection.close()
    res

  }

  /**
   *  underlining wrapper all get mapPartition functions in HBaseContext
   */
  private class GetMapPartition[T, U](tableName: TableName,
                                      batchSize: Integer,
                                      makeGet: (T) => Get,
                                      convertResult: (Result) => U)
    extends Serializable {

    val tName = tableName.getName

    def run(iterator: Iterator[T], connection: Connection): Iterator[U] = {
      val table = connection.getTable(TableName.valueOf(tName))

      val gets = new java.util.ArrayList[Get]()
      var res = List[U]()

      while (iterator.hasNext) {
        gets.add(makeGet(iterator.next()))

        if (gets.size() == batchSize) {
          val results = table.get(gets)
          res = res ++ results.map(convertResult)
          gets.clear()
        }
      }
      if (gets.size() > 0) {
        val results = table.get(gets)
        res = res ++ results.map(convertResult)
        gets.clear()
      }
      table.close()
      res.iterator
    }
  }

  /**
   * Produces a ClassTag[T], which is actually just a casted ClassTag[AnyRef].
   *
   * This method is used to keep ClassTags out of the external Java API, as
   * the Java compiler cannot produce them automatically. While this
   * ClassTag-faking does please the compiler, it can cause problems at runtime
   * if the Scala API relies on ClassTags for correctness.
   *
   * Often, though, a ClassTag[AnyRef] will not lead to incorrect behavior,
   * just worse performance or security issues.
   * For instance, an Array of AnyRef can hold any type T, but may lose primitive
   * specialization.
   */
  private[spark]
  def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]

  /**
   * Spark Implementation of HBase Bulk load for wide rows or when
   * values are not already combined at the time of the map process
   *
   * This will take the content from an existing RDD then sort and shuffle
   * it with respect to region splits.  The result of that sort and shuffle
   * will be written to HFiles.
   *
   * After this function is executed the user will have to call
   * LoadIncrementalHFiles.doBulkLoad(...) to move the files into HBase
   *
   * Also note this version of bulk load is different from past versions in
   * that it includes the qualifier as part of the sort process. The
   * reason for this is to be able to support rows will very large number
   * of columns.
   *
   * @param rdd                            The RDD we are bulk loading from
   * @param tableName                      The HBase table we are loading into
   * @param flatMap                        A flapMap function that will make every
   *                                       row in the RDD
   *                                       into N cells for the bulk load
   * @param stagingDir                     The location on the FileSystem to bulk load into
   * @param familyHFileWriteOptionsMap     Options that will define how the HFile for a
   *                                       column family is written
   * @param compactionExclude              Compaction excluded for the HFiles
   * @param maxSize                        Max size for the HFiles before they roll
   * @tparam T                             The Type of values in the original RDD
   */
  def bulkLoad[T](rdd:RDD[T],
                  tableName: TableName,
                  flatMap: (T) => Iterator[(KeyFamilyQualifier, Array[Byte])],
                  stagingDir:String,
                  familyHFileWriteOptionsMap:
                  util.Map[Array[Byte], FamilyHFileWriteOptions] =
                  new util.HashMap[Array[Byte], FamilyHFileWriteOptions],
                  compactionExclude: Boolean = false,
                  maxSize:Long = HConstants.DEFAULT_MAX_FILE_SIZE):
  Unit = {
    val conn = ConnectionFactory.createConnection(config)
    val regionLocator = conn.getRegionLocator(tableName)
    val startKeys = regionLocator.getStartKeys
    val defaultCompressionStr = config.get("hfile.compression",
      Compression.Algorithm.NONE.getName)
    val hfileCompression = HFileWriterImpl
      .compressionByName(defaultCompressionStr)
    val nowTimeStamp = System.currentTimeMillis()
    val tableRawName = tableName.getName

    val familyHFileWriteOptionsMapInternal =
      new util.HashMap[ByteArrayWrapper, FamilyHFileWriteOptions]

    val entrySetIt = familyHFileWriteOptionsMap.entrySet().iterator()

    while (entrySetIt.hasNext) {
      val entry = entrySetIt.next()
      familyHFileWriteOptionsMapInternal.put(new ByteArrayWrapper(entry.getKey), entry.getValue)
    }

    val regionSplitPartitioner =
      new BulkLoadPartitioner(startKeys)

    //This is where all the magic happens
    //Here we are going to do the following things
    // 1. FlapMap every row in the RDD into key column value tuples
    // 2. Then we are going to repartition sort and shuffle
    // 3. Finally we are going to write out our HFiles
    rdd.flatMap( r => flatMap(r)).
      repartitionAndSortWithinPartitions(regionSplitPartitioner).
      hbaseForeachPartition(this, (it, conn) => {

      val conf = broadcastedConf.value.value
      val fs = FileSystem.get(conf)
      val writerMap = new mutable.HashMap[ByteArrayWrapper, WriterLength]
      var previousRow:Array[Byte] = HConstants.EMPTY_BYTE_ARRAY
      var rollOverRequested = false
      val localTableName = TableName.valueOf(tableRawName)

      //Here is where we finally iterate through the data in this partition of the
      //RDD that has been sorted and partitioned
      it.foreach{ case (keyFamilyQualifier, cellValue:Array[Byte]) =>

        val wl = writeValueToHFile(keyFamilyQualifier.rowKey,
          keyFamilyQualifier.family,
          keyFamilyQualifier.qualifier,
          cellValue,
          nowTimeStamp,
          fs,
          conn,
          localTableName,
          conf,
          familyHFileWriteOptionsMapInternal,
          hfileCompression,
          writerMap,
          stagingDir)

        rollOverRequested = rollOverRequested || wl.written > maxSize

        //This will only roll if we have at least one column family file that is
        //bigger then maxSize and we have finished a given row key
        if (rollOverRequested && Bytes.compareTo(previousRow, keyFamilyQualifier.rowKey) != 0) {
          rollWriters(writerMap,
            regionSplitPartitioner,
            previousRow,
            compactionExclude)
          rollOverRequested = false
        }

        previousRow = keyFamilyQualifier.rowKey
      }
      //We have finished all the data so lets close up the writers
      rollWriters(writerMap,
        regionSplitPartitioner,
        previousRow,
        compactionExclude)
      rollOverRequested = false
    })
  }

  /**
   * Spark Implementation of HBase Bulk load for short rows some where less then
   * a 1000 columns.  This bulk load should be faster for tables will thinner
   * rows then the other spark implementation of bulk load that puts only one
   * value into a record going into a shuffle
   *
   * This will take the content from an existing RDD then sort and shuffle
   * it with respect to region splits.  The result of that sort and shuffle
   * will be written to HFiles.
   *
   * After this function is executed the user will have to call
   * LoadIncrementalHFiles.doBulkLoad(...) to move the files into HBase
   *
   * In this implementation, only the rowKey is given to the shuffle as the key
   * and all the columns are already linked to the RowKey before the shuffle
   * stage.  The sorting of the qualifier is done in memory out side of the
   * shuffle stage
   *
   * Also make sure that incoming RDDs only have one record for every row key.
   *
   * @param rdd                            The RDD we are bulk loading from
   * @param tableName                      The HBase table we are loading into
   * @param mapFunction                    A function that will convert the RDD records to
   *                                       the key value format used for the shuffle to prep
   *                                       for writing to the bulk loaded HFiles
   * @param stagingDir                     The location on the FileSystem to bulk load into
   * @param familyHFileWriteOptionsMap     Options that will define how the HFile for a
   *                                       column family is written
   * @param compactionExclude              Compaction excluded for the HFiles
   * @param maxSize                        Max size for the HFiles before they roll
   * @tparam T                             The Type of values in the original RDD
   */
  def bulkLoadThinRows[T](rdd:RDD[T],
                  tableName: TableName,
                  mapFunction: (T) =>
                    (ByteArrayWrapper, FamiliesQualifiersValues),
                  stagingDir:String,
                  familyHFileWriteOptionsMap:
                  util.Map[Array[Byte], FamilyHFileWriteOptions] =
                  new util.HashMap[Array[Byte], FamilyHFileWriteOptions],
                  compactionExclude: Boolean = false,
                  maxSize:Long = HConstants.DEFAULT_MAX_FILE_SIZE):
  Unit = {
    val conn = ConnectionFactory.createConnection(config)
    val regionLocator = conn.getRegionLocator(tableName)
    val startKeys = regionLocator.getStartKeys
    val defaultCompressionStr = config.get("hfile.compression",
      Compression.Algorithm.NONE.getName)
    val defaultCompression = HFileWriterImpl
      .compressionByName(defaultCompressionStr)
    val nowTimeStamp = System.currentTimeMillis()
    val tableRawName = tableName.getName

    val familyHFileWriteOptionsMapInternal =
      new util.HashMap[ByteArrayWrapper, FamilyHFileWriteOptions]

    val entrySetIt = familyHFileWriteOptionsMap.entrySet().iterator()

    while (entrySetIt.hasNext) {
      val entry = entrySetIt.next()
      familyHFileWriteOptionsMapInternal.put(new ByteArrayWrapper(entry.getKey), entry.getValue)
    }

    val regionSplitPartitioner =
      new BulkLoadPartitioner(startKeys)

    //This is where all the magic happens
    //Here we are going to do the following things
    // 1. FlapMap every row in the RDD into key column value tuples
    // 2. Then we are going to repartition sort and shuffle
    // 3. Finally we are going to write out our HFiles
    rdd.map( r => mapFunction(r)).
      repartitionAndSortWithinPartitions(regionSplitPartitioner).
      hbaseForeachPartition(this, (it, conn) => {

      val conf = broadcastedConf.value.value
      val fs = FileSystem.get(conf)
      val writerMap = new mutable.HashMap[ByteArrayWrapper, WriterLength]
      var previousRow:Array[Byte] = HConstants.EMPTY_BYTE_ARRAY
      var rollOverRequested = false
      val localTableName = TableName.valueOf(tableRawName)

      //Here is where we finally iterate through the data in this partition of the
      //RDD that has been sorted and partitioned
      it.foreach{ case (rowKey:ByteArrayWrapper,
      familiesQualifiersValues:FamiliesQualifiersValues) =>


        if (Bytes.compareTo(previousRow, rowKey.value) == 0) {
          throw new KeyAlreadyExistsException("The following key was sent to the " +
            "HFile load more then one: " + Bytes.toString(previousRow))
        }

        //The family map is a tree map so the families will be sorted
        val familyIt = familiesQualifiersValues.familyMap.entrySet().iterator()
        while (familyIt.hasNext) {
          val familyEntry = familyIt.next()

          val family = familyEntry.getKey.value

          val qualifierIt = familyEntry.getValue.entrySet().iterator()

          //The qualifier map is a tree map so the families will be sorted
          while (qualifierIt.hasNext) {

            val qualifierEntry = qualifierIt.next()
            val qualifier = qualifierEntry.getKey
            val cellValue = qualifierEntry.getValue

            writeValueToHFile(rowKey.value,
              family,
              qualifier.value, // qualifier
              cellValue, // value
              nowTimeStamp,
              fs,
              conn,
              localTableName,
              conf,
              familyHFileWriteOptionsMapInternal,
              defaultCompression,
              writerMap,
              stagingDir)

            previousRow = rowKey.value
          }

          writerMap.values.foreach( wl => {
            rollOverRequested = rollOverRequested || wl.written > maxSize

            //This will only roll if we have at least one column family file that is
            //bigger then maxSize and we have finished a given row key
            if (rollOverRequested) {
              rollWriters(writerMap,
                regionSplitPartitioner,
                previousRow,
                compactionExclude)
              rollOverRequested = false
            }
          })
        }
      }

      //This will get a writer for the column family
      //If there is no writer for a given column family then
      //it will get created here.
      //We have finished all the data so lets close up the writers
      rollWriters(writerMap,
        regionSplitPartitioner,
        previousRow,
        compactionExclude)
      rollOverRequested = false
    })
  }

  /**
   *  This will return a new HFile writer when requested
   *
   * @param family       column family
   * @param conf         configuration to connect to HBase
   * @param favoredNodes nodes that we would like to write too
   * @param fs           FileSystem object where we will be writing the HFiles to
   * @return WriterLength object
   */
  private def getNewHFileWriter(family: Array[Byte], conf: Configuration,
                   favoredNodes: Array[InetSocketAddress],
                   fs:FileSystem,
                   familydir:Path,
                   familyHFileWriteOptionsMapInternal:
                   util.HashMap[ByteArrayWrapper, FamilyHFileWriteOptions],
                   defaultCompression:Compression.Algorithm): WriterLength = {


    var familyOptions = familyHFileWriteOptionsMapInternal.get(new ByteArrayWrapper(family))

    if (familyOptions == null) {
      familyOptions = new FamilyHFileWriteOptions(defaultCompression.toString,
        BloomType.NONE.toString, HConstants.DEFAULT_BLOCKSIZE, DataBlockEncoding.NONE.toString)
      familyHFileWriteOptionsMapInternal.put(new ByteArrayWrapper(family), familyOptions)
    }

    val tempConf = new Configuration(conf)
    tempConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f)
    val contextBuilder = new HFileContextBuilder()
      .withCompression(Algorithm.valueOf(familyOptions.compression))
      .withChecksumType(HStore.getChecksumType(conf))
      .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf))
      .withBlockSize(familyOptions.blockSize)
    contextBuilder.withDataBlockEncoding(DataBlockEncoding.
      valueOf(familyOptions.dataBlockEncoding))
    val hFileContext = contextBuilder.build()

    if (null == favoredNodes) {
      new WriterLength(0, new StoreFile.WriterBuilder(conf, new CacheConfig(tempConf), fs)
        .withOutputDir(familydir).withBloomType(BloomType.valueOf(familyOptions.bloomType))
        .withComparator(CellComparator.COMPARATOR).withFileContext(hFileContext).build())
    } else {
      new WriterLength(0,
        new StoreFile.WriterBuilder(conf, new CacheConfig(tempConf), new HFileSystem(fs))
          .withOutputDir(familydir).withBloomType(BloomType.valueOf(familyOptions.bloomType))
          .withComparator(CellComparator.COMPARATOR).withFileContext(hFileContext)
          .withFavoredNodes(favoredNodes).build())
    }
  }

  /**
   * Encompasses the logic to write a value to an HFile
   *
   * @param rowKey                             The RowKey for the record
   * @param family                             HBase column family for the record
   * @param qualifier                          HBase column qualifier for the record
   * @param cellValue                          HBase cell value
   * @param nowTimeStamp                       The cell time stamp
   * @param fs                                 Connection to the FileSystem for the HFile
   * @param conn                               Connection to HBaes
   * @param tableName                          HBase TableName object
   * @param conf                               Configuration to be used when making a new HFile
   * @param familyHFileWriteOptionsMapInternal Extra configs for the HFile
   * @param hfileCompression                   The compression codec for the new HFile
   * @param writerMap                          HashMap of existing writers and their offsets
   * @param stagingDir                         The staging directory on the FileSystem to store
   *                                           the HFiles
   * @return                                   The writer for the given HFile that was writen
   *                                           too
   */
  private def writeValueToHFile(rowKey: Array[Byte],
                        family: Array[Byte],
                        qualifier: Array[Byte],
                        cellValue:Array[Byte],
                        nowTimeStamp: Long,
                        fs: FileSystem,
                        conn: Connection,
                        tableName: TableName,
                        conf: Configuration,
                        familyHFileWriteOptionsMapInternal:
                        util.HashMap[ByteArrayWrapper, FamilyHFileWriteOptions],
                        hfileCompression:Compression.Algorithm,
                        writerMap:mutable.HashMap[ByteArrayWrapper, WriterLength],
                        stagingDir: String
                         ): WriterLength = {

    val wl = writerMap.getOrElseUpdate(new ByteArrayWrapper(family), {
      val familyDir = new Path(stagingDir, Bytes.toString(family))

      fs.mkdirs(familyDir)

      val loc:HRegionLocation = {
        try {
          val locator =
            conn.getRegionLocator(tableName)
          locator.getRegionLocation(rowKey)
        } catch {
          case e: Throwable =>
            logWarning("there's something wrong when locating rowkey: " +
              Bytes.toString(rowKey))
            null
        }
      }
      if (null == loc) {
        if (log.isTraceEnabled) {
          logTrace("failed to get region location, so use default writer: " +
            Bytes.toString(rowKey))
        }
        getNewHFileWriter(family = family,
          conf = conf,
          favoredNodes = null,
          fs = fs,
          familydir = familyDir,
          familyHFileWriteOptionsMapInternal,
          hfileCompression)
      } else {
        if (log.isDebugEnabled) {
          logDebug("first rowkey: [" + Bytes.toString(rowKey) + "]")
        }
        val initialIsa =
          new InetSocketAddress(loc.getHostname, loc.getPort)
        if (initialIsa.isUnresolved) {
          if (log.isTraceEnabled) {
            logTrace("failed to resolve bind address: " + loc.getHostname + ":"
              + loc.getPort + ", so use default writer")
          }
          getNewHFileWriter(family,
            conf,
            null,
            fs,
            familyDir,
            familyHFileWriteOptionsMapInternal,
            hfileCompression)
        } else {
          if(log.isDebugEnabled) {
            logDebug("use favored nodes writer: " + initialIsa.getHostString)
          }
          getNewHFileWriter(family,
            conf,
            Array[InetSocketAddress](initialIsa),
            fs,
            familyDir,
            familyHFileWriteOptionsMapInternal,
            hfileCompression)
        }
      }
    })

    val keyValue =new KeyValue(rowKey,
      family,
      qualifier,
      nowTimeStamp,cellValue)

    wl.writer.append(keyValue)
    wl.written += keyValue.getLength

    wl
  }

  /**
   * This will roll all Writers
   * @param writerMap              HashMap that contains all the writers
   * @param regionSplitPartitioner The partitioner with knowledge of how the
   *                               Region's are split by row key
   * @param previousRow            The last row to fill the HFile ending range metadata
   * @param compactionExclude      The exclude compaction metadata flag for the HFile
   */
  private def rollWriters(writerMap:mutable.HashMap[ByteArrayWrapper, WriterLength],
                  regionSplitPartitioner: BulkLoadPartitioner,
                  previousRow: Array[Byte],
                  compactionExclude: Boolean): Unit = {
    writerMap.values.foreach( wl => {
      if (wl.writer != null) {
        logDebug("Writer=" + wl.writer.getPath +
          (if (wl.written == 0) "" else ", wrote=" + wl.written))
        closeHFileWriter(wl.writer,
          regionSplitPartitioner,
          previousRow,
          compactionExclude)
      }
    })
    writerMap.clear()

  }

  /**
   * Function to close an HFile
   * @param w                      HFile Writer
   * @param regionSplitPartitioner The partitioner with knowledge of how the
   *                               Region's are split by row key
   * @param previousRow            The last row to fill the HFile ending range metadata
   * @param compactionExclude      The exclude compaction metadata flag for the HFile
   */
  private def closeHFileWriter(w: StoreFile.Writer,
            regionSplitPartitioner: BulkLoadPartitioner,
            previousRow: Array[Byte],
            compactionExclude: Boolean): Unit = {
    if (w != null) {
      w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
        Bytes.toBytes(System.currentTimeMillis()))
      w.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY,
        Bytes.toBytes(regionSplitPartitioner.getPartition(previousRow)))
      w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY,
        Bytes.toBytes(true))
      w.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
        Bytes.toBytes(compactionExclude))
      w.appendTrackedTimestampsToMetadata()
      w.close()
    }
  }

  /**
   * This is a wrapper class around StoreFile.Writer.  The reason for the
   * wrapper is to keep the length of the file along side the writer
   *
   * @param written The writer to be wrapped
   * @param writer  The number of bytes written to the writer
   */
  class WriterLength(var written:Long, val writer:StoreFile.Writer)
}

object LatestHBaseContextCache {
  var latest:HBaseContext = null
}
