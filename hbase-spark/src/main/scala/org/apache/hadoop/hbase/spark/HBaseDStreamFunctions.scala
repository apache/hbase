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

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
 * HBaseDStreamFunctions contains a set of implicit functions that can be
 * applied to a Spark DStream so that we can easily interact with HBase
 */
object HBaseDStreamFunctions {

  /**
   * These are implicit methods for a DStream that contains any type of
   * data.
   *
   * @param dStream  This is for dStreams of any type
   * @tparam T       Type T
   */
  implicit class GenericHBaseDStreamFunctions[T](val dStream: DStream[T]) {

    /**
     * Implicit method that gives easy access to HBaseContext's bulk
     * put.  This will not return a new Stream.  Think of it like a foreach
     *
     * @param hc         The hbaseContext object to identify which
     *                   HBase cluster connection to use
     * @param tableName  The tableName that the put will be sent to
     * @param f          The function that will turn the DStream values
     *                   into HBase Put objects.
     */
    def hbaseBulkPut(hc: HBaseContext,
                     tableName: TableName,
                     f: (T) => Put): Unit = {
      hc.streamBulkPut(dStream, tableName, f)
    }

    /**
     * Implicit method that gives easy access to HBaseContext's bulk
     * get.  This will return a new DStream.  Think about it as a DStream map
     * function.  In that every DStream value will get a new value out of
     * HBase.  That new value will populate the newly generated DStream.
     *
     * @param hc             The hbaseContext object to identify which
     *                       HBase cluster connection to use
     * @param tableName      The tableName that the put will be sent to
     * @param batchSize      How many gets to execute in a single batch
     * @param f              The function that will turn the RDD values
     *                       in HBase Get objects
     * @param convertResult  The function that will convert a HBase
     *                       Result object into a value that will go
     *                       into the resulting DStream
     * @tparam R             The type of Object that will be coming
     *                       out of the resulting DStream
     * @return               A resulting DStream with type R objects
     */
    def hbaseBulkGet[R: ClassTag](hc: HBaseContext,
                     tableName: TableName,
                     batchSize:Int, f: (T) => Get, convertResult: (Result) => R):
    DStream[R] = {
      hc.streamBulkGet[T, R](tableName, batchSize, dStream, f, convertResult)
    }

    /**
     * Implicit method that gives easy access to HBaseContext's bulk
     * get.  This will return a new DStream.  Think about it as a DStream map
     * function.  In that every DStream value will get a new value out of
     * HBase.  That new value will populate the newly generated DStream.
     *
     * @param hc             The hbaseContext object to identify which
     *                       HBase cluster connection to use
     * @param tableName      The tableName that the put will be sent to
     * @param batchSize      How many gets to execute in a single batch
     * @param f              The function that will turn the RDD values
     *                       in HBase Get objects
     * @return               A resulting DStream with type R objects
     */
    def hbaseBulkGet(hc: HBaseContext,
                     tableName: TableName, batchSize:Int,
                     f: (T) => Get): DStream[(ImmutableBytesWritable, Result)] = {
        hc.streamBulkGet[T, (ImmutableBytesWritable, Result)](
          tableName, batchSize, dStream, f,
          result => (new ImmutableBytesWritable(result.getRow), result))
    }

    /**
     * Implicit method that gives easy access to HBaseContext's bulk
     * Delete.  This will not return a new DStream.
     *
     * @param hc         The hbaseContext object to identify which HBase
     *                   cluster connection to use
     * @param tableName  The tableName that the deletes will be sent to
     * @param f          The function that will convert the DStream value into
     *                   a HBase Delete Object
     * @param batchSize  The number of Deletes to be sent in a single batch
     */
    def hbaseBulkDelete(hc: HBaseContext,
                        tableName: TableName,
                        f:(T) => Delete, batchSize:Int): Unit = {
      hc.streamBulkDelete(dStream, tableName, f, batchSize)
    }

    /**
     * Implicit method that gives easy access to HBaseContext's
     * foreachPartition method.  This will ack very much like a normal DStream
     * foreach method but for the fact that you will now have a HBase connection
     * while iterating through the values.
     *
     * @param hc  The hbaseContext object to identify which HBase
     *            cluster connection to use
     * @param f   This function will get an iterator for a Partition of an
     *            DStream along with a connection object to HBase
     */
    def hbaseForeachPartition(hc: HBaseContext,
                              f: (Iterator[T], Connection) => Unit): Unit = {
      hc.streamForeachPartition(dStream, f)
    }

    /**
     * Implicit method that gives easy access to HBaseContext's
     * mapPartitions method.  This will ask very much like a normal DStream
     * map partitions method but for the fact that you will now have a
     * HBase connection while iterating through the values
     *
     * @param hc  The hbaseContext object to identify which HBase
     *            cluster connection to use
     * @param f   This function will get an iterator for a Partition of an
     *            DStream along with a connection object to HBase
     * @tparam R  This is the type of objects that will go into the resulting
     *            DStream
     * @return    A resulting DStream of type R
     */
    def hbaseMapPartitions[R: ClassTag](hc: HBaseContext,
                                        f: (Iterator[T], Connection) => Iterator[R]):
    DStream[R] = {
      hc.streamMapPartitions(dStream, f)
    }
  }
}
