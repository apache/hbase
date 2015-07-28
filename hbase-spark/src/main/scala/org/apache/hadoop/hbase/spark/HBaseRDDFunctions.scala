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
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * HBaseRDDFunctions contains a set of implicit functions that can be
 * applied to a Spark RDD so that we can easily interact with HBase
 */
object HBaseRDDFunctions
{

  /**
   * These are implicit methods for a RDD that contains any type of
   * data.
   *
   * @param rdd This is for rdd of any type
   * @tparam T  This is any type
   */
  implicit class GenericHBaseRDDFunctions[T](val rdd: RDD[T]) {

    /**
     * Implicit method that gives easy access to HBaseContext's bulk
     * put.  This will not return a new RDD.  Think of it like a foreach
     *
     * @param hc         The hbaseContext object to identify which
     *                   HBase cluster connection to use
     * @param tableName  The tableName that the put will be sent to
     * @param f          The function that will turn the RDD values
     *                   into HBase Put objects.
     */
    def hbaseBulkPut(hc: HBaseContext,
                     tableName: TableName,
                     f: (T) => Put): Unit = {
      hc.bulkPut(rdd, tableName, f)
    }

    /**
     * Implicit method that gives easy access to HBaseContext's bulk
     * get.  This will return a new RDD.  Think about it as a RDD map
     * function.  In that every RDD value will get a new value out of
     * HBase.  That new value will populate the newly generated RDD.
     *
     * @param hc             The hbaseContext object to identify which
     *                       HBase cluster connection to use
     * @param tableName      The tableName that the put will be sent to
     * @param batchSize      How many gets to execute in a single batch
     * @param f              The function that will turn the RDD values
     *                       in HBase Get objects
     * @param convertResult  The function that will convert a HBase
     *                       Result object into a value that will go
     *                       into the resulting RDD
     * @tparam R             The type of Object that will be coming
     *                       out of the resulting RDD
     * @return               A resulting RDD with type R objects
     */
    def hbaseBulkGet[R: ClassTag](hc: HBaseContext,
                            tableName: TableName, batchSize:Int,
                            f: (T) => Get, convertResult: (Result) => R): RDD[R] = {
      hc.bulkGet[T, R](tableName, batchSize, rdd, f, convertResult)
    }

    /**
     * Implicit method that gives easy access to HBaseContext's bulk
     * get.  This will return a new RDD.  Think about it as a RDD map
     * function.  In that every RDD value will get a new value out of
     * HBase.  That new value will populate the newly generated RDD.
     *
     * @param hc             The hbaseContext object to identify which
     *                       HBase cluster connection to use
     * @param tableName      The tableName that the put will be sent to
     * @param batchSize      How many gets to execute in a single batch
     * @param f              The function that will turn the RDD values
     *                       in HBase Get objects
     * @return               A resulting RDD with type R objects
     */
    def hbaseBulkGet(hc: HBaseContext,
                                  tableName: TableName, batchSize:Int,
                                  f: (T) => Get): RDD[(ImmutableBytesWritable, Result)] = {
      hc.bulkGet[T, (ImmutableBytesWritable, Result)](tableName,
        batchSize, rdd, f,
        result => if (result != null && result.getRow != null) {
          (new ImmutableBytesWritable(result.getRow), result)
        } else {
          null
        })
    }

    /**
     * Implicit method that gives easy access to HBaseContext's bulk
     * Delete.  This will not return a new RDD.
     *
     * @param hc         The hbaseContext object to identify which HBase
     *                   cluster connection to use
     * @param tableName  The tableName that the deletes will be sent to
     * @param f          The function that will convert the RDD value into
     *                   a HBase Delete Object
     * @param batchSize  The number of Deletes to be sent in a single batch
     */
    def hbaseBulkDelete(hc: HBaseContext,
                        tableName: TableName, f:(T) => Delete, batchSize:Int): Unit = {
      hc.bulkDelete(rdd, tableName, f, batchSize)
    }

    /**
     * Implicit method that gives easy access to HBaseContext's
     * foreachPartition method.  This will ack very much like a normal RDD
     * foreach method but for the fact that you will now have a HBase connection
     * while iterating through the values.
     *
     * @param hc  The hbaseContext object to identify which HBase
     *            cluster connection to use
     * @param f   This function will get an iterator for a Partition of an
     *            RDD along with a connection object to HBase
     */
    def hbaseForeachPartition(hc: HBaseContext,
                              f: (Iterator[T], Connection) => Unit): Unit = {
      hc.foreachPartition(rdd, f)
    }

    /**
     * Implicit method that gives easy access to HBaseContext's
     * mapPartitions method.  This will ask very much like a normal RDD
     * map partitions method but for the fact that you will now have a
     * HBase connection while iterating through the values
     *
     * @param hc  The hbaseContext object to identify which HBase
     *            cluster connection to use
     * @param f   This function will get an iterator for a Partition of an
     *            RDD along with a connection object to HBase
     * @tparam R  This is the type of objects that will go into the resulting
     *            RDD
     * @return    A resulting RDD of type R
     */
    def hbaseMapPartitions[R: ClassTag](hc: HBaseContext,
                                        f: (Iterator[T], Connection) => Iterator[R]):
    RDD[R] = {
      hc.mapPartitions[T,R](rdd, f)
    }
  }
}
