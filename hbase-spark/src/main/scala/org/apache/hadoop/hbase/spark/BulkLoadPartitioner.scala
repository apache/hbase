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

import java.util
import java.util.Comparator

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Partitioner

/**
 * A Partitioner implementation that will separate records to different
 * HBase Regions based on region splits
 *
 * @param startKeys   The start keys for the given table
 */
class BulkLoadPartitioner(startKeys:Array[Array[Byte]])
  extends Partitioner {

  override def numPartitions: Int = startKeys.length

  override def getPartition(key: Any): Int = {

    val rowKey:Array[Byte] =
      key match {
        case qualifier: KeyFamilyQualifier =>
          qualifier.rowKey
        case _ =>
          key.asInstanceOf[Array[Byte]]
      }

    val comparator: Comparator[Array[Byte]] = new Comparator[Array[Byte]] {
      override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
        Bytes.compareTo(o1, o2)
      }
    }
    val partition = util.Arrays.binarySearch(startKeys, rowKey, comparator)
    if (partition < 0) partition * -1 + -2
    else partition
  }
}
