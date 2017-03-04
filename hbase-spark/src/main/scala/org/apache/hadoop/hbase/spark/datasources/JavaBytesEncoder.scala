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

package org.apache.hadoop.hbase.spark.datasources

import org.apache.hadoop.hbase.spark.datasources.JavaBytesEncoder.JavaBytesEncoder
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.apache.spark.sql.types._

/**
  * The ranges for the data type whose size is known. Whether the bound is inclusive
  * or exclusive is undefind, and upper to the caller to decide.
  *
  * @param low: the lower bound of the range.
  * @param upper: the upper bound of the range.
  */
case class BoundRange(low: Array[Byte],upper: Array[Byte])

/**
  * The class identifies the ranges for a java primitive type. The caller needs
  * to decide the bound is either inclusive or exclusive on its own.
  * information
  *
  * @param less: the set of ranges for LessThan/LessOrEqualThan
  * @param greater: the set of ranges for GreaterThan/GreaterThanOrEqualTo
  * @param value: the byte array of the original value
  */
case class BoundRanges(less: Array[BoundRange], greater: Array[BoundRange], value: Array[Byte])

/**
  * The trait to support plugin architecture for different encoder/decoder.
  * encode is used for serializing the data type to byte array and the filter is
  * used to filter out the unnecessary records.
  */
trait BytesEncoder {
  def encode(dt: DataType, value: Any): Array[Byte]

  /**
    * The function performing real filtering operations. The format of filterBytes depends on the
    * implementation of the BytesEncoder.
    *
    * @param input: the current input byte array that needs to be filtered out
    * @param offset1: the starting offset of the input byte array.
    * @param length1: the length of the input byte array.
    * @param filterBytes: the byte array provided by query condition.
    * @param offset2: the starting offset in the filterBytes.
    * @param length2: the length of the bytes in the filterBytes
    * @param ops: The operation of the filter operator.
    * @return true: the record satisfies the predicates
    *         false: the record does not satisfy the predicates.
    */
  def filter(input: Array[Byte], offset1: Int, length1: Int,
             filterBytes: Array[Byte], offset2: Int, length2: Int,
             ops: JavaBytesEncoder): Boolean

  /**
    * Currently, it is used for partition pruning.
    * As for some codec, the order may be inconsistent between java primitive
    * type and its byte array. We may have to  split the predicates on some
    * of the java primitive type into multiple predicates.
    *
    * For example in naive codec,  some of the java primitive types have to be
    * split into multiple predicates, and union these predicates together to
    * make the predicates be performed correctly.
    * For example, if we have "COLUMN < 2", we will transform it into
    * "0 <= COLUMN < 2 OR Integer.MIN_VALUE <= COLUMN <= -1"
    */
  def ranges(in: Any): Option[BoundRanges]
}

object JavaBytesEncoder extends Enumeration with Logging{
  type JavaBytesEncoder = Value
  val Greater, GreaterEqual, Less, LessEqual, Equal, Unknown = Value

  /**
    * create the encoder/decoder
    *
    * @param clsName: the class name of the encoder/decoder class
    * @return the instance of the encoder plugin.
    */
  def create(clsName: String): BytesEncoder = {
    try {
      Class.forName(clsName).newInstance.asInstanceOf[BytesEncoder]
    } catch {
      case _: Throwable =>
        logWarning(s"$clsName cannot be initiated, falling back to naive encoder")
        new NaiveEncoder()
    }
  }
}