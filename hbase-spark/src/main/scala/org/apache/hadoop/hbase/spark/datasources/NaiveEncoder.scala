package org.apache.hadoop.hbase.spark.datasources
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

import org.apache.hadoop.hbase.spark.datasources.JavaBytesEncoder.JavaBytesEncoder
import org.apache.hadoop.hbase.spark.hbase._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


/**
  * This is the naive non-order preserving encoder/decoder.
  * Due to the inconsistency of the order between java primitive types
  * and their bytearray. The data type has to be passed in so that the filter
  * can work correctly, which is done by wrapping the type into the first byte
  * of the serialized array.
  */
class NaiveEncoder extends BytesEncoder with Logging{
  var code = 0
  def nextCode: Byte = {
    code += 1
    (code - 1).asInstanceOf[Byte]
  }
  val BooleanEnc = nextCode
  val ShortEnc = nextCode
  val IntEnc = nextCode
  val LongEnc = nextCode
  val FloatEnc = nextCode
  val DoubleEnc = nextCode
  val StringEnc = nextCode
  val BinaryEnc = nextCode
  val TimestampEnc = nextCode
  val UnknownEnc = nextCode


  /**
    * Evaluate the java primitive type and return the BoundRanges. For one value, it may have
    * multiple output ranges because of the inconsistency of order between java primitive type
    * and its byte array order.
    *
    * For short, integer, and long, the order of number is consistent with byte array order
    * if two number has the same sign bit. But the negative number is larger than positive
    * number in byte array.
    *
    * For double and float, the order of positive number is consistent with its byte array order.
    * But the order of negative number is the reverse order of byte array. Please refer to IEEE-754
    * and https://en.wikipedia.org/wiki/Single-precision_floating-point_format
    */
  def ranges(in: Any): Option[BoundRanges] = in match {
    case a: Integer =>
      val b =  Bytes.toBytes(a)
      if (a >= 0) {
        logDebug(s"range is 0 to $a and ${Integer.MIN_VALUE} to -1")
        Some(BoundRanges(
          Array(BoundRange(Bytes.toBytes(0: Int), b),
            BoundRange(Bytes.toBytes(Integer.MIN_VALUE),  Bytes.toBytes(-1: Int))),
          Array(BoundRange(b,  Bytes.toBytes(Integer.MAX_VALUE))), b))
      } else {
        Some(BoundRanges(
          Array(BoundRange(Bytes.toBytes(Integer.MIN_VALUE), b)),
          Array(BoundRange(b, Bytes.toBytes(-1: Integer)),
            BoundRange(Bytes.toBytes(0: Int), Bytes.toBytes(Integer.MAX_VALUE))), b))
      }
    case a: Long =>
      val b =  Bytes.toBytes(a)
      if (a >= 0) {
        Some(BoundRanges(
          Array(BoundRange(Bytes.toBytes(0: Long), b),
            BoundRange(Bytes.toBytes(Long.MinValue),  Bytes.toBytes(-1: Long))),
          Array(BoundRange(b,  Bytes.toBytes(Long.MaxValue))), b))
      } else {
        Some(BoundRanges(
          Array(BoundRange(Bytes.toBytes(Long.MinValue), b)),
          Array(BoundRange(b, Bytes.toBytes(-1: Long)),
            BoundRange(Bytes.toBytes(0: Long), Bytes.toBytes(Long.MaxValue))), b))
      }
    case a: Short =>
      val b =  Bytes.toBytes(a)
      if (a >= 0) {
        Some(BoundRanges(
          Array(BoundRange(Bytes.toBytes(0: Short), b),
            BoundRange(Bytes.toBytes(Short.MinValue),  Bytes.toBytes(-1: Short))),
          Array(BoundRange(b,  Bytes.toBytes(Short.MaxValue))), b))
      } else {
        Some(BoundRanges(
          Array(BoundRange(Bytes.toBytes(Short.MinValue), b)),
          Array(BoundRange(b, Bytes.toBytes(-1: Short)),
            BoundRange(Bytes.toBytes(0: Short), Bytes.toBytes(Short.MaxValue))), b))
      }
    case a: Double =>
      val b =  Bytes.toBytes(a)
      if (a >= 0.0f) {
        Some(BoundRanges(
          Array(BoundRange(Bytes.toBytes(0.0d), b),
            BoundRange(Bytes.toBytes(-0.0d),  Bytes.toBytes(Double.MinValue))),
          Array(BoundRange(b,  Bytes.toBytes(Double.MaxValue))), b))
      } else {
        Some(BoundRanges(
          Array(BoundRange(b, Bytes.toBytes(Double.MinValue))),
          Array(BoundRange(Bytes.toBytes(-0.0d), b),
            BoundRange(Bytes.toBytes(0.0d), Bytes.toBytes(Double.MaxValue))), b))
      }
    case a: Float =>
      val b =  Bytes.toBytes(a)
      if (a >= 0.0f) {
        Some(BoundRanges(
          Array(BoundRange(Bytes.toBytes(0.0f), b),
            BoundRange(Bytes.toBytes(-0.0f),  Bytes.toBytes(Float.MinValue))),
          Array(BoundRange(b,  Bytes.toBytes(Float.MaxValue))), b))
      } else {
        Some(BoundRanges(
          Array(BoundRange(b, Bytes.toBytes(Float.MinValue))),
          Array(BoundRange(Bytes.toBytes(-0.0f), b),
            BoundRange(Bytes.toBytes(0.0f), Bytes.toBytes(Float.MaxValue))), b))
      }
    case a: Array[Byte] =>
      Some(BoundRanges(
        Array(BoundRange(bytesMin, a)),
        Array(BoundRange(a, bytesMax)), a))
    case a: Byte =>
      val b =  Array(a)
      Some(BoundRanges(
        Array(BoundRange(bytesMin, b)),
        Array(BoundRange(b, bytesMax)), b))
    case a: String =>
      val b =  Bytes.toBytes(a)
      Some(BoundRanges(
        Array(BoundRange(bytesMin, b)),
        Array(BoundRange(b, bytesMax)), b))
    case a: UTF8String =>
      val b = a.getBytes
      Some(BoundRanges(
        Array(BoundRange(bytesMin, b)),
        Array(BoundRange(b, bytesMax)), b))
    case _ => None
  }

  def compare(c: Int, ops: JavaBytesEncoder): Boolean = {
    ops match {
      case JavaBytesEncoder.Greater =>  c > 0
      case JavaBytesEncoder.GreaterEqual =>  c >= 0
      case JavaBytesEncoder.Less =>  c < 0
      case JavaBytesEncoder.LessEqual =>  c <= 0
    }
  }

  /**
    * encode the data type into byte array. Note that it is a naive implementation with the
    * data type byte appending to the head of the serialized byte array.
    *
    * @param dt: The data type of the input
    * @param value: the value of the input
    * @return the byte array with the first byte indicating the data type.
    */
  override def encode(dt: DataType,
                      value: Any): Array[Byte] = {
    dt match {
      case BooleanType =>
        val result = new Array[Byte](Bytes.SIZEOF_BOOLEAN + 1)
        result(0) = BooleanEnc
        value.asInstanceOf[Boolean] match {
          case true => result(1) = -1: Byte
          case false => result(1) = 0: Byte
        }
        result
      case ShortType =>
        val result = new Array[Byte](Bytes.SIZEOF_SHORT + 1)
        result(0) = ShortEnc
        Bytes.putShort(result, 1, value.asInstanceOf[Short])
        result
      case IntegerType =>
        val result = new Array[Byte](Bytes.SIZEOF_INT + 1)
        result(0) = IntEnc
        Bytes.putInt(result, 1, value.asInstanceOf[Int])
        result
      case LongType|TimestampType =>
        val result = new Array[Byte](Bytes.SIZEOF_LONG + 1)
        result(0) = LongEnc
        Bytes.putLong(result, 1, value.asInstanceOf[Long])
        result
      case FloatType =>
        val result = new Array[Byte](Bytes.SIZEOF_FLOAT + 1)
        result(0) = FloatEnc
        Bytes.putFloat(result, 1, value.asInstanceOf[Float])
        result
      case DoubleType =>
        val result = new Array[Byte](Bytes.SIZEOF_DOUBLE + 1)
        result(0) = DoubleEnc
        Bytes.putDouble(result, 1, value.asInstanceOf[Double])
        result
      case BinaryType =>
        val v = value.asInstanceOf[Array[Bytes]]
        val result = new Array[Byte](v.length + 1)
        result(0) = BinaryEnc
        System.arraycopy(v, 0, result, 1, v.length)
        result
      case StringType =>
        val bytes = Bytes.toBytes(value.asInstanceOf[String])
        val result = new Array[Byte](bytes.length + 1)
        result(0) = StringEnc
        System.arraycopy(bytes, 0, result, 1, bytes.length)
        result
      case _ =>
        val bytes = Bytes.toBytes(value.toString)
        val result = new Array[Byte](bytes.length + 1)
        result(0) = UnknownEnc
        System.arraycopy(bytes, 0, result, 1, bytes.length)
        result
    }
  }

  override def filter(input: Array[Byte], offset1: Int, length1: Int,
                      filterBytes: Array[Byte], offset2: Int, length2: Int,
                      ops: JavaBytesEncoder): Boolean = {
    filterBytes(offset2) match {
      case ShortEnc =>
        val in = Bytes.toShort(input, offset1)
        val value = Bytes.toShort(filterBytes, offset2 + 1)
        compare(in.compareTo(value), ops)
      case IntEnc =>
        val in = Bytes.toInt(input, offset1)
        val value = Bytes.toInt(filterBytes, offset2 + 1)
        compare(in.compareTo(value), ops)
      case LongEnc | TimestampEnc =>
        val in = Bytes.toInt(input, offset1)
        val value = Bytes.toInt(filterBytes, offset2 + 1)
        compare(in.compareTo(value), ops)
      case FloatEnc =>
        val in = Bytes.toFloat(input, offset1)
        val value = Bytes.toFloat(filterBytes, offset2 + 1)
        compare(in.compareTo(value), ops)
      case DoubleEnc =>
        val in = Bytes.toDouble(input, offset1)
        val value = Bytes.toDouble(filterBytes, offset2 + 1)
        compare(in.compareTo(value), ops)
      case _ =>
        // for String, Byte, Binary, Boolean and other types
        // we can use the order of byte array directly.
        compare(
          Bytes.compareTo(input, offset1, length1, filterBytes, offset2 + 1, length2 - 1), ops)
    }
  }
}
