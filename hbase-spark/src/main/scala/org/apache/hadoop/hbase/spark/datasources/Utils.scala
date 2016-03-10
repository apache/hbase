
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

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.datasources.hbase.Field
import org.apache.spark.unsafe.types.UTF8String

object Utils {
  // convert input to data type
  def toBytes(input: Any, field: Field): Array[Byte] = {
    input match {
      case data: Boolean => Bytes.toBytes(data)
      case data: Byte => Array(data)
      case data: Array[Byte] => data
      case data: Double => Bytes.toBytes(data)
      case data: Float => Bytes.toBytes(data)
      case data: Int => Bytes.toBytes(data)
      case data: Long => Bytes.toBytes(data)
      case data: Short => Bytes.toBytes(data)
      case data: UTF8String => data.getBytes
      case data: String => Bytes.toBytes(data)
      // TODO: add more data type support
      case _ => throw new Exception(s"unsupported data type ${field.dt}")
    }
  }
}
