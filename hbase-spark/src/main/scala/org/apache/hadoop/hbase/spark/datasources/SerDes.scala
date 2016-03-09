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

import java.io.ByteArrayInputStream

import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io._
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types._

trait SerDes {
  def serialize(value: Any): Array[Byte]
  def deserialize(bytes: Array[Byte], start: Int, end: Int): Any
}

class DoubleSerDes extends SerDes {
  override def serialize(value: Any): Array[Byte] = Bytes.toBytes(value.asInstanceOf[Double])
  override def deserialize(bytes: Array[Byte], start: Int, end: Int): Any = {
    Bytes.toDouble(bytes, start)
  }
}


