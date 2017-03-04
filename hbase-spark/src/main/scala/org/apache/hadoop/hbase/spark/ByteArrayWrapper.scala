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

import java.io.Serializable

import org.apache.hadoop.hbase.util.Bytes

/**
 * This is a wrapper over a byte array so it can work as
 * a key in a hashMap
 *
 * @param value The Byte Array value
 */
class ByteArrayWrapper (var value:Array[Byte])
  extends Comparable[ByteArrayWrapper] with Serializable {
  override def compareTo(valueOther: ByteArrayWrapper): Int = {
    Bytes.compareTo(value,valueOther.value)
  }
  override def equals(o2: Any): Boolean = {
    o2 match {
      case wrapper: ByteArrayWrapper =>
        Bytes.equals(value, wrapper.value)
      case _ =>
        false
    }
  }
  override def hashCode():Int = {
    Bytes.hashCode(value)
  }
}
