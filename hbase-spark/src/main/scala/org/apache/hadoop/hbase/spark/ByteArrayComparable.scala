/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.spark

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes

@InterfaceAudience.Public
class ByteArrayComparable(val bytes:Array[Byte], val offset:Int = 0, var length:Int = -1)
  extends Comparable[ByteArrayComparable] {

  if (length == -1) {
    length = bytes.length
  }

  override def compareTo(o: ByteArrayComparable): Int = {
    Bytes.compareTo(bytes, offset, length, o.bytes, o.offset, o.length)
  }

  override def hashCode(): Int = {
    Bytes.hashCode(bytes, offset, length)
  }

  override def equals (obj: Any): Boolean = {
    obj match {
      case b: ByteArrayComparable =>
        Bytes.equals(bytes, offset, length, b.bytes, b.offset, b.length)
      case _ =>
        false
    }
  }
}
