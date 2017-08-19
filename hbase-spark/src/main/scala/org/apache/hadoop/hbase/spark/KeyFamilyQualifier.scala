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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes

/**
 * This is the key to be used for sorting and shuffling.
 *
 * We will only partition on the rowKey but we will sort on all three
 *
 * @param rowKey    Record RowKey
 * @param family    Record ColumnFamily
 * @param qualifier Cell Qualifier
 */
@InterfaceAudience.Public
class KeyFamilyQualifier(val rowKey:Array[Byte], val family:Array[Byte], val qualifier:Array[Byte])
  extends Comparable[KeyFamilyQualifier] with Serializable {
  override def compareTo(o: KeyFamilyQualifier): Int = {
    var result = Bytes.compareTo(rowKey, o.rowKey)
    if (result == 0) {
      result = Bytes.compareTo(family, o.family)
      if (result == 0) result = Bytes.compareTo(qualifier, o.qualifier)
    }
    result
  }
  override def toString: String = {
    Bytes.toString(rowKey) + ":" + Bytes.toString(family) + ":" + Bytes.toString(qualifier)
  }
}
