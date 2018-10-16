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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * This object is a clean way to store and sort all cells that will be bulk
 * loaded into a single row
 */
@InterfaceAudience.Public
class FamiliesQualifiersValues extends Serializable {
  //Tree maps are used because we need the results to
  // be sorted when we read them
  val familyMap = new util.TreeMap[ByteArrayWrapper,
    util.TreeMap[ByteArrayWrapper, Array[Byte]]]()

  //normally in a row there are more columns then
  //column families this wrapper is reused for column
  //family look ups
  val reusableWrapper = new ByteArrayWrapper(null)

  /**
   * Adds a new cell to an existing row
   * @param family    HBase column family
   * @param qualifier HBase column qualifier
   * @param value     HBase cell value
   */
  def += (family: Array[Byte], qualifier: Array[Byte], value: Array[Byte]): Unit = {

    reusableWrapper.value = family

    var qualifierValues = familyMap.get(reusableWrapper)

    if (qualifierValues == null) {
      qualifierValues = new util.TreeMap[ByteArrayWrapper, Array[Byte]]()
      familyMap.put(new ByteArrayWrapper(family), qualifierValues)
    }

    qualifierValues.put(new ByteArrayWrapper(qualifier), value)
  }

  /**
    * A wrapper for "+=" method above, can be used by Java
    * @param family    HBase column family
    * @param qualifier HBase column qualifier
    * @param value     HBase cell value
    */
  def add(family: Array[Byte], qualifier: Array[Byte], value: Array[Byte]): Unit = {
    this += (family, qualifier, value)
  }
}
