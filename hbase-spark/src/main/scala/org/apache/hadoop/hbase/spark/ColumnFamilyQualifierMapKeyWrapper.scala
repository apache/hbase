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

import org.apache.hadoop.hbase.util.Bytes

/**
 * A wrapper class that will allow both columnFamily and qualifier to
 * be the key of a hashMap.  Also allow for finding the value in a hashmap
 * with out cloning the HBase value from the HBase Cell object
 * @param columnFamily       ColumnFamily byte array
 * @param columnFamilyOffSet Offset of columnFamily value in the array
 * @param columnFamilyLength Length of the columnFamily value in the columnFamily array
 * @param qualifier          Qualifier byte array
 * @param qualifierOffSet    Offset of qualifier value in the array
 * @param qualifierLength    Length of the qualifier value with in the array
 */
class ColumnFamilyQualifierMapKeyWrapper(val columnFamily:Array[Byte],
                                         val columnFamilyOffSet:Int,
                                         val columnFamilyLength:Int,
                                         val qualifier:Array[Byte],
                                         val qualifierOffSet:Int,
                                         val qualifierLength:Int)
  extends Serializable{

  override def equals(other:Any): Boolean = {
    val otherWrapper = other.asInstanceOf[ColumnFamilyQualifierMapKeyWrapper]

    Bytes.compareTo(columnFamily,
      columnFamilyOffSet,
      columnFamilyLength,
      otherWrapper.columnFamily,
      otherWrapper.columnFamilyOffSet,
      otherWrapper.columnFamilyLength) == 0 && Bytes.compareTo(qualifier,
        qualifierOffSet,
        qualifierLength,
        otherWrapper.qualifier,
        otherWrapper.qualifierOffSet,
        otherWrapper.qualifierLength) == 0
  }

  override def hashCode():Int = {
    Bytes.hashCode(columnFamily, columnFamilyOffSet, columnFamilyLength) +
      Bytes.hashCode(qualifier, qualifierOffSet, qualifierLength)
  }

  def cloneColumnFamily():Array[Byte] = {
    val resultArray = new Array[Byte](columnFamilyLength)
    System.arraycopy(columnFamily, columnFamilyOffSet, resultArray, 0, columnFamilyLength)
    resultArray
  }

  def cloneQualifier():Array[Byte] = {
    val resultArray = new Array[Byte](qualifierLength)
    System.arraycopy(qualifier, qualifierOffSet, resultArray, 0, qualifierLength)
    resultArray
  }
}
