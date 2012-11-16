/*
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

package org.apache.hbase.cell;

import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hbase.Cell;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class CellTool {

  /******************* ByteRange *******************************/

  public static ByteRange fillRowRange(Cell cell, ByteRange range) {
    return range.set(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
  }

  public static ByteRange fillFamilyRange(Cell cell, ByteRange range) {
    return range.set(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
  }

  public static ByteRange fillQualifierRange(Cell cell, ByteRange range) {
    return range.set(cell.getQualifierArray(), cell.getQualifierOffset(),
      cell.getQualifierLength());
  }


  /***************** get individual arrays for tests ************/

  public static byte[] getRowArray(Cell cell){
    byte[] output = new byte[cell.getRowLength()];
    copyRowTo(cell, output, 0);
    return output;
  }

  public static byte[] getFamilyArray(Cell cell){
    byte[] output = new byte[cell.getFamilyLength()];
    copyFamilyTo(cell, output, 0);
    return output;
  }

  public static byte[] getQualifierArray(Cell cell){
    byte[] output = new byte[cell.getQualifierLength()];
    copyQualifierTo(cell, output, 0);
    return output;
  }

  public static byte[] getValueArray(Cell cell){
    byte[] output = new byte[cell.getValueLength()];
    copyValueTo(cell, output, 0);
    return output;
  }


  /******************** copyTo **********************************/

  public static int copyRowTo(Cell cell, byte[] destination, int destinationOffset) {
    System.arraycopy(cell.getRowArray(), cell.getRowOffset(), destination, destinationOffset,
      cell.getRowLength());
    return destinationOffset + cell.getRowLength();
  }

  public static int copyFamilyTo(Cell cell, byte[] destination, int destinationOffset) {
    System.arraycopy(cell.getFamilyArray(), cell.getFamilyOffset(), destination, destinationOffset,
      cell.getFamilyLength());
    return destinationOffset + cell.getFamilyLength();
  }

  public static int copyQualifierTo(Cell cell, byte[] destination, int destinationOffset) {
    System.arraycopy(cell.getQualifierArray(), cell.getQualifierOffset(), destination,
      destinationOffset, cell.getQualifierLength());
    return destinationOffset + cell.getQualifierLength();
  }

  public static int copyValueTo(Cell cell, byte[] destination, int destinationOffset) {
    System.arraycopy(cell.getValueArray(), cell.getValueOffset(), destination, destinationOffset,
      cell.getValueLength());
    return destinationOffset + cell.getValueLength();
  }


  /********************* misc *************************************/

  public static byte getRowByte(Cell cell, int index) {
    return cell.getRowArray()[cell.getRowOffset() + index];
  }


  /********************** KeyValue (move to KeyValueUtils) *********************/

  public static ByteBuffer getValueBufferShallowCopy(Cell cell) {
    ByteBuffer buffer = ByteBuffer.wrap(cell.getValueArray(), cell.getValueOffset(),
      cell.getValueLength());
//    buffer.position(buffer.limit());//make it look as if value was appended
    return buffer;
  }

}
