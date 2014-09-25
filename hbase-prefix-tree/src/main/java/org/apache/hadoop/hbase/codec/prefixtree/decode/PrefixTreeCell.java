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

package org.apache.hadoop.hbase.codec.prefixtree.decode;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;

/**
 * As the PrefixTreeArrayScanner moves through the tree bytes, it changes the values in the fields
 * of this class so that Cell logic can be applied, but without allocating new memory for every Cell
 * iterated through.
 */
@InterfaceAudience.Private
public class PrefixTreeCell implements Cell, Comparable<Cell> {

  /********************** static **********************/

  public static final KeyValue.Type[] TYPES = new KeyValue.Type[256];
  static {
    for (KeyValue.Type type : KeyValue.Type.values()) {
      TYPES[type.getCode() & 0xff] = type;
    }
  }

  //Same as KeyValue constructor.  Only used to avoid NPE's when full cell hasn't been initialized.
  public static final KeyValue.Type DEFAULT_TYPE = KeyValue.Type.Put;

  /******************** fields ************************/

  protected byte[] block;
  //we could also avoid setting the mvccVersion in the scanner/searcher, but this is simpler
  protected boolean includeMvccVersion;

  protected byte[] rowBuffer;
  protected int rowLength;

  protected byte[] familyBuffer;
  protected int familyOffset;
  protected int familyLength;

  protected byte[] qualifierBuffer;// aligned to the end of the array
  protected int qualifierOffset;
  protected int qualifierLength;

  protected Long timestamp;
  protected Long mvccVersion;

  protected KeyValue.Type type;

  protected int absoluteValueOffset;
  protected int valueLength;

  protected byte[] tagsBuffer;
  protected int tagsOffset;
  protected int tagsLength;

  /********************** Cell methods ******************/

  /**
   * For debugging.  Currently creates new KeyValue to utilize its toString() method.
   */
  @Override
  public String toString() {
    return getKeyValueString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Cell)) {
      return false;
    }
    //Temporary hack to maintain backwards compatibility with KeyValue.equals
    return CellComparator.equalsIgnoreMvccVersion(this, (Cell)obj);

    //TODO return CellComparator.equals(this, (Cell)obj);//see HBASE-6907
  }

  @Override
  public int hashCode(){
    //Temporary hack to maintain backwards compatibility with KeyValue.hashCode
    //I don't think this is used in any hot code paths
    return KeyValueUtil.copyToNewKeyValue(this).hashCode();

    //TODO return CellComparator.hashCode(this);//see HBASE-6907
  }

  @Override
  public int compareTo(Cell other) {
    return CellComparator.compareStatic(this, other);
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public long getMvccVersion() {
    if (!includeMvccVersion) {
      return 0L;
    }
    return mvccVersion;
  }

  @Override
  public int getValueLength() {
    return valueLength;
  }

  @Override
  public byte[] getRowArray() {
    return rowBuffer;
  }

  @Override
  public int getRowOffset() {
    return 0;
  }

  @Override
  public short getRowLength() {
    return (short) rowLength;
  }

  @Override
  public byte[] getFamilyArray() {
    return familyBuffer;
  }

  @Override
  public int getFamilyOffset() {
    return familyOffset;
  }

  @Override
  public byte getFamilyLength() {
    return (byte) familyLength;
  }

  @Override
  public byte[] getQualifierArray() {
    return qualifierBuffer;
  }

  @Override
  public int getQualifierOffset() {
    return qualifierOffset;
  }

  @Override
  public int getQualifierLength() {
    return qualifierLength;
  }

  @Override
  public byte[] getValueArray() {
    return block;
  }

  @Override
  public int getValueOffset() {
    return absoluteValueOffset;
  }

  @Override
  public byte getTypeByte() {
    return type.getCode();
  }

  /* Deprecated methods pushed into the Cell interface */
  @Override
  public byte[] getValue() {
    return CellUtil.cloneValue(this);
  }

  @Override
  public byte[] getFamily() {
    return CellUtil.cloneFamily(this);
  }

  @Override
  public byte[] getQualifier() {
    return CellUtil.cloneQualifier(this);
  }

  @Override
  public byte[] getRow() {
    return CellUtil.cloneRow(this);
  }

  /************************* helper methods *************************/

  /**
   * Need this separate method so we can call it from subclasses' toString() methods
   */
  protected String getKeyValueString(){
    KeyValue kv = KeyValueUtil.copyToNewKeyValue(this);
    return kv.toString();
  }

  @Override
  public int getTagsOffset() {
    return tagsOffset;
  }

  @Override
  @Deprecated
  public int getTagsLengthUnsigned() {
    return tagsLength;
  }

  @Override
  @Deprecated
  public short getTagsLength() {
    return (short) tagsLength;
  }

  @Override
  public byte[] getTagsArray() {
    return this.tagsBuffer;
  }
}
