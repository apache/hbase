/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client.idx;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * The description of an indexed column family qualifier.
 */
public class IdxIndexDescriptor implements Writable {
  /**
   * Qualifier name;
   */
  private byte[] qualifierName;

  /**
   * The qualifier type - affects the translation of bytes into indexed
   * properties. 
   */
  private IdxQualifierType qualifierType;

  /**
   * Empty constructor to support the writable interface - DO NOT USE.
   */
  public IdxIndexDescriptor() {
  }

  /**
   * Construct a new index descriptor.
   * @param qualifierName the qualifier name
   * @param qualifierType the qualifier type
   */
  public IdxIndexDescriptor(byte[] qualifierName,
    IdxQualifierType qualifierType) {
    this.qualifierName = qualifierName;
    this.qualifierType = qualifierType;
  }

  /**
   * The column family qualifier name.
   * @return column family qualifier name
   */
  public byte[] getQualifierName() {
    return qualifierName;
  }

  /**
   * The column family qualifier name.
   * @param qualifierName column family qualifier name
   */
  public void setQualifierName(byte[] qualifierName) {
    this.qualifierName = qualifierName;
  }

  /**
   * The data type that the column family qualifier contains.
   * @return data type that the column family qualifier contains
   */
  public IdxQualifierType getQualifierType() {
    return qualifierType;
  }

  /**
   * The data type that the column family qualifier contains.
   * @param qualifierType data type that the column family qualifier contains
   */
  public void setQualifierType(IdxQualifierType qualifierType) {
    this.qualifierType = qualifierType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Bytes.writeByteArray(dataOutput, qualifierName);
    WritableUtils.writeEnum(dataOutput, qualifierType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    qualifierName = Bytes.readByteArray(dataInput);
    qualifierType = WritableUtils.readEnum(dataInput, IdxQualifierType.class);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    IdxIndexDescriptor that = (IdxIndexDescriptor) o;

    if (!Arrays.equals(qualifierName, that.qualifierName)) return false;

    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return Arrays.hashCode(qualifierName);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    StringBuffer s = new StringBuffer();
    s.append('{');
    s.append("QUALIFIER");
    s.append(" => '");
    s.append(Bytes.toString(qualifierName));
    s.append("',");
    s.append("TYPE");
    s.append(" => '");
    s.append(qualifierType);
    s.append("'}");
    return s.toString();
  }
}
