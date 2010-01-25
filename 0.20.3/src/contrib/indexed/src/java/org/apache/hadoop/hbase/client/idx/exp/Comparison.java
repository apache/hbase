/*
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
package org.apache.hadoop.hbase.client.idx.exp;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * The comparison expression.
 */
public class Comparison extends Expression {
  private byte[] columnName;
  private byte[] qualifier;
  private Operator operator;
  private byte[] value;

  /**
   * No args constructor.
   */
  public Comparison() {
  }

  /**
   * Convenience constrcutor that takes strings and converts from to byte[].
   * @param columnName the column name
   * @param qualifier  the column qualifier
   * @param operator   the operator
   * @param value      the value
   */
  public Comparison(String columnName, String qualifier, Operator operator, byte[] value) {
    this(Bytes.toBytes(columnName), Bytes.toBytes(qualifier), operator, value);
  }

  /**
   * Full constructor with all required fields.
   * @param columnName the column name
   * @param qualifier  the column qualifier
   * @param operator   the operator
   * @param value      the value
   */
  public Comparison(byte[] columnName, byte[] qualifier, Operator operator,
                    byte[] value) {
    assert columnName != null : "The columnName must not be null";
    assert qualifier != null : "The qualifier must not be null";
    assert operator != null : "The operator must not be null";
    assert value != null : "The value must not be null";

    this.columnName = columnName;
    this.qualifier = qualifier;
    this.operator = operator;
    this.value = value;
  }

  /**
   * The {@link org.apache.hadoop.hbase.HColumnDescriptor#getName() column
   * family name} that the {@link #getQualifier() qualifier} is a member of.
   * @return the column family name
   */
  public byte[] getColumnName() {
    return columnName;
  }

  /**
   * The column qualifier.
   * @return the qualifier
   */
  public byte[] getQualifier() {
    return qualifier;
  }

  /**
   * The operator.
   * @return the operator
   */
  public Operator getOperator() {
    return operator;
  }

  /**
   * The value.
   * @return the value
   */
  public byte[] getValue() {
    return value;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Bytes.writeByteArray(dataOutput, columnName);
    Bytes.writeByteArray(dataOutput, qualifier);
    Bytes.writeByteArray(dataOutput, Bytes.toBytes(operator.toString()));
    Bytes.writeByteArray(dataOutput, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    columnName = Bytes.readByteArray(dataInput);
    qualifier = Bytes.readByteArray(dataInput);
    operator = Operator.valueOf(Bytes.toString(Bytes.readByteArray(dataInput)));
    value = Bytes.readByteArray(dataInput);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Comparison that = (Comparison) o;

    if (!Arrays.equals(columnName, that.columnName)) return false;
    if (operator != that.operator) return false;
    if (!Arrays.equals(qualifier, that.qualifier)) return false;
    if (!Arrays.equals(value, that.value)) return false;

    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    int result = Arrays.hashCode(columnName);
    result = 31 * result + Arrays.hashCode(qualifier);
    result = 31 * result + operator.hashCode();
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  /**
   * The enum for specifying the function we're performing in a {@link
   * Comparison}.
   */
  public enum Operator {
    /**
     * The equals function.
     */
    EQ,
    /**
     * The greater than function.
     */
    GT,
    /**
     * The greater than or equals function.
     */
    GTE,
    /**
     * The less than function.
     */
    LT,
    /**
     * The less than or equals function.
     */
    LTE
  }
}
