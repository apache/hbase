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
package org.apache.hadoop.hbase;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.client.idx.exp.Comparison;
import org.apache.hadoop.hbase.client.idx.exp.Expression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import java.io.IOException;

/**
 * Tests the {@link org.apache.hadoop.hbase.WritableHelper}.
 */
public class TestWritableHelper extends TestCase {
  /**
   * Tests the {@link org.apache.hadoop.hbase.WritableHelper#instanceForName(String,
   * Class)} works as expected.
   */
  public void testInstanceForName() {
    Expression expression = WritableHelper.instanceForName(Comparison.class.getName(), Expression.class);

    Assert.assertNotNull("Instance should not be null", expression);
    Assert.assertEquals("Wrong class returned", Comparison.class, expression.getClass());
  }

  /**
   * Tests the {@link org.apache.hadoop.hbase.WritableHelper#instanceForName(String,
   * Class)} works as expected when an invalid class name is provided.
   */
  public void testInstanceForNameInvalidClassName() {
    try {
      WritableHelper.instanceForName(Comparison.class.getName() + "1", Expression.class);
      Assert.fail("An exception should have been thrown");
    } catch (Exception e) {
      Assert.assertEquals("Wrong exception was thrown", IllegalArgumentException.class, e.getClass());
    }
  }

  /**
   * Tests the that {@link org.apache.hadoop.hbase.WritableHelper#writeInstance(java.io.DataOutput,
   * org.apache.hadoop.io.Writable)} fails as expected when null is provided.
   * @throws IOException if an error occurs
   */
  public void testWriteInstanceFailsWithNull() throws IOException {
    DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
    try {
      WritableHelper.writeInstance(dataOutputBuffer, null);
      Assert.fail("Expected an exception");
    } catch (Exception e) {
      Assert.assertEquals("Wrong exception thrown when null was provided", IllegalArgumentException.class, e.getClass());
    }
  }

  /**
   * Tests the that {@link org.apache.hadoop.hbase.WritableHelper#writeInstance(java.io.DataOutput,
   * org.apache.hadoop.io.Writable)} and {@link org.apache.hadoop.hbase.WritableHelper#readInstance(java.io.DataInput,
   * Class)} works as expected.
   * @throws IOException if an error occurs
   */
  public void testWriteReadInstance() throws IOException {
    Expression expression = Expression.comparison("columnName1", "qualifier1", Comparison.Operator.EQ, Bytes.toBytes("value"));

    DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
    WritableHelper.writeInstance(dataOutputBuffer, expression);

    DataInputBuffer dataInputBuffer = new DataInputBuffer();
    dataInputBuffer.reset(dataOutputBuffer.getData(), dataOutputBuffer.getLength());

    Expression clonedExpression = WritableHelper.readInstance(dataInputBuffer, Expression.class);

    Assert.assertEquals("The expression was not the same after being written and read", expression, clonedExpression);
  }

  /**
   * Tests the that {@link org.apache.hadoop.hbase.WritableHelper#writeInstanceNullable(java.io.DataOutput,
   * org.apache.hadoop.io.Writable)} and {@link org.apache.hadoop.hbase.WritableHelper#readInstanceNullable(java.io.DataInput,
   * Class)} works as expected.
   * @throws IOException if an error occurs
   */
  public void testWriteReadInstanceNullable() throws IOException {
    Expression expression = Expression.comparison("columnName1", "qualifier1", Comparison.Operator.EQ, Bytes.toBytes("value"));

    DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
    WritableHelper.writeInstanceNullable(dataOutputBuffer, expression);

    DataInputBuffer dataInputBuffer = new DataInputBuffer();
    dataInputBuffer.reset(dataOutputBuffer.getData(), dataOutputBuffer.getLength());

    Expression clonedExpression = WritableHelper.readInstanceNullable(dataInputBuffer, Expression.class);

    Assert.assertEquals("The expression was not the same after being written and read", expression, clonedExpression);
  }

  /**
   * Tests the that {@link org.apache.hadoop.hbase.WritableHelper#writeInstanceNullable(java.io.DataOutput,
   * org.apache.hadoop.io.Writable)} and {@link org.apache.hadoop.hbase.WritableHelper#readInstanceNullable(java.io.DataInput,
   * Class)} works as expected when null is provided.
   * @throws IOException if an error occurs
   */
  public void testWriteReadInstanceNullableWithNull() throws IOException {
    DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
    WritableHelper.writeInstanceNullable(dataOutputBuffer, null);

    DataInputBuffer dataInputBuffer = new DataInputBuffer();
    dataInputBuffer.reset(dataOutputBuffer.getData(), dataOutputBuffer.getLength());

    Expression clonedExpression = WritableHelper.readInstanceNullable(dataInputBuffer, Expression.class);

    Assert.assertNull("A null value was expected", clonedExpression);
  }
}
