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

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.idx.exp.Comparison;
import org.apache.hadoop.hbase.client.idx.exp.Expression;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import java.io.IOException;

/**
 * Tests the expression class.
 */
public class TestComparison extends TestCase {
  /**
   * Tests the constuctor.
   */
  public void testConstructor() {
    byte[] columnName1 = Bytes.toBytes("columnName1");
    byte[] qualifer1 = Bytes.toBytes("qualifier1");
    byte[] value1 = Bytes.toBytes("value1");
    Comparison.Operator operator1 = Comparison.Operator.EQ;

    Comparison comparison = new Comparison(columnName1, qualifer1, operator1, value1);

    Assert.assertEquals("columnName was incorrect", columnName1, comparison.getColumnName());
    Assert.assertEquals("qualifier was incorrect", qualifer1, comparison.getQualifier());
    Assert.assertEquals("value was incorrect", value1, comparison.getValue());
    Assert.assertEquals("operator was incorrect", operator1, comparison.getOperator());
  }

  /**
   * Tests that the equals method works.
   */
  public void testEquals() {
    Expression expression1 = Expression.comparison("columnName", "qualifier", Comparison.Operator.EQ, Bytes.toBytes("value"));
    Expression expression2 = Expression.comparison("columnName", "qualifier", Comparison.Operator.EQ, Bytes.toBytes("value"));

    Assert.assertTrue("equals didn't work as expected", expression1.equals(expression2));
  }

  /**
   * Tests that the equals method works.
   */
  public void testEqualsFalse() {
    Expression expression1 = Expression.comparison("columnName", "qualifier", Comparison.Operator.EQ, Bytes.toBytes("value"));
    Expression expression2 = Expression.comparison("columnName", "qualifier", Comparison.Operator.EQ, Bytes.toBytes("othervalue"));

    Assert.assertFalse("equals didn't work as expected", expression1.equals(expression2));
  }

  /**
   * Tests the an comparison can be written and read and still be equal.
   *
   * @throws java.io.IOException if an io error occurs
   */
  public void testWritable() throws IOException {
    Expression expression = Expression.comparison("columnName1", "qualifier1", Comparison.Operator.EQ, Bytes.toBytes("value"));

    DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
    expression.write(dataOutputBuffer);

    DataInputBuffer dataInputBuffer = new DataInputBuffer();
    dataInputBuffer.reset(dataOutputBuffer.getData(), dataOutputBuffer.getLength());

    Expression clonedExpression = new Comparison();
    clonedExpression.readFields(dataInputBuffer);

    Assert.assertEquals("The expression was not the same after being written and read", expression, clonedExpression);
  }
}