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
import org.apache.hadoop.hbase.client.idx.exp.And;
import org.apache.hadoop.hbase.client.idx.exp.Comparison;
import org.apache.hadoop.hbase.client.idx.exp.Expression;
import org.apache.hadoop.hbase.client.idx.exp.Or;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import java.io.IOException;

/**
 * Tests the expression class.
 */
public class TestExpression extends TestCase {
  /**
   * Tests that the methods to build an expression all result in equal instances
   * when provided the same input.
   */
  public void testExpressionBuilder() {
    String columnName1 = "columnName1";
    String qualifer1 = "qualifier1";
    byte[] value1 = Bytes.toBytes("value1");
    Comparison.Operator operator1 = Comparison.Operator.EQ;

    String columnName2 = "columnName2";
    String qualifer2 = "qualifier2";
    byte[] value2 = Bytes.toBytes("value2");
    Comparison.Operator operator2 = Comparison.Operator.GT;

    String columnName3 = "columnName3";
    String qualifer3 = "qualifier3";
    byte[] value3 = Bytes.toBytes("value3");
    Comparison.Operator operator3 = Comparison.Operator.LT;

    Expression expression1 = new Or(
        new Comparison(columnName1, qualifer1, operator1, value1),
        new And(
            new Comparison(columnName2, qualifer2, operator2, value2),
            new Comparison(columnName3, qualifer3, operator3, value3)
        )
    );

    Expression expression2 = Expression
        .or(
            Expression.comparison(columnName1, qualifer1, operator1, value1)
        )
        .or(
            Expression.and()
                .and(Expression.comparison(columnName2, qualifer2, operator2, value2))
                .and(Expression.comparison(columnName3, qualifer3, operator3, value3))
        );

    Expression expression3 = Expression.or(
        Expression.comparison(columnName1, qualifer1, operator1, value1),
        Expression.and(
            Expression.comparison(columnName2, qualifer2, operator2, value2),
            Expression.comparison(columnName3, qualifer3, operator3, value3)
        )
    );

    Assert.assertTrue("The expressions didn't match", expression1.equals(expression2) && expression1.equals(expression3));
  }

  /**
   * Tests the an expression tree can be written and read and still be equal.
   *
   * @throws java.io.IOException if an io error occurs
   */
  public void testWritable() throws IOException {
    Expression expression = Expression.or(
        Expression.comparison("columnName1", "qualifier1", Comparison.Operator.EQ, Bytes.toBytes("value")),
        Expression.and(
            Expression.comparison("columnName2", "qualifier2", Comparison.Operator.GT, Bytes.toBytes("value2")),
            Expression.comparison("columnName3", "qualifier3", Comparison.Operator.LT, Bytes.toBytes("value3"))
        )
    );

    DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
    expression.write(dataOutputBuffer);

    DataInputBuffer dataInputBuffer = new DataInputBuffer();
    dataInputBuffer.reset(dataOutputBuffer.getData(), dataOutputBuffer.getLength());

    Expression clonedExpression = new Or();
    clonedExpression.readFields(dataInputBuffer);

    Assert.assertEquals("The expression was not the same after being written and read", expression, clonedExpression);
  }
}
