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
package org.apache.hadoop.hbase.client.idx;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.client.idx.exp.Comparison;
import org.apache.hadoop.hbase.client.idx.exp.Expression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import java.io.IOException;

/**
 * Tests the {@link IdxScan} class.
 */
public class TestIdxScan extends TestCase {

  /**
   * Tests that the writable and readFields methods work as expected.
   *
   * @throws java.io.IOException if an IO error occurs
   */
  public void testWritable() throws IOException {
    Expression expression = Expression.comparison("columnName", "qualifier", Comparison.Operator.EQ, Bytes.toBytes("value"));

    IdxScan idxScan = new IdxScan(expression);
    DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
    idxScan.write(dataOutputBuffer);

    DataInputBuffer dataInputBuffer = new DataInputBuffer();
    dataInputBuffer.reset(dataOutputBuffer.getData(), dataOutputBuffer.getLength());

    IdxScan clonedScan = new IdxScan();
    clonedScan.readFields(dataInputBuffer);

    Assert.assertEquals("The expression was not the same after being written and read", idxScan.getExpression(), clonedScan.getExpression());
  }

  /**
   * Tests that the writable and readFields methods work as expected.
   *
   * @throws java.io.IOException if an IO error occurs
   */
  public void testWritableNullExpression() throws IOException {
    IdxScan idxScan = new IdxScan();
    DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
    idxScan.write(dataOutputBuffer);

    DataInputBuffer dataInputBuffer = new DataInputBuffer();
    dataInputBuffer.reset(dataOutputBuffer.getData(), dataOutputBuffer.getLength());

    IdxScan clonedScan = new IdxScan();
    clonedScan.readFields(dataInputBuffer);

    Assert.assertEquals("The expression was not the same after being written and read", idxScan.getExpression(), clonedScan.getExpression());
  }
}
