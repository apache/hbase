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
package org.apache.hadoop.hbase.regionserver;

import junit.framework.TestCase;
import junit.framework.Assert;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSet;
//import org.apache.hadoop.hbase.regionserver.idx.support.sets.BitSet;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSetBuilder;
import org.apache.hadoop.hbase.client.idx.exp.Expression;
import org.apache.hadoop.hbase.client.idx.exp.Comparison;
import org.easymock.EasyMock;

import java.util.Map;
import java.util.HashMap;

/**
 * Tests the {@link IdxExpressionEvaluator} class.
 */
public class TestIdxExpressionEvaluator extends TestCase {
  private TestSearchContext searchContext;

  @Override
  protected void setUp() throws Exception {
    searchContext = new TestSearchContext();
  }

  /**
   * Test a simple comparison expression interacts with the indices correctly.
   */
  public void testEvaluationSimpleComparisonExpression() {
    // set up the indices
    byte[] column = Bytes.toBytes("column");
    byte[] qualifier = Bytes.toBytes("qualifier");
    byte[] value = Bytes.toBytes("value");
    IdxIndex index = EasyMock.createMock(IdxIndex.class);
    EasyMock.expect(index.lookup(value)).andReturn(IntSetBuilder.newEmptyIntSet(1));
    EasyMock.expect(index.probeToString(value)).andReturn(Bytes.toString(value)).anyTimes();
    EasyMock.replay(index);
    searchContext.indices.put(Pair.of(column, qualifier), index);

    // perform the test
    IdxExpressionEvaluator evaluator = new IdxExpressionEvaluator();
    Expression exp = Expression.comparison(column, qualifier, Comparison.Operator.EQ, value);
    IntSet intSet = evaluator.evaluate(searchContext, exp);

    // assert the evaluator interacted with the indices correctly
    Assert.assertNotNull("The response from the evaluator should not be null", intSet);
    EasyMock.verify(index);
  }

  /**
   * Tests an Or expression containing two comparisons.
   */
  public void testEvaluationOrExpression() {
    // set up the indices
    byte[] column1 = Bytes.toBytes("column1");
    byte[] qualifier1 = Bytes.toBytes("qualifier1");
    byte[] value1 = Bytes.toBytes("value1");
    IdxIndex index1 = EasyMock.createMock(IdxIndex.class);
    IntSet bitSet1 = new IntSetBuilder().start().addAll(1,2,3,4,5).finish(100);
    EasyMock.expect(index1.head(value1, false)).andReturn(bitSet1);
    EasyMock.expect(index1.probeToString(value1)).andReturn(Bytes.toString(value1)).anyTimes();
    EasyMock.replay(index1);
    searchContext.indices.put(Pair.of(column1, qualifier1), index1);

    IdxIndex index2 = EasyMock.createMock(IdxIndex.class);
    byte[] column2 = Bytes.toBytes("column2");
    byte[] qualifier2 = Bytes.toBytes("qualifier2");
    byte[] value2 = Bytes.toBytes("value2");
    IntSet bitSet2 = new IntSetBuilder().start().addAll(6, 7, 8, 9, 10).finish(100);
    EasyMock.expect(index2.tail(value2, false)).andReturn(bitSet2);
    EasyMock.expect(index2.probeToString(value2)).andReturn(Bytes.toString(value2)).anyTimes();
    EasyMock.replay(index2);
    searchContext.indices.put(Pair.of(column2, qualifier2), index2);

    // perform the test
    IdxExpressionEvaluator evaluator = new IdxExpressionEvaluator();
    Expression exp = Expression.or(
        Expression.comparison(column1, qualifier1, Comparison.Operator.LT, value1),
        Expression.comparison(column2, qualifier2, Comparison.Operator.GT, value2)
    );
    IntSet intSet = evaluator.evaluate(searchContext, exp);

    // assert the evaluator interacted with the indices correctly
    Assert.assertNotNull("The response from the evaluator should not be null", intSet);
    for (int i = 1; i <= 10; i++) {
      Assert.assertTrue("The resulting IntSet should contain " + i, intSet.contains(i));
    }
    EasyMock.verify(index1, index2);
  }

  /**
   * Tests an And expression containing two comparisons.
   */
  public void testEvaluationAndExpression() {
    // set up the indices
    byte[] column1 = Bytes.toBytes("column1");
    byte[] qualifier1 = Bytes.toBytes("qualifier1");
    byte[] value1 = Bytes.toBytes("value1");
    IdxIndex index1 = EasyMock.createMock(IdxIndex.class);
    IntSet bitSet1 = new IntSetBuilder().start().addAll(1, 2, 3, 4, 5, 6).finish(100);
    EasyMock.expect(index1.head(value1, true)).andReturn(bitSet1);
    EasyMock.expect(index1.probeToString(value1)).andReturn(Bytes.toString(value1)).anyTimes();
    EasyMock.replay(index1);
    searchContext.indices.put(Pair.of(column1, qualifier1), index1);

    IdxIndex index2 = EasyMock.createMock(IdxIndex.class);
    byte[] column2 = Bytes.toBytes("column2");
    byte[] qualifier2 = Bytes.toBytes("qualifier2");
    byte[] value2 = Bytes.toBytes("value2");
    IntSet bitSet2 = new IntSetBuilder().start().addAll(5, 6, 7, 8, 9, 10).finish(100);
    EasyMock.expect(index2.tail(value2, true)).andReturn(bitSet2);
    EasyMock.expect(index2.probeToString(value2)).andReturn(Bytes.toString(value2)).anyTimes();
    EasyMock.replay(index2);
    searchContext.indices.put(Pair.of(column2, qualifier2), index2);

    // perform the test
    IdxExpressionEvaluator evaluator = new IdxExpressionEvaluator();
    Expression exp = Expression.and(
        Expression.comparison(column1, qualifier1, Comparison.Operator.LTE, value1),
        Expression.comparison(column2, qualifier2, Comparison.Operator.GTE, value2)
    );
    IntSet intSet = evaluator.evaluate(searchContext, exp);

    // assert the evaluator interacted with the indices correctly
    Assert.assertNotNull("The response from the evaluator should not be null", intSet);
    Assert.assertEquals("The resulting IntSet contained the wrong number of results", 2, intSet.size());
    for (int i : new int[]{5, 6}) {
      Assert.assertTrue("The resulting IntSet should contain " + i, intSet.contains(i));
    }
    EasyMock.verify(index1, index2);
  }

  /**
   * Tests a more complex combination of expressions.
   */
  public void testEvaluationComplexExpression() {
    // set up the indices
    IdxIndex index1 = EasyMock.createMock(IdxIndex.class);
    byte[] column1 = Bytes.toBytes("column1");
    byte[] qualifier1 = Bytes.toBytes("qualifier1");
    byte[] value1 = Bytes.toBytes("value1");
    IntSet bitSet1 = new IntSetBuilder().start().addAll(1,2,3,4,5,6).finish(100);
    EasyMock.expect(index1.head(value1, true)).andReturn(bitSet1);
    EasyMock.expect(index1.probeToString(value1)).andReturn(Bytes.toString(value1)).anyTimes();
    EasyMock.replay(index1);
    searchContext.indices.put(Pair.of(column1, qualifier1), index1);

    IdxIndex index2 = EasyMock.createMock(IdxIndex.class);
    byte[] column2 = Bytes.toBytes("column2");
    byte[] qualifier2 = Bytes.toBytes("qualifier2");
    byte[] value2 = Bytes.toBytes("value2");
    IntSet bitSet2 = new IntSetBuilder().start().addAll(5, 6, 7, 8, 9, 10).finish(100);
    EasyMock.expect(index2.tail(value2, true)).andReturn(bitSet2);
    EasyMock.expect(index2.probeToString(value2)).andReturn(Bytes.toString(value2)).anyTimes();
    EasyMock.replay(index2);
    searchContext.indices.put(Pair.of(column2, qualifier2), index2);

    IdxIndex index3 = EasyMock.createMock(IdxIndex.class);
    byte[] column3 = Bytes.toBytes("column3");
    byte[] qualifier3 = Bytes.toBytes("qualifier3");
    byte[] value3 = Bytes.toBytes("value3");
    IntSet bitSet3 = new IntSetBuilder().start().addAll(11).finish(100);
    EasyMock.expect(index3.lookup(value3)).andReturn(bitSet3);
    EasyMock.expect(index3.probeToString(value3)).andReturn(Bytes.toString(value3)).anyTimes();
    EasyMock.replay(index3);
    searchContext.indices.put(Pair.of(column3, qualifier3), index3);

    // perform the test
    IdxExpressionEvaluator evaluator = new IdxExpressionEvaluator();
    Expression exp = Expression.or(
        Expression.and(
            Expression.comparison(column1, qualifier1, Comparison.Operator.LTE, value1),
            Expression.comparison(column2, qualifier2, Comparison.Operator.GTE, value2)
        ),
        Expression.comparison(column3, qualifier3, Comparison.Operator.EQ, value3)
    );

    IntSet intSet = evaluator.evaluate(searchContext, exp);

    // assert the evaluator interacted with the indices correctly
    Assert.assertNotNull("The response from the evaluator should not be null", intSet);
    Assert.assertEquals("The resulting IntSet contained the wrong number of results", 3, intSet.size());
    for (int i : new int[]{5, 6, 11}) {
      Assert.assertTrue("The resulting IntSet should contain " + i, intSet.contains(i));
    }
    EasyMock.verify(index1, index2, index3);
  }

  /**
   * Test that ensures that a null expression is handled correctly.
   */
  public void testNullExpression() {
    IdxExpressionEvaluator evaluator = new IdxExpressionEvaluator();
    IntSet result = evaluator.evaluate(searchContext, (Expression) null);

    Assert.assertNull("The result should be null", result);
  }

  /**
   * Test that ensures that if an expression contains a reference to a column/qualifier
   * that doesn't exists the correct error is thrown.
   */
  public void testNullIndex() {
    Expression exp = Expression.comparison("column", "qualifier", Comparison.Operator.EQ, Bytes.toBytes("value"));

    IdxExpressionEvaluator evaluator = new IdxExpressionEvaluator();
    IntSet result = null;
    try {
      result = evaluator.evaluate(searchContext, exp);
      Assert.fail("An exception should have been thrown");
    } catch (IllegalStateException e) {
      Assert.assertTrue("Exception did not contain the correct message", e.getMessage().contains("Could not find an index"));
    } catch (Exception e) {
      Assert.fail("Wrong exception thrown");
    }

    Assert.assertNull("The result should be null", result);
  }

  private class TestSearchContext extends IdxSearchContext {
    Map<Pair<byte[], byte[]>, IdxIndex> indices;

    public TestSearchContext() {
      super(null, null);
      indices = new HashMap<Pair<byte[],byte[]>, IdxIndex>();
    }

    @Override
    public IdxIndex getIndex(byte[] column, byte[] qualifier) {
      return indices.get(Pair.of(column, qualifier));
    }
  }
}
