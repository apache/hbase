/**
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
package org.apache.hadoop.hbase.security.visibility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.security.visibility.expression.ExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.LeafExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.NonLeafExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.Operator;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestExpressionParser {

  private ExpressionParser parser = new ExpressionParser();

  @Test
  public void testPositiveCases() throws Exception {
    // abc -> (abc)
    ExpressionNode node = parser.parse("abc");
    assertTrue(node instanceof LeafExpressionNode);
    assertEquals("abc", ((LeafExpressionNode) node).getIdentifier());

    // a&b|c&d -> (((a & b) | c) & )
    node = parser.parse("a&b|c&d");
    assertTrue(node instanceof NonLeafExpressionNode);
    NonLeafExpressionNode nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("d", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    assertTrue(nlNode.getChildExps().get(0) instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    assertEquals(Operator.OR, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("c", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    assertTrue(nlNode.getChildExps().get(0) instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("b", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    assertEquals("a", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());

    // (a) -> (a)
    node = parser.parse("(a)");
    assertTrue(node instanceof LeafExpressionNode);
    assertEquals("a", ((LeafExpressionNode) node).getIdentifier());

    // (a&b) -> (a & b)
    node = parser.parse(" ( a & b )");
    assertTrue(node instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("a", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());
    assertEquals("b", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());

    // ((((a&b)))) -> (a & b)
    node = parser.parse("((((a&b))))");
    assertTrue(node instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("a", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());
    assertEquals("b", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());

    // (a|b)&(cc|def) -> ((a | b) & (cc | def))
    node = parser.parse("( a | b ) & (cc|def)");
    assertTrue(node instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertTrue(nlNode.getChildExps().get(0) instanceof NonLeafExpressionNode);
    assertTrue(nlNode.getChildExps().get(1) instanceof NonLeafExpressionNode);
    NonLeafExpressionNode nlNodeLeft = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    NonLeafExpressionNode nlNodeRight = (NonLeafExpressionNode) nlNode.getChildExps().get(1);
    assertEquals(Operator.OR, nlNodeLeft.getOperator());
    assertEquals(2, nlNodeLeft.getChildExps().size());
    assertEquals("a", ((LeafExpressionNode) nlNodeLeft.getChildExps().get(0)).getIdentifier());
    assertEquals("b", ((LeafExpressionNode) nlNodeLeft.getChildExps().get(1)).getIdentifier());
    assertEquals(Operator.OR, nlNodeRight.getOperator());
    assertEquals(2, nlNodeRight.getChildExps().size());
    assertEquals("cc", ((LeafExpressionNode) nlNodeRight.getChildExps().get(0)).getIdentifier());
    assertEquals("def", ((LeafExpressionNode) nlNodeRight.getChildExps().get(1)).getIdentifier());

    // a&(cc|de) -> (a & (cc | de))
    node = parser.parse("a&(cc|de)");
    assertTrue(node instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("a", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());
    assertTrue(nlNode.getChildExps().get(1) instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(1);
    assertEquals(Operator.OR, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("cc", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());
    assertEquals("de", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());

    // (a&b)|c -> ((a & b) | c)
    node = parser.parse("(a&b)|c");
    assertTrue(node instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.OR, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("c", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    assertTrue(nlNode.getChildExps().get(0) instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("a", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());
    assertEquals("b", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());

    // (a&b&c)|d -> (((a & b) & c) | d)
    node = parser.parse("(a&b&c)|d");
    assertTrue(node instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.OR, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("d", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    assertTrue(nlNode.getChildExps().get(0) instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("c", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    assertTrue(nlNode.getChildExps().get(0) instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("b", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    assertEquals("a", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());

    // a&(b|(c|d)) -> (a & (b | (c | d)))
    node = parser.parse("a&(b|(c|d))");
    assertTrue(node instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("a", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());
    assertTrue(nlNode.getChildExps().get(1) instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(1);
    assertEquals(Operator.OR, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("b", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());
    assertTrue(nlNode.getChildExps().get(1) instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(1);
    assertEquals(Operator.OR, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("c", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());
    assertEquals("d", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());

    // (!a) -> (!a)
    node = parser.parse("(!a)");
    assertTrue(node instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.NOT, nlNode.getOperator());
    assertEquals("a", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());

    // a&(!b) -> (a & (!b))
    node = parser.parse("a&(!b)");
    assertTrue(node instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("a", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());
    assertTrue(nlNode.getChildExps().get(1) instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(1);
    assertEquals(Operator.NOT, nlNode.getOperator());
    assertEquals(1, nlNode.getChildExps().size());
    assertEquals("b", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());

    // !a&b -> ((!a) & b)
    node = parser.parse("!a&b");
    assertTrue(node instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("b", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    assertTrue(nlNode.getChildExps().get(0) instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    assertEquals(Operator.NOT, nlNode.getOperator());
    assertEquals(1, nlNode.getChildExps().size());
    assertEquals("a", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());

    // !a&(!b) -> ((!a) & (!b))
    node = parser.parse("!a&(!b)");
    assertTrue(node instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertTrue(nlNode.getChildExps().get(0) instanceof NonLeafExpressionNode);
    assertTrue(nlNode.getChildExps().get(1) instanceof NonLeafExpressionNode);
    nlNodeLeft = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    nlNodeRight = (NonLeafExpressionNode) nlNode.getChildExps().get(1);
    assertEquals(Operator.NOT, nlNodeLeft.getOperator());
    assertEquals(1, nlNodeLeft.getChildExps().size());
    assertEquals("a", ((LeafExpressionNode) nlNodeLeft.getChildExps().get(0)).getIdentifier());
    assertEquals(Operator.NOT, nlNodeRight.getOperator());
    assertEquals(1, nlNodeRight.getChildExps().size());
    assertEquals("b", ((LeafExpressionNode) nlNodeRight.getChildExps().get(0)).getIdentifier());

    // !a&!b -> ((!a) & (!b))
    node = parser.parse("!a&!b");
    assertTrue(node instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertTrue(nlNode.getChildExps().get(0) instanceof NonLeafExpressionNode);
    assertTrue(nlNode.getChildExps().get(1) instanceof NonLeafExpressionNode);
    nlNodeLeft = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    nlNodeRight = (NonLeafExpressionNode) nlNode.getChildExps().get(1);
    assertEquals(Operator.NOT, nlNodeLeft.getOperator());
    assertEquals(1, nlNodeLeft.getChildExps().size());
    assertEquals("a", ((LeafExpressionNode) nlNodeLeft.getChildExps().get(0)).getIdentifier());
    assertEquals(Operator.NOT, nlNodeRight.getOperator());
    assertEquals(1, nlNodeRight.getChildExps().size());
    assertEquals("b", ((LeafExpressionNode) nlNodeRight.getChildExps().get(0)).getIdentifier());

    // !(a&b) -> (!(a & b))
    node = parser.parse("!(a&b)");
    assertTrue(node instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.NOT, nlNode.getOperator());
    assertEquals(1, nlNode.getChildExps().size());
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("a", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());
    assertEquals("b", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());

    // a&!b -> (a & (!b))
    node = parser.parse("a&!b");
    assertTrue(node instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("a", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());
    assertTrue(nlNode.getChildExps().get(1) instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(1);
    assertEquals(Operator.NOT, nlNode.getOperator());
    assertEquals(1, nlNode.getChildExps().size());
    assertEquals("b", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());

    // !((a|b)&!(c&!b)) -> (!((a | b) & (!(c & (!b)))))
    node = parser.parse("!((a | b) & !(c & !b))");
    assertTrue(node instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.NOT, nlNode.getOperator());
    assertEquals(1, nlNode.getChildExps().size());
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertTrue(nlNode.getChildExps().get(0) instanceof NonLeafExpressionNode);
    assertTrue(nlNode.getChildExps().get(1) instanceof NonLeafExpressionNode);
    nlNodeLeft = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    nlNodeRight = (NonLeafExpressionNode) nlNode.getChildExps().get(1);
    assertEquals(Operator.OR, nlNodeLeft.getOperator());
    assertEquals("a", ((LeafExpressionNode) nlNodeLeft.getChildExps().get(0)).getIdentifier());
    assertEquals("b", ((LeafExpressionNode) nlNodeLeft.getChildExps().get(1)).getIdentifier());
    assertEquals(Operator.NOT, nlNodeRight.getOperator());
    assertEquals(1, nlNodeRight.getChildExps().size());
    nlNodeRight = (NonLeafExpressionNode) nlNodeRight.getChildExps().get(0);
    assertEquals(Operator.AND, nlNodeRight.getOperator());
    assertEquals(2, nlNodeRight.getChildExps().size());
    assertEquals("c", ((LeafExpressionNode) nlNodeRight.getChildExps().get(0)).getIdentifier());
    assertTrue(nlNodeRight.getChildExps().get(1) instanceof NonLeafExpressionNode);
    nlNodeRight = (NonLeafExpressionNode) nlNodeRight.getChildExps().get(1);
    assertEquals(Operator.NOT, nlNodeRight.getOperator());
    assertEquals(1, nlNodeRight.getChildExps().size());
    assertEquals("b", ((LeafExpressionNode) nlNodeRight.getChildExps().get(0)).getIdentifier());
  }

  @Test
  public void testNegativeCases() throws Exception {
    executeNegativeCase("(");
    executeNegativeCase(")");
    executeNegativeCase("()");
    executeNegativeCase("(a");
    executeNegativeCase("a&");
    executeNegativeCase("a&|b");
    executeNegativeCase("!");
    executeNegativeCase("a!");
    executeNegativeCase("a!&");
    executeNegativeCase("&");
    executeNegativeCase("|");
    executeNegativeCase("!(a|(b&c)&!b");
    executeNegativeCase("!!a");
    executeNegativeCase("( a & b ) | ( c & d e)");
    executeNegativeCase("! a");
  }

  @Test
  public void testNonAsciiCases() throws Exception {
    ExpressionNode node = parser.parse(CellVisibility.quote("\u0027") + "&"
        + CellVisibility.quote("\u002b") + "|" + CellVisibility.quote("\u002d") + "&"
        + CellVisibility.quote("\u003f"));
    assertTrue(node instanceof NonLeafExpressionNode);
    NonLeafExpressionNode nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("\u003f", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    assertTrue(nlNode.getChildExps().get(0) instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    assertEquals(Operator.OR, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("\u002d", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    assertTrue(nlNode.getChildExps().get(0) instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("\u002b", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    assertEquals("\u0027", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());

    node = parser.parse(CellVisibility.quote("\u0027") + "&" + CellVisibility.quote("\u002b") + "|"
        + CellVisibility.quote("\u002d") + "&" + CellVisibility.quote("\u003f"));
    assertTrue(node instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("\u003f", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    assertTrue(nlNode.getChildExps().get(0) instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    assertEquals(Operator.OR, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("\u002d", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    assertTrue(nlNode.getChildExps().get(0) instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("\u002b", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    assertEquals("\u0027", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());
  }

  @Test
  public void testCasesSeperatedByDoubleQuotes() throws Exception {
    ExpressionNode node = null;
    try {
      node = parser.parse("\u0027&\"|\u002b&\u003f");
      fail("Excpetion must be thrown as there are special characters without quotes");
    } catch (ParseException e) {
    }
    node = parser.parse(CellVisibility.quote("\u0027") + "&" + CellVisibility.quote("\"") + "|"
        + CellVisibility.quote("\u002b" + "&" + "\u003f"));
    assertTrue(node instanceof NonLeafExpressionNode);
    NonLeafExpressionNode nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.OR, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("\u002b" + "&" + "\u003f",
        ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    assertTrue(nlNode.getChildExps().get(0) instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("\"", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    assertEquals("\u0027", ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());
    try {
      node = parser.parse(CellVisibility.quote("\u0027&\\") + "|"
          + CellVisibility.quote("\u002b" + "&" + "\\") + CellVisibility.quote("$$\""));
      fail("Excpetion must be thrown as there is not operator");
    } catch (ParseException e) {
    }
    node = parser.parse(CellVisibility.quote("\u0027" + "&" + "\\") + "|"
        + CellVisibility.quote("\u003f" + "&" + "\\") + "&" + CellVisibility.quote("$$\""));
    assertTrue(node instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) node;
    assertEquals(Operator.AND, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("$$\"", ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    assertTrue(nlNode.getChildExps().get(0) instanceof NonLeafExpressionNode);
    nlNode = (NonLeafExpressionNode) nlNode.getChildExps().get(0);
    assertEquals(Operator.OR, nlNode.getOperator());
    assertEquals(2, nlNode.getChildExps().size());
    assertEquals("\u0027" + "&" + "\\",
        ((LeafExpressionNode) nlNode.getChildExps().get(0)).getIdentifier());
    assertEquals("\u003f" + "&" + "\\",
        ((LeafExpressionNode) nlNode.getChildExps().get(1)).getIdentifier());
    try {
      node = parser.parse(CellVisibility.quote("\u002b&\\") + "|" + CellVisibility.quote("\u0027&\\") + "&"
          + "\"$$");
      fail("Excpetion must be thrown as there is no end quote");
    } catch (ParseException e) {
    }
  }

  private void executeNegativeCase(String exp) {
    try {
      parser.parse(exp);
      fail("Expected ParseException for expression " + exp);
    } catch (ParseException e) {
    }
  }
}
