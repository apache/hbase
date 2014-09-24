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

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.security.visibility.expression.ExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.LeafExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.NonLeafExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.Operator;
import org.apache.hadoop.hbase.util.Bytes;

@InterfaceAudience.Private
public class ExpressionParser {

  private static final char CLOSE_PARAN = ')';
  private static final char OPEN_PARAN = '(';
  private static final char OR = '|';
  private static final char AND = '&';
  private static final char NOT = '!';
  private static final char SPACE = ' ';
  private static final char DOUBLE_QUOTES = '"';
  public ExpressionNode parse(String expS) throws ParseException {
    expS = expS.trim();
    Stack<ExpressionNode> expStack = new Stack<ExpressionNode>();
    int index = 0;
    byte[] exp = Bytes.toBytes(expS);
    int endPos = exp.length;
    while (index < endPos) {
      byte b = exp[index];
      switch (b) {
        case OPEN_PARAN:
          processOpenParan(expStack, expS, index);
          index = skipSpaces(exp, index);
          break;
        case CLOSE_PARAN:
          processCloseParan(expStack, expS, index);
          index = skipSpaces(exp, index);
          break;
        case AND:
        case OR:
          processANDorOROp(getOperator(b), expStack, expS, index);
          index = skipSpaces(exp, index);
          break;
        case NOT:
          processNOTOp(expStack, expS, index);
          break;
        case DOUBLE_QUOTES:
          int labelOffset = ++index;
          // We have to rewrite the expression within double quotes as incase of expressions 
          // with escape characters we may have to avoid them as the original expression did
          // not have them
          List<Byte> list = new ArrayList<Byte>();
          while (index < endPos && !endDoubleQuotesFound(exp[index])) {
            if (exp[index] == '\\') {
              index++;
              if (exp[index] != '\\' && exp[index] != '"')
                throw new ParseException("invalid escaping with quotes " + expS + " at column : "
                    + index);
            }
            list.add(exp[index]);
            index++;
          }
          // The expression has come to the end. still no double quotes found 
          if(index == endPos) {
            throw new ParseException("No terminating quotes " + expS + " at column : " + index);
          }
          // This could be costly. but do we have any alternative?
          // If we don't do this way then we may have to handle while checking the authorizations.
          // Better to do it here.
          byte[] array = com.google.common.primitives.Bytes.toArray(list);
          String leafExp = Bytes.toString(array).trim();
          if (leafExp.isEmpty()) {
            throw new ParseException("Error parsing expression " + expS + " at column : " + index);
          }
          processLabelExpNode(new LeafExpressionNode(leafExp), expStack, expS, index);
          index = skipSpaces(exp, index);
          break;
        default:
          labelOffset = index;
          do {
            if (!VisibilityLabelsValidator.isValidAuthChar(exp[index])) {
              throw new ParseException("Error parsing expression " 
                 + expS + " at column : " + index);
            }
            index++;
          } while (index < endPos && !isEndOfLabel(exp[index]));
          leafExp = new String(exp, labelOffset, index - labelOffset).trim();
          if (leafExp.isEmpty()) {
            throw new ParseException("Error parsing expression " + expS + " at column : " + index);
          }
          processLabelExpNode(new LeafExpressionNode(leafExp), expStack, expS, index);
          // We already crossed the label node index. So need to reduce 1 here.
          index--;
          index = skipSpaces(exp, index);
      }
      index++;
    }
    if (expStack.size() != 1) {
      throw new ParseException("Error parsing expression " + expS);
    }
    ExpressionNode top = expStack.pop();
    if (top == LeafExpressionNode.OPEN_PARAN_NODE) {
      throw new ParseException("Error parsing expression " + expS);
    }
    if (top instanceof NonLeafExpressionNode) {
      NonLeafExpressionNode nlTop = (NonLeafExpressionNode) top;
      if (nlTop.getOperator() == Operator.NOT) {
        if (nlTop.getChildExps().size() != 1) {
          throw new ParseException("Error parsing expression " + expS);
        }
      } else if (nlTop.getChildExps().size() != 2) {
        throw new ParseException("Error parsing expression " + expS);
      }
    }
    return top;
  }

  private int skipSpaces(byte[] exp, int index) {
    while (index < exp.length -1 && exp[index+1] == SPACE) {
      index++;
    }
    return index;
  }

  private void processCloseParan(Stack<ExpressionNode> expStack, String expS, int index)
      throws ParseException {
    if (expStack.size() < 2) {
      // When ) comes we expect atleast a ( node and another leaf/non leaf node
      // in stack.
      throw new ParseException();
    } else {
      ExpressionNode top = expStack.pop();
      ExpressionNode secondTop = expStack.pop();
      // The second top must be a ( node and top should not be a ). Top can be
      // any thing else
      if (top == LeafExpressionNode.OPEN_PARAN_NODE
          || secondTop != LeafExpressionNode.OPEN_PARAN_NODE) {
        throw new ParseException("Error parsing expression " + expS + " at column : " + index);
      }
      // a&(b|) is not valid.
      // The top can be a ! node but with exactly child nodes. !).. is invalid
      // Other NonLeafExpressionNode , then there should be exactly 2 child.
      // (a&) is not valid.
      if (top instanceof NonLeafExpressionNode) {
        NonLeafExpressionNode nlTop = (NonLeafExpressionNode) top;
        if ((nlTop.getOperator() == Operator.NOT && nlTop.getChildExps().size() != 1)
            || (nlTop.getOperator() != Operator.NOT && nlTop.getChildExps().size() != 2)) {
          throw new ParseException("Error parsing expression " + expS + " at column : " + index);
        }
      }
      // When (a|b)&(c|d) comes while processing the second ) there will be
      // already (a|b)& node
      // avail in the stack. The top will be c|d node. We need to take it out
      // and combine as one
      // node.
      if (!expStack.isEmpty()) {
        ExpressionNode thirdTop = expStack.peek();
        if (thirdTop instanceof NonLeafExpressionNode) {
          NonLeafExpressionNode nlThirdTop = (NonLeafExpressionNode) expStack.pop();
          nlThirdTop.addChildExp(top);
          if (nlThirdTop.getOperator() == Operator.NOT) {
            // It is a NOT node. So there may be a NonLeafExpressionNode below
            // it to which the
            // completed NOT can be added now.
            if (!expStack.isEmpty()) {
              ExpressionNode fourthTop = expStack.peek();
              if (fourthTop instanceof NonLeafExpressionNode) {
                // Its Operator will be OR or AND
                NonLeafExpressionNode nlFourthTop = (NonLeafExpressionNode) fourthTop;
                assert nlFourthTop.getOperator() != Operator.NOT;
                // Also for sure its number of children will be 1
                assert nlFourthTop.getChildExps().size() == 1;
                nlFourthTop.addChildExp(nlThirdTop);
                return;// This case no need to add back the nlThirdTop.
              }
            }
          }
          top = nlThirdTop;
        }
      }
      expStack.push(top);
    }
  }

  private void processOpenParan(Stack<ExpressionNode> expStack, String expS, int index)
      throws ParseException {
    if (!expStack.isEmpty()) {
      ExpressionNode top = expStack.peek();
      // Top can not be a Label Node. a(.. is not valid. but ((a.. is fine.
      if (top instanceof LeafExpressionNode && top != LeafExpressionNode.OPEN_PARAN_NODE) {
        throw new ParseException("Error parsing expression " + expS + " at column : " + index);
      } else if (top instanceof NonLeafExpressionNode) {
        // Top is non leaf.
        // It can be ! node but with out any child nodes. !a(.. is invalid
        // Other NonLeafExpressionNode , then there should be exactly 1 child.
        // a&b( is not valid.
        // a&( is valid though. Also !( is valid
        NonLeafExpressionNode nlTop = (NonLeafExpressionNode) top;
        if ((nlTop.getOperator() == Operator.NOT && nlTop.getChildExps().size() != 0)
            || (nlTop.getOperator() != Operator.NOT && nlTop.getChildExps().size() != 1)) {
          throw new ParseException("Error parsing expression " + expS + " at column : " + index);
        }
      }
    }
    expStack.push(LeafExpressionNode.OPEN_PARAN_NODE);
  }

  private void processLabelExpNode(LeafExpressionNode node, Stack<ExpressionNode> expStack,
      String expS, int index) throws ParseException {
    if (expStack.isEmpty()) {
      expStack.push(node);
    } else {
      ExpressionNode top = expStack.peek();
      if (top == LeafExpressionNode.OPEN_PARAN_NODE) {
        expStack.push(node);
      } else if (top instanceof NonLeafExpressionNode) {
        NonLeafExpressionNode nlTop = (NonLeafExpressionNode) expStack.pop();
        nlTop.addChildExp(node);
        if (nlTop.getOperator() == Operator.NOT && !expStack.isEmpty()) {
          ExpressionNode secondTop = expStack.peek();
          if (secondTop == LeafExpressionNode.OPEN_PARAN_NODE) {
            expStack.push(nlTop);
          } else if (secondTop instanceof NonLeafExpressionNode) {
            ((NonLeafExpressionNode) secondTop).addChildExp(nlTop);
          }
        } else {
          expStack.push(nlTop);
        }
      } else {
        throw new ParseException("Error parsing expression " + expS + " at column : " + index);
      }
    }
  }

  private void processANDorOROp(Operator op, Stack<ExpressionNode> expStack, String expS, int index)
      throws ParseException {
    if (expStack.isEmpty()) {
      throw new ParseException("Error parsing expression " + expS + " at column : " + index);
    }
    ExpressionNode top = expStack.pop();
    if (top.isSingleNode()) {
      if (top == LeafExpressionNode.OPEN_PARAN_NODE) {
        throw new ParseException("Error parsing expression " + expS + " at column : " + index);
      }
      expStack.push(new NonLeafExpressionNode(op, top));
    } else {
      NonLeafExpressionNode nlTop = (NonLeafExpressionNode) top;
      if (nlTop.getChildExps().size() != 2) {
        throw new ParseException("Error parsing expression " + expS + " at column : " + index);
      }
      expStack.push(new NonLeafExpressionNode(op, nlTop));
    }
  }

  private void processNOTOp(Stack<ExpressionNode> expStack, String expS, int index)
      throws ParseException {
    // When ! comes, the stack can be empty or top ( or top can be some exp like
    // a&
    // !!.., a!, a&b!, !a! are invalid
    if (!expStack.isEmpty()) {
      ExpressionNode top = expStack.peek();
      if (top.isSingleNode() && top != LeafExpressionNode.OPEN_PARAN_NODE) {
        throw new ParseException("Error parsing expression " + expS + " at column : " + index);
      }
      if (!top.isSingleNode() && ((NonLeafExpressionNode) top).getChildExps().size() != 1) {
        throw new ParseException("Error parsing expression " + expS + " at column : " + index);
      }
    }
    expStack.push(new NonLeafExpressionNode(Operator.NOT));
  }

  private static boolean endDoubleQuotesFound(byte b) {
    return (b == DOUBLE_QUOTES);
  }
  private static boolean isEndOfLabel(byte b) {
    return (b == OPEN_PARAN || b == CLOSE_PARAN || b == OR || b == AND || 
        b == NOT || b == SPACE);
  }

  private static Operator getOperator(byte op) {
    switch (op) {
    case AND:
      return Operator.AND;
    case OR:
      return Operator.OR;
    case NOT:
      return Operator.NOT;
    }
    return null;
  }
}
