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

import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.security.visibility.expression.ExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.LeafExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.NonLeafExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.Operator;

@InterfaceAudience.Private
public class ExpressionExpander {

  public ExpressionNode expand(ExpressionNode src) {
    if (!src.isSingleNode()) {
      NonLeafExpressionNode nlExp = (NonLeafExpressionNode) src;
      List<ExpressionNode> childExps = nlExp.getChildExps();
      Operator outerOp = nlExp.getOperator();
      if (isToBeExpanded(childExps)) {
        // Any of the child exp is a non leaf exp with & or | operator
        NonLeafExpressionNode newNode = new NonLeafExpressionNode(nlExp.getOperator());
        for (ExpressionNode exp : childExps) {
          if (exp.isSingleNode()) {
            newNode.addChildExp(exp);
          } else {
            newNode.addChildExp(expand(exp));
          }
        }
        nlExp = expandNonLeaf(newNode, outerOp);
      }
      return nlExp;
    }
    if (src instanceof NonLeafExpressionNode
        && ((NonLeafExpressionNode) src).getOperator() == Operator.NOT) {
      // Negate the exp
      return negate((NonLeafExpressionNode) src);
    }
    return src;
  }

  private ExpressionNode negate(NonLeafExpressionNode nlExp) {
    ExpressionNode notChild = nlExp.getChildExps().get(0);
    if (notChild instanceof LeafExpressionNode) {
      return nlExp;
    }
    NonLeafExpressionNode nlNotChild = (NonLeafExpressionNode) notChild;
    if (nlNotChild.getOperator() == Operator.NOT) {
      // negate the negate
      return nlNotChild.getChildExps().get(0);
    }
    Operator negateOp = nlNotChild.getOperator() == Operator.AND ? Operator.OR : Operator.AND;
    NonLeafExpressionNode newNode = new NonLeafExpressionNode(negateOp);
    for (ExpressionNode expNode : nlNotChild.getChildExps()) {
      NonLeafExpressionNode negateNode = new NonLeafExpressionNode(Operator.NOT);
      negateNode.addChildExp(expNode.deepClone());
      newNode.addChildExp(expand(negateNode));
    }
    return newNode;
  }

  private boolean isToBeExpanded(List<ExpressionNode> childExps) {
    for (ExpressionNode exp : childExps) {
      if (!exp.isSingleNode()) {
        return true;
      }
    }
    return false;
  }

  private NonLeafExpressionNode expandNonLeaf(NonLeafExpressionNode newNode, Operator outerOp) {
    // Now go for the merge or expansion across brackets
    List<ExpressionNode> newChildExps = newNode.getChildExps();
    assert newChildExps.size() == 2;
    ExpressionNode leftChild = newChildExps.get(0);
    ExpressionNode rightChild = newChildExps.get(1);
    if (rightChild.isSingleNode()) {
      // Merge the single right node into the left side
      assert leftChild instanceof NonLeafExpressionNode;
      newNode = mergeChildNodes(newNode, outerOp, rightChild, (NonLeafExpressionNode) leftChild);
    } else if (leftChild.isSingleNode()) {
      // Merge the single left node into the right side
      assert rightChild instanceof NonLeafExpressionNode;
      newNode = mergeChildNodes(newNode, outerOp, leftChild, (NonLeafExpressionNode) rightChild);
    } else {
      // Both the child exp nodes are non single.
      NonLeafExpressionNode leftChildNLE = (NonLeafExpressionNode) leftChild;
      NonLeafExpressionNode rightChildNLE = (NonLeafExpressionNode) rightChild;
      if (outerOp == leftChildNLE.getOperator() && outerOp == rightChildNLE.getOperator()) {
        // Merge
        NonLeafExpressionNode leftChildNLEClone = leftChildNLE.deepClone();
        leftChildNLEClone.addChildExps(rightChildNLE.getChildExps());
        newNode = leftChildNLEClone;
      } else {
        // (a | b) & (c & d) ...
        if (outerOp == Operator.OR) {
          // (a | b) | (c & d)
          if (leftChildNLE.getOperator() == Operator.OR
              && rightChildNLE.getOperator() == Operator.AND) {
            leftChildNLE.addChildExp(rightChildNLE);
            newNode = leftChildNLE;
          } else if (leftChildNLE.getOperator() == Operator.AND
              && rightChildNLE.getOperator() == Operator.OR) {
            // (a & b) | (c | d)
            rightChildNLE.addChildExp(leftChildNLE);
            newNode = rightChildNLE;
          }
          // (a & b) | (c & d)
          // This case no need to do any thing
        } else {
          // outer op is &
          // (a | b) & (c & d) => (a & c & d) | (b & c & d)
          if (leftChildNLE.getOperator() == Operator.OR
              && rightChildNLE.getOperator() == Operator.AND) {
            newNode = new NonLeafExpressionNode(Operator.OR);
            for (ExpressionNode exp : leftChildNLE.getChildExps()) {
              NonLeafExpressionNode rightChildNLEClone = rightChildNLE.deepClone();
              rightChildNLEClone.addChildExp(exp);
              newNode.addChildExp(rightChildNLEClone);
            }
          } else if (leftChildNLE.getOperator() == Operator.AND
              && rightChildNLE.getOperator() == Operator.OR) {
            // (a & b) & (c | d) => (a & b & c) | (a & b & d)
            newNode = new NonLeafExpressionNode(Operator.OR);
            for (ExpressionNode exp : rightChildNLE.getChildExps()) {
              NonLeafExpressionNode leftChildNLEClone = leftChildNLE.deepClone();
              leftChildNLEClone.addChildExp(exp);
              newNode.addChildExp(leftChildNLEClone);
            }
          } else {
            // (a | b) & (c | d) => (a & c) | (a & d) | (b & c) | (b & d)
            newNode = new NonLeafExpressionNode(Operator.OR);
            for (ExpressionNode leftExp : leftChildNLE.getChildExps()) {
              for (ExpressionNode rightExp : rightChildNLE.getChildExps()) {
                NonLeafExpressionNode newChild = new NonLeafExpressionNode(Operator.AND);
                newChild.addChildExp(leftExp.deepClone());
                newChild.addChildExp(rightExp.deepClone());
                newNode.addChildExp(newChild);
              }
            }
          }
        }
      }
    }
    return newNode;
  }

  private NonLeafExpressionNode mergeChildNodes(NonLeafExpressionNode newOuterNode,
      Operator outerOp, ExpressionNode lChild, NonLeafExpressionNode nlChild) {
    // Merge the single right/left node into the other side
    if (nlChild.getOperator() == outerOp) {
      NonLeafExpressionNode leftChildNLEClone = nlChild.deepClone();
      leftChildNLEClone.addChildExp(lChild);
      newOuterNode = leftChildNLEClone;
    } else if (outerOp == Operator.AND) {
      assert nlChild.getOperator() == Operator.OR;
      // outerOp is & here. We need to expand the node here
      // (a | b) & c -> (a & c) | (b & c)
      // OR
      // c & (a | b) -> (c & a) | (c & b)
      newOuterNode = new NonLeafExpressionNode(Operator.OR);
      for (ExpressionNode exp : nlChild.getChildExps()) {
        newOuterNode.addChildExp(new NonLeafExpressionNode(Operator.AND, exp, lChild));
      }
    }
    return newOuterNode;
  }
}
