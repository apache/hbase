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
package org.apache.hadoop.hbase.security.visibility.expression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class NonLeafExpressionNode implements ExpressionNode {
  private Operator op;
  private List<ExpressionNode> childExps = new ArrayList<>(2);

  public NonLeafExpressionNode() {

  }

  public NonLeafExpressionNode(Operator op) {
    this.op = op;
  }

  public NonLeafExpressionNode(Operator op, List<ExpressionNode> exps) {
    this.op = op;
    if (op == Operator.NOT && exps.size() > 1) {
      throw new IllegalArgumentException(Operator.NOT + " should be on 1 child expression");
    }
    this.childExps = exps;
  }

  public NonLeafExpressionNode(Operator op, ExpressionNode... exps) {
    this.op = op;
    List<ExpressionNode> expLst = new ArrayList<>();
    Collections.addAll(expLst, exps);
    this.childExps = expLst;
  }

  public Operator getOperator() {
    return op;
  }

  public List<ExpressionNode> getChildExps() {
    return childExps;
  }

  public void addChildExp(ExpressionNode exp) {
    if (op == Operator.NOT && this.childExps.size() == 1) {
      throw new IllegalStateException(Operator.NOT + " should be on 1 child expression");
    }
    this.childExps.add(exp);
  }

  public void addChildExps(List<ExpressionNode> exps) {
    this.childExps.addAll(exps);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("(");
    if (this.op == Operator.NOT) {
      sb.append(this.op);
    }
    for (int i = 0; i < this.childExps.size(); i++) {
      sb.append(childExps.get(i));
      if (i < this.childExps.size() - 1) {
        sb.append(" " + this.op + " ");
      }
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean isSingleNode() {
    return this.op == Operator.NOT;
  }

  @Override
  public NonLeafExpressionNode deepClone() {
    NonLeafExpressionNode clone = new NonLeafExpressionNode(this.op);
    for (ExpressionNode exp : this.childExps) {
      clone.addChildExp(exp.deepClone());
    }
    return clone;
  }
}
