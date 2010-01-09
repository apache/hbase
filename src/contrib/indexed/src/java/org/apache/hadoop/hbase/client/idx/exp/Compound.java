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

import org.apache.hadoop.hbase.WritableHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A compound expression has no built-in tests but aggregates a set of child
 * expressions into a logical group such as boolean logic.
 */
public abstract class Compound extends Expression {
  /**
   * The set of child expressions.
   */
  protected Set<Expression> children;

  /**
   * Class constructor.
   * @param expressions the expressions to be evaluated
   */
  public Compound(Expression... expressions) {
    assert expressions != null : "expressions cannot be null or empty";
    this.children = new HashSet<Expression>(Arrays.asList(expressions));
  }

  /**
   * Class constructor.
   * @param expressions the expressions to be evaluated
   */
  public Compound(Collection<Expression> expressions) {
    this.children = new HashSet<Expression>(expressions);
  }

  /**
   * Add an expression to the child set.
   * @param expression the expression to add
   * @return this
   */
  public Compound add(Expression expression) {
    this.children.add(expression);
    return this;
  }

  /**
   * Returns the set of child expressions.
   * @return the expression set
   */
  public Set<Expression> getChildren() {
    return children;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Compound that = (Compound) o;

    if (!children.equals(that.children)) {
      return false;
    }

    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return children.hashCode();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(children.size());
    for (Expression child : children) {
      WritableHelper.writeInstance(dataOutput, child);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int size = dataInput.readInt();
    children = new HashSet<Expression>(size);
    for (int i = 0; i < size; i++) {
      Expression expression = WritableHelper.readInstance(dataInput, Expression.class);
      children.add(expression);
    }
  }
}
