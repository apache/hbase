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

import java.util.Collection;

/**
 * This class implements boolean OR - any sub-expressions can be true in order
 * for it to be true.
 */
public class Or extends Compound {
  /**
   * Internal constructor.
   */
  public Or() {
    super();
  }

  /**
   * Constructs an or expression with provided expression.
   * @param expressions the expression
   */
  public Or(Expression... expressions) {
    super(expressions);
  }

  /**
   * Constructs an or expression with provided expression.
   * @param expressions the expression
   */
  public Or(Collection<Expression> expressions) {
    super(expressions);
  }

  /**
   * Adds the expression to the set of expression.
   * @param expression the expression
   * @return this
   * @see Compound#add(Expression)
   */
  public Or or(Expression expression) {
    return (Or) super.add(expression);
  }
}
