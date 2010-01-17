/**
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

import org.apache.hadoop.io.Writable;

/**
 * Class representing an expression.
 */
public abstract class Expression implements Writable {
  /**
   * {@inheritDoc}
   */
  public abstract int hashCode();

  /**
   * {@inheritDoc}
   */
  public abstract boolean equals(Object o);

  /**
   * Creates and returns an {@link Or} instance.
   * @param expressions the expressions
   * @return an instance
   */
  public static Or or(Expression... expressions) {
    return new Or(expressions);
  }

  /**
   * Creates and returns an {@link And} instance.
   * @param expressions the expressions
   * @return an instance
   */
  public static And and(Expression... expressions) {
    return new And(expressions);
  }

  /**
   * Creates and returns an {@link Comparison}
   * instance.
   * @param family the column family name
   * @param qualifier  the qualifier
   * @param operator   the operator
   * @param value      the value
   * @return the instance
   */
  public static Comparison comparison(byte[] family, byte[] qualifier, Comparison.Operator operator, byte[] value) {
    return new Comparison(family, qualifier, operator, value);
  }

  /**
   * Creates and returns an {@link Comparison}
   * instance.
   * @param family the column family name
   * @param qualifier  the qualifier
   * @param operator   the operator
   * @param value      the value
   * @return the instance
   */
  public static Comparison comparison(String family, String qualifier, Comparison.Operator operator, byte[] value) {
    return new Comparison(family, qualifier, operator, value);
  }
}
