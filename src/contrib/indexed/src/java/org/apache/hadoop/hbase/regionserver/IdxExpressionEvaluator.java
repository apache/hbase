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

import org.apache.hadoop.hbase.client.idx.exp.And;
import org.apache.hadoop.hbase.client.idx.exp.Comparison;
import org.apache.hadoop.hbase.client.idx.exp.Expression;
import org.apache.hadoop.hbase.client.idx.exp.Or;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSet;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Evaluates an {@link Expression}.
 */
public class IdxExpressionEvaluator implements HeapSize {
  private static final Log LOG = LogFactory.getLog(IdxExpressionEvaluator.class);
  /**
   * Evaluates the expression using the provided search context.
   *
   * @param searchContext the search context to use whe evaluating the
   *                      exrpession
   * @param expression    the expression to evaluate.
   * @return a set which contains ids of rows matching the expression provided
   */
  public IntSet evaluate(IdxSearchContext searchContext, Expression expression) {
    if (expression == null) return null;

    if (expression instanceof And) {
      return evaluate(searchContext, (And) expression);
    } else if (expression instanceof Or) {
      return evaluate(searchContext, (Or) expression);
    } else if (expression instanceof Comparison) {
      return evaluate(searchContext, (Comparison) expression);
    } else {
      throw new IllegalArgumentException("Could not evaluate expression type " +
        expression.getClass().getName());
    }
  }

  protected IntSet evaluate(IdxSearchContext searchContext, And and) {
    IntSet result = null;
    for (Expression expression : and.getChildren()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Intersecting expression:");
      }
      IntSet childResult = evaluate(searchContext, expression);
      if (result == null) {
        result = childResult;
      } else if (childResult != null) {
        result = result.intersect(childResult);
      }
    }
    return result;
  }

  protected IntSet evaluate(IdxSearchContext searchContext, Or or) {
    IntSet result = null;
    for (Expression expression : or.getChildren()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Uniting expression:");
      }
      IntSet childResult = evaluate(searchContext, expression);
      if (result == null) {
        result = childResult;
      } else if (childResult != null) {
        result = result.unite(childResult);
      }
    }
    return result;
  }

  protected IntSet evaluate(IdxSearchContext searchContext, Comparison comparison) {
    IdxIndex index = searchContext.getIndex(comparison.getColumnName(), comparison.getQualifier());
    if (index == null) throw new IllegalStateException(
            String.format("Could not find an index for column: '%s', qualifier: '%s'",
                    Bytes.toString(comparison.getColumnName()),
                    Bytes.toString(comparison.getQualifier())));

    IntSet matched = null;
    boolean resultIncludesMissing = false;
    switch (comparison.getOperator()) {
      case EQ:
        matched = index.lookup(comparison.getValue());
        break;
      case NEQ:
        matched = index.lookup(comparison.getValue());
        matched = matched.complement();
        // When we complement the matched set we may include ids which are
        // missing from the index
        resultIncludesMissing = true;
        break;
      case GT:
        matched = index.tail(comparison.getValue(), false);
        break;
      case GTE:
        matched = index.tail(comparison.getValue(), true);
        break;
      case LT:
        matched = index.head(comparison.getValue(), false);
        break;
      case LTE:
        matched = index.head(comparison.getValue(), true);
        break;
    }

    if (comparison.getIncludeMissing() != resultIncludesMissing) {
      matched = resultIncludesMissing ? matched.intersect(index.all()) : matched.unite(index.all().complement());
    }

    if (LOG.isDebugEnabled() && matched != null) {
      LOG.debug(String.format("Evaluation of comparison on column: '%s', " +
        "qualifier: '%s', operator: %s, value: '%s' include missing: '%b' " +
        "yielded %s matches",
        Bytes.toString(comparison.getColumnName()),
        Bytes.toString(comparison.getQualifier()),
        comparison.getOperator(),
        index.probeToString(comparison.getValue()),
        comparison.getIncludeMissing(), matched.size()));
    }

    return matched != null ? matched : null;
  }

  @Override
  public long heapSize() {
    return ClassSize.OBJECT;
  }
}
