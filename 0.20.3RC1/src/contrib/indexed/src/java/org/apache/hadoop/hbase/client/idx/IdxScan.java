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

import org.apache.hadoop.hbase.WritableHelper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.client.idx.exp.Expression;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Collection;

/**
 * Extends the {@link Scan} class to provide an {@link Expression}
 * that is used to quickly reduce the scope of the scan.
 */
public class IdxScan extends Scan {
  /**
   * The key used to store and retrieve the scan index expression.
   */
  public static final ImmutableBytesWritable EXPRESSION =
      new ImmutableBytesWritable(Bytes.toBytes("EXPRESSION"));

  private Expression expression;

  /**
   * No-args constructor.
   */
  public IdxScan() {
  }

  /**
   * Constructs a scan.
   * @param expression the index expression
   */
  public IdxScan(Expression expression) {
    this.expression = expression;
  }

  /**
   * Constructs a scan.
   * @param startRow   row to start scanner at or after (inclusive)
   * @param filter     the filter that will applied to the scan
   * @param expression the index expression
   */
  public IdxScan(byte[] startRow, Filter filter, Expression expression) {
    super(startRow, filter);
    this.expression = expression;
  }

  /**
   * Constructs a scan.
   * @param startRow   row to start scanner at or after (inclusive)
   * @param expression the index expression
   */
  public IdxScan(byte[] startRow, Expression expression) {
    super(startRow);
    this.expression = expression;
  }

  /**
   * Constructs a scan.
   * @param startRow   row to start scanner at or after (inclusive)
   * @param stopRow    row to stop scanner before (exclusive)
   * @param expression the index expression
   */
  public IdxScan(byte[] startRow, byte[] stopRow, Expression expression) {
    super(startRow, stopRow);
    this.expression = expression;
  }

  /**
   * Constructs a scan from the provided scan with the expression.
   * @param scan       the scan to copy from
   * @param expression the index expression
   * @throws IOException if thrown by {@link Scan#Scan(org.apache.hadoop.hbase.client.Scan)}
   */
  public IdxScan(Scan scan, Expression expression) throws IOException {
    super(scan);
    this.expression = expression;
  }

  /**
   * Returns the index expression used by the scan.
   * @return the index expression
   */
  public Expression getExpression() {
    return expression;
  }

  /**
   * Sets the index expression used by the scan.
   * @param expression the index expression
   */
  public void setExpression(Expression expression) {
    this.expression = expression;
  }

  /**
   * Scanning all versions is not currently supported.
   * @return never
   * @throws IllegalStateException if this method is called
   */
  @Override
  public Scan setMaxVersions() {
    throw new IllegalStateException("Scanning all versions is not currently supported.");
  }

  /**
   * Scanning all versions is not currently supported.
   * @param maxVersions maximum versions for each column
   * @return never
   * @throws IllegalStateException if this method is called
   */
  @Override
  public Scan setMaxVersions(int maxVersions) {
    throw new IllegalStateException("Scanning all versions is not currently supported.");
  }

  /**
   * {@inheritDoc}.
   * <p/>
   * Also writes the optional {@link #getExpression()}.
   */
  @Override
  public void write(DataOutput out) throws IOException {
    if (expression != null) {
      values.put(EXPRESSION, writeExpression(expression));
    } else {
      values.remove(EXPRESSION);
    }
    super.write(out);
  }

  private static ImmutableBytesWritable writeExpression(Expression expression) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();

    WritableHelper.writeInstanceNullable(out, expression);

    return new ImmutableBytesWritable(out.getData());
  }

  /**
   * {@inheritDoc}.
   * <p/>
   * Also reads the optional {@link #getExpression()}.
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.expression = getExpression(this);
  }

  public static Expression getExpression(Scan scan) throws IOException {
    if (scan instanceof IdxScan && ((IdxScan) scan).getExpression() != null) {
      return ((IdxScan) scan).getExpression();
    }

    Map<ImmutableBytesWritable,ImmutableBytesWritable> values = scan.getValues();
    if (values.containsKey(EXPRESSION)) {
      DataInputBuffer in = new DataInputBuffer();
      byte[] bytes = values.get(EXPRESSION).get();
      in.reset(bytes, bytes.length);

      return WritableHelper.readInstanceNullable(in, Expression.class);
    } else {
      return null;
    }
  }
}
