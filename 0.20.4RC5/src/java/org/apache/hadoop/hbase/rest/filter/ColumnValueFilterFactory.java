/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.rest.filter;

import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;

/**
 * FilterFactory that constructs a ColumnValueFilter from a JSON arg String.
 * Expects a Stringified JSON argument with the following form:
 * 
 * { "column_name" : "MY_COLUMN_NAME", "compare_op" : "INSERT_COMPARE_OP_HERE",
 * "value" : "MY_COMPARE_VALUE" }
 * 
 * The current valid compare ops are: equal, greater, greater_or_equal, less,
 * less_or_equal, not_equal
 */
public class ColumnValueFilterFactory implements FilterFactory {

  public RowFilterInterface getFilterFromJSON(String args)
      throws HBaseRestException {
    throw new HBaseRestException("Not implemented in > 0.20.3 HBase");
  }
}
