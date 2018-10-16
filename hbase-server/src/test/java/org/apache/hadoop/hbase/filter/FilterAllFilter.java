/*
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
package org.apache.hadoop.hbase.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;

public class FilterAllFilter extends FilterBase {

  public FilterAllFilter() {
  }

  @Override
  public ReturnCode filterCell(final Cell c) throws IOException {
    return ReturnCode.SKIP;
  }

  @Override
  public boolean hasFilterRow() {
    return true;
  }

  @Override
  public boolean filterRow() throws IOException {
    return true;
  }

  @Override
  public boolean filterRowKey(Cell cell) throws IOException {
    return false;
  }

  public static FilterAllFilter parseFrom(final byte[] pbBytes) throws DeserializationException {
    // No options to parse, so why bother
    return new FilterAllFilter();
  }

  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this)
      return true;
    if (!(o instanceof FilterAllFilter))
      return false;

    return true;
  }
}
