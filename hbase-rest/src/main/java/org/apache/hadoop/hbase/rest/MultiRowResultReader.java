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
package org.apache.hadoop.hbase.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MultiRowResultReader {

  private static final Logger LOG = LoggerFactory.getLogger(MultiRowResultReader.class);

  private Result[] results;

  public MultiRowResultReader(final String tableName, final Collection<RowSpec> rowspecs,
    final Filter filter, final boolean cacheBlocks) throws IOException {
    try (Table table = RESTServlet.getInstance().getTable(tableName)) {
      List<Get> gets = new ArrayList<>(rowspecs.size());
      for (RowSpec rowspec : rowspecs) {
        Get get = new Get(rowspec.getRow());
        if (rowspec.hasColumns()) {
          for (byte[] col : rowspec.getColumns()) {
            byte[][] split = CellUtil.parseColumn(col);
            if (split.length == 1) {
              get.addFamily(split[0]);
            } else if (split.length == 2) {
              get.addColumn(split[0], split[1]);
            } else {
              throw new IllegalArgumentException("Invalid column specifier.");
            }
          }
        }
        get.setTimeRange(rowspec.getStartTime(), rowspec.getEndTime());
        get.readVersions(rowspec.getMaxVersions());
        if (filter != null) {
          get.setFilter(filter);
        }
        get.setCacheBlocks(cacheBlocks);
        gets.add(get);
      }
      results = table.get(gets);
    } catch (DoNotRetryIOException e) {
      // TODO this is copied from RowResultGenerator, but we probably shouldn't swallow
      // every type of exception but AccessDeniedException
      LOG.warn(StringUtils.stringifyException(e));
      // Lets get the exception rethrown to get a more meaningful error message than 404
      if (e instanceof AccessDeniedException) {
        throw e;
      }
    }
  }

  public Result[] getResults() {
    return results;
  }

}
