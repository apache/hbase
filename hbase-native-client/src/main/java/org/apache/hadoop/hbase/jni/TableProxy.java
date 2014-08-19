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
 *
 */
package org.apache.hadoop.hbase.jni;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

public final class TableProxy {
  protected HTable table_;

  public TableProxy(Configuration conf, String name) throws IOException {
    table_ = new HTable(conf, name);
  }

  public void setAutoFlush(boolean autoFlush) {
    table_.setAutoFlushTo(autoFlush);
  }

  public void close() throws IOException {
    table_.close();
  }

  public void flushCommits() throws IOException {
    table_.flushCommits();
  }

  public void put(final List<PutProxy> putProxies) throws IOException {
    List<Put> puts = new ArrayList<Put>(putProxies.size());
    for (PutProxy putProxy : putProxies) {
      puts.add((Put) putProxy.toHBaseMutation());
    }
    table_.put(puts);
  }

  @Deprecated
  public Object[] batch(final List<MutationProxy> mutations)
      throws InterruptedException, IOException {
    List<Row> actions = new ArrayList<Row>(mutations.size());
    for (MutationProxy mutation : mutations) {
      actions.add(mutation.toHBaseMutation());
    }
    return table_.batch(actions);
  }

}
