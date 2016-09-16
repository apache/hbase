/*
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

package org.apache.hadoop.hbase.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ServerName;

import java.util.ArrayList;
import java.util.List;

class BatchErrors {
  private static final Log LOG = LogFactory.getLog(BatchErrors.class);
  final List<Throwable> throwables = new ArrayList<Throwable>();
  final List<Row> actions = new ArrayList<Row>();
  final List<String> addresses = new ArrayList<String>();

  public synchronized void add(Throwable ex, Row row, ServerName serverName) {
    if (row == null){
      throw new IllegalArgumentException("row cannot be null. location=" + serverName);
    }

    throwables.add(ex);
    actions.add(row);
    addresses.add(serverName != null ? serverName.toString() : "null");
  }

  public boolean hasErrors() {
    return !throwables.isEmpty();
  }

  synchronized RetriesExhaustedWithDetailsException makeException(boolean logDetails) {
    if (logDetails) {
      LOG.error("Exception occurred! Exception details: " + throwables + ";\nActions: "
              + actions);
    }
    return new RetriesExhaustedWithDetailsException(new ArrayList<Throwable>(throwables),
            new ArrayList<Row>(actions), new ArrayList<String>(addresses));
  }

  public synchronized void clear() {
    throwables.clear();
    actions.clear();
    addresses.clear();
  }

  public synchronized void merge(BatchErrors other) {
    throwables.addAll(other.throwables);
    actions.addAll(other.actions);
    addresses.addAll(other.addresses);
  }
}
