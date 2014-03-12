/**
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
package org.apache.hadoop.hbase.util.rpcbench;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;

/**
 * Factory class which can create a ThriftBenchmarkClient which performs
 * simple operations via thrift.
 */
public class ThriftBenchmarkFactory implements BenchmarkFactory {
  private static final Log LOG = LogFactory.getLog(ThriftBenchmarkFactory.class);

  @Override
  public BenchmarkClient makeBenchmarkClient(byte[] table, Configuration conf) {
    HTable htable;
    try {
      Configuration c = HBaseConfiguration.create(conf);
      htable = new HTable(c, table);
    } catch (IOException e) {
      LOG.debug("Unabe to create an HTable client please" +
          "check the error trace for signs of problems.");
      e.printStackTrace();
      return null;
    }
    return new ThriftBenchmarkClient(htable);
  }
}
