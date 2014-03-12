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
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Implements the BenchmarkClient interface and provides functions to perform
 * gets and puts.
 *
 */
public class ThriftBenchmarkClient implements BenchmarkClient {
  private static final Log LOG = LogFactory.getLog(ThriftBenchmarkClient.class);
  private HTable htable = null;

  ThriftBenchmarkClient(HTable htable) {
    this.htable = htable;
    this.htable.setAutoFlush(true);
  }

  @Override
  public Result executeGet(Get get) {
    Result r = null;
    try {
      r = this.htable.get(get);
    } catch (IOException e) {
      LOG.debug("Unable to perform get");
      e.printStackTrace();
    }
    return r;
  }

  @Override
  public void executePut(Put put) {
    try {
      this.htable.put(put);
    } catch (IOException e) {
      LOG.debug("Unable to perform put");
      e.printStackTrace();
    }
  }

  /**
   * TODO: make use of qual or get rid of it
   */
  public Get createGet(byte[] row, byte[] family, byte[] qual) {
    return new Get.Builder(row).addFamily(family).create();
  }

  public Put createPut(byte[] row, byte[] family, byte[] qual, byte[] value) {
    Put p = new Put(row);
    p.add(family, qual, value);
    return p;
  }

  @Override
  public List<Result> executeScan(Scan scan) {
    throw new NotImplementedException();
  }

  @Override
  public Scan createScan(byte[] row, byte[] family, int nbRows) {
    throw new NotImplementedException();
  }

}
