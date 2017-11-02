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
package org.apache.hadoop.hbase.regionserver.querymatcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;

public class AbstractTestScanQueryMatcher {

  protected Configuration conf;

  protected byte[] row1;
  protected byte[] row2;
  protected byte[] row3;
  protected byte[] fam1;
  protected byte[] fam2;
  protected byte[] col1;
  protected byte[] col2;
  protected byte[] col3;
  protected byte[] col4;
  protected byte[] col5;

  protected byte[] data;

  protected Get get;

  protected long ttl = Long.MAX_VALUE;
  protected CellComparator rowComparator;
  protected Scan scan;

  @Before
  public void setUp() throws Exception {
    this.conf = HBaseConfiguration.create();
    row1 = Bytes.toBytes("row1");
    row2 = Bytes.toBytes("row2");
    row3 = Bytes.toBytes("row3");
    fam1 = Bytes.toBytes("fam1");
    fam2 = Bytes.toBytes("fam2");
    col1 = Bytes.toBytes("col1");
    col2 = Bytes.toBytes("col2");
    col3 = Bytes.toBytes("col3");
    col4 = Bytes.toBytes("col4");
    col5 = Bytes.toBytes("col5");

    data = Bytes.toBytes("data");

    // Create Get
    get = new Get(row1);
    get.addFamily(fam1);
    get.addColumn(fam2, col2);
    get.addColumn(fam2, col4);
    get.addColumn(fam2, col5);
    this.scan = new Scan(get);

    rowComparator = CellComparator.getInstance();
  }
}
