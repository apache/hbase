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
package org.apache.hadoop.hbase.thrift2;

import java.util.ArrayList;
import java.util.function.Supplier;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.thrift.TestThriftHttpServer;
import org.apache.hadoop.hbase.thrift.ThriftServer;
import org.apache.hadoop.hbase.thrift2.generated.TColumnFamilyDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.TTableDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.TTableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, MediumTests.class})
public class TestThrift2HttpServer extends TestThriftHttpServer {
  private static final String TABLENAME = "TestThrift2HttpServerTable";

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestThrift2HttpServer.class);

  @Override protected Supplier<ThriftServer> getThriftServerSupplier() {
    return () -> new org.apache.hadoop.hbase.thrift2.ThriftServer(TEST_UTIL.getConfiguration());
  }

  @Override
  protected void talkToThriftServer(String url, int customHeaderSize) throws Exception {
    THttpClient httpClient = new THttpClient(url);
    httpClient.open();

    if (customHeaderSize > 0) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < customHeaderSize; i++) {
        sb.append("a");
      }
      httpClient.setCustomHeader("User-Agent", sb.toString());
    }

    try {
      TProtocol prot;
      prot = new TBinaryProtocol(httpClient);
      THBaseService.Client client = new THBaseService.Client(prot);
      TTableName tTableName = new TTableName();
      tTableName.setNs(Bytes.toBytes(""));
      tTableName.setQualifier(Bytes.toBytes(TABLENAME));
      if (!tableCreated){
        Assert.assertTrue(!client.tableExists(tTableName));
        TTableDescriptor tTableDescriptor = new TTableDescriptor();
        tTableDescriptor.setTableName(tTableName);
        TColumnFamilyDescriptor columnFamilyDescriptor = new TColumnFamilyDescriptor();
        columnFamilyDescriptor.setName(Bytes.toBytes(TABLENAME));
        tTableDescriptor.addToColumns(columnFamilyDescriptor);
        client.createTable(tTableDescriptor, new ArrayList<>());
        tableCreated = true;
      }
      Assert.assertTrue(client.tableExists(tTableName));
    } finally {
      httpClient.close();
    }
  }


}
