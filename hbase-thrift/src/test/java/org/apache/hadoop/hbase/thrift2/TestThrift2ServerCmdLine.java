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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.function.Supplier;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.thrift.ImplType;
import org.apache.hadoop.hbase.thrift.TestThriftServerCmdLine;
import org.apache.hadoop.hbase.thrift.ThriftServer;
import org.apache.hadoop.hbase.thrift2.generated.TColumnFamilyDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.TTableDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.TTableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, MediumTests.class})
public class TestThrift2ServerCmdLine extends TestThriftServerCmdLine {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestThrift2ServerCmdLine.class);

  private static final String TABLENAME = "TestThrift2ServerCmdLineTable";

  public TestThrift2ServerCmdLine(ImplType implType, boolean specifyFramed,
      boolean specifyBindIP, boolean specifyCompact) {
    super(implType, specifyFramed, specifyBindIP, specifyCompact);
  }

  @Override protected Supplier<ThriftServer> getThriftServerSupplier() {
    return () -> new org.apache.hadoop.hbase.thrift2.ThriftServer(TEST_UTIL.getConfiguration());
  }

  @Override
  protected void talkToThriftServer(int port) throws Exception {
    TSocket sock = new TSocket(InetAddress.getLoopbackAddress().getHostName(), port);
    TTransport transport = sock;
    if (specifyFramed || implType.isAlwaysFramed()) {
      transport = new TFramedTransport(transport);
    }

    sock.open();
    try {
      TProtocol tProtocol;
      if (specifyCompact) {
        tProtocol = new TCompactProtocol(transport);
      } else {
        tProtocol = new TBinaryProtocol(transport);
      }
      THBaseService.Client client = new THBaseService.Client(tProtocol);
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
      Assert.assertTrue("tableCreated " + tableCreated, client.tableExists(tTableName));
    } finally {
      sock.close();
    }
  }
}
