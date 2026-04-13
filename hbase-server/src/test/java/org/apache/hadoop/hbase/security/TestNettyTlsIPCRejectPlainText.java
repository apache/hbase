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
package org.apache.hadoop.hbase.security;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.io.crypto.tls.X509KeyType;
import org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;

@Tag(RPCTests.TAG)
@Tag(SmallTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: caKeyType={0}, certKeyType={1}, keyPassword={2}")
public class TestNettyTlsIPCRejectPlainText extends AbstractTestTlsRejectPlainText {

  public TestNettyTlsIPCRejectPlainText(X509KeyType caKeyType, X509KeyType certKeyType,
    char[] keyPassword) {
    super(caKeyType, certKeyType, keyPassword);
  }

  @BeforeAll
  public static void setUpBeforeClass() throws IOException {
    UTIL = new HBaseCommonTestingUtil();
    initialize();
  }

  @AfterAll
  public static void tearDownAfterClass() {
    cleanUp();
  }

  @Override
  protected BlockingInterface createStub() throws Exception {
    return TestProtobufRpcServiceImpl.newBlockingStub(rpcClient, rpcServer.getListenerAddress());
  }
}
