/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import static org.junit.Assert.*;

import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Test;

import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.ThriftCodecManager;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestHServerAddress {

  /**
   * Tests if HServerAddress was correctly serialized and deserialized
   * @throws Exception
   */
  @Test
  public void testSerializeDeserialize() throws Exception {
    ThriftCodec<HServerAddress> codec = new ThriftCodecManager()
        .getCodec(HServerAddress.class);
    TMemoryBuffer transport = new TMemoryBuffer(1000 * 1024);
    TCompactProtocol protocol = new TCompactProtocol(transport);
    HServerAddress serverAddress = new HServerAddress("127.0.0.1", 60020);
    codec.write(serverAddress, protocol);
    HServerAddress serverAddrCopy = codec.read(protocol);
    assertEquals(serverAddress, serverAddrCopy);
  }

}
