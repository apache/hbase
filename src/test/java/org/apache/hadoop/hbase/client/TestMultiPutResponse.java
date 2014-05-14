/*
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
package org.apache.hadoop.hbase.client;

import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.ThriftCodecManager;
import junit.framework.Assert;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMultiPutResponse {

  /**
   * Test if the MultiPutResponse object is correctly serialized and
   * deserialized.
   */
  @Test
  public void testSwiftSerDe() throws Exception {
    byte[][] r = { Bytes.toBytes("r1"), Bytes.toBytes("r2"),
        Bytes.toBytes("r3") };
    MultiPutResponse m = new MultiPutResponse();
    for (int i = 0; i < r.length; i++) {
      m.addResult(r[i], i);
    }
    ThriftCodec<MultiPutResponse> codec = new ThriftCodecManager().
        getCodec(MultiPutResponse.class);
    TMemoryBuffer transport = new TMemoryBuffer(1000 * 1024);
    TCompactProtocol protocol = new TCompactProtocol(transport);
    codec.write(m, protocol);

    MultiPutResponse other = codec.read(protocol);
    Assert.assertEquals(m, other);
  }
}
