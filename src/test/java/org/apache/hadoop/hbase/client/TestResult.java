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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Assert;
import org.junit.Test;

import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.ThriftCodecManager;


public class TestResult {

  /**
   * Tests if the Result Object is correctly serialized & deserialized
   *
   * @throws Exception
   */
  @Test
  public void testSwiftSerDe() throws Exception {
    ThriftCodec<Result> codec = new ThriftCodecManager().getCodec(Result.class);
    TMemoryBuffer transport = new TMemoryBuffer(1000 * 1024);
    TCompactProtocol protocol = new TCompactProtocol(transport);

    List<KeyValue> kvs = constuctKvList(100);
    Result res = new Result(kvs);

    codec.write(res, protocol);
    Result resCopy = codec.read(protocol);
    Assert.assertEquals(res, resCopy);
  }

  /**
   * Returns a dummy list of KeyValue
   *
   * @param n - the number of kvs in the resulting list
   * @return
   */
  public List<KeyValue> constuctKvList(int n) {
    List<KeyValue> list = new ArrayList<KeyValue>();
    for (int i = 0; i < n; i++) {
      KeyValue kv = new KeyValue(Bytes.toBytes("myRow" + i),
          Bytes.toBytes("myCF"), Bytes.toBytes("myQualifier"), 12345L,
          Bytes.toBytes("myValue"));
      list.add(kv);
    }
    return list;
  }
}
