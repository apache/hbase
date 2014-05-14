/**
 * Copyright 2014 The Apache Software Foundation
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
package org.apache.hadoop.hbase.thrift;

import java.io.ByteArrayOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcase for ThriftResultSerializer and ThriftResultSerializer2
 */
@Category(SmallTests.class)
public class TestThriftResultSerializer {

  private static KeyValue[] constuctKvList(int n) {
    KeyValue[] kvs = new KeyValue[n];
    for (int i = 0; i < n; i++) {
      KeyValue kv = new KeyValue(Bytes.toBytes("myRow" + i),
          Bytes.toBytes("myCF"), Bytes.toBytes("myQualifier"), 12345L,
          Bytes.toBytes("myValue"));
      kvs[i] = kv;
    }
    return kvs;
  }

  @Test
  public void testBasic() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    ThriftResultSerializer trs = new ThriftResultSerializer();

    Configuration conf = HBaseConfiguration.create();
    trs.setConf(conf);

    trs.open(out);
    try {
      trs.serialize(new ImmutableBytesWritable(Bytes.toBytes("row")));
      trs.serialize(new Result(constuctKvList(10)));
    } finally {
      trs.close();
    }

    Assert.assertTrue("output is empty", out.size() > 0);
  }

  @Test
  public void testWithClassPath() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    ThriftResultSerializer2 trs = new ThriftResultSerializer2();

    String cp = System.getProperty("java.class.path");
    System.out.println("Cutting classpath: " + cp);

    Configuration conf = HBaseConfiguration.create();
    conf.set(ThriftResultSerializer2.CUTTING_CLASSPATH_KEY, cp);
    conf.set(ThriftResultSerializer.PROTOCOL_CONF_KEY,
        "org.apache.thrift.protocol.TCompactProtocol");
    trs.setConf(conf);

    trs.open(out);
    try {
      trs.serialize(new ImmutableBytesWritable(Bytes.toBytes("row")));
      trs.serialize(new Result(constuctKvList(10)));
    } finally {
      trs.close();
    }

    Assert.assertTrue("output is empty", out.size() > 0);
  }

  public static void main(String[] args) throws Exception {
    System.out.println("java.class.path: "
        + System.getProperty("java.class.path"));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ThriftResultSerializer2 trs = new ThriftResultSerializer2();

    Configuration conf = HBaseConfiguration.create();
    if (args.length >= 1) {
      System.out.println("Setting "
          + ThriftResultSerializer2.CUTTING_CLASSPATH_KEY + " to " + args[0]);
      conf.set(ThriftResultSerializer2.CUTTING_CLASSPATH_KEY, args[0]);
    }
    if (args.length >= 2) {
      System.out.println("Setting " + ThriftResultSerializer.PROTOCOL_CONF_KEY
          + " to " + args[1]);
      conf.set(ThriftResultSerializer.PROTOCOL_CONF_KEY, args[1]);
    }
    trs.setConf(conf);

    trs.open(out);
    try {
      trs.serialize(new ImmutableBytesWritable(Bytes.toBytes("row")));
      trs.serialize(new Result(constuctKvList(10)));
    } finally {
      trs.close();
    }
  }
}
