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
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 * Testcase for ThriftResultSerializer and ThriftResultSerializer2
 */
public class TestThriftResultSerializer {

  private KeyValue[] constuctKvList(int n) {
    List<KeyValue> list = new ArrayList<KeyValue>();
    for (int i = 0; i < n; i++) {
      KeyValue kv = new KeyValue(Bytes.toBytes("myRow" + i),
          Bytes.toBytes("myCF"), Bytes.toBytes("myQualifier"), 12345L,
          Bytes.toBytes("myValue"));
      list.add(kv);
    }
    return list.toArray(new KeyValue[list.size()]);
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

  private static String findThriftPath() {
    String[] paths = System.getProperty("java.class.path").split(
        File.pathSeparator);
    for (String path : paths) {
      if (path.contains("/libthrift-")) {
        return path;
      }
    }
    return "";
  }

  @Test
  public void testWithClassPath() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    ThriftResultSerializer2 trs = new ThriftResultSerializer2();

    String cp = findThriftPath();
    System.out.println("Cutting classpath: " + cp);

    Configuration conf = HBaseConfiguration.create();
    conf.set(ThriftResultSerializer2.CUTTING_CLASSPATH_KEY, cp);
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
}
