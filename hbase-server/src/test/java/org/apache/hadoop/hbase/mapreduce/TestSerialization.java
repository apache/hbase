/**
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
package org.apache.hadoop.hbase.mapreduce;


import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;

import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(SmallTests.class)
public class TestSerialization {
  @Rule public TestName name = new TestName();
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;
  private static final byte [] row = Bytes.toBytes("row1");
  private static final byte [] qualifier = Bytes.toBytes("qualifier1");
  private static final byte [] family = Bytes.toBytes("family1");
  private static final byte [] value = new byte[100 * 1024 * 1024];

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setInt("hbase.client.keyvalue.maxsize", Integer.MAX_VALUE);
  }

  @Test
  public void testLargeMutation()
  throws Exception {
    Put put = new Put(row);
    put.add(family, qualifier, value);

    MutationSerialization serialization = new MutationSerialization();
    Serializer<Mutation> serializer = serialization.getSerializer(Mutation.class);
    Deserializer<Mutation> deserializer = serialization.getDeserializer(Mutation.class);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    ByteArrayInputStream is = null;
    try {
      serializer.open(os);
      serializer.serialize(put);
      os.flush();
      is = new ByteArrayInputStream(os.toByteArray());
      deserializer.open(is);
      deserializer.deserialize(null);
    } catch (InvalidProtocolBufferException e) {
      assertTrue("Got InvalidProtocolBufferException in " + name.getMethodName(),
        e.getCause() instanceof InvalidProtocolBufferException);
    } catch (Exception e) {
      fail("Got an invalid exception: " + e);
    }
  }

  @Test
  public void testLargeResult()
  throws Exception {
    Result res = Result.create(new KeyValue[] {new KeyValue(row, family, qualifier, 0L, value)});

    ResultSerialization serialization = new ResultSerialization();
    Serializer<Result> serializer = serialization.getSerializer(Result.class);
    Deserializer<Result> deserializer = serialization.getDeserializer(Result.class);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    ByteArrayInputStream is = null;
    try {
      serializer.open(os);
      serializer.serialize(res);
      os.flush();
      is = new ByteArrayInputStream(os.toByteArray());
      deserializer.open(is);
      deserializer.deserialize(null);
    } catch (InvalidProtocolBufferException e) {
      assertTrue("Got InvalidProtocolBufferException in " + name.getMethodName(),
        e.getCause() instanceof InvalidProtocolBufferException);
    } catch (Exception e) {
      fail("Got an invalid exception: " + e);
    }
  }
}
