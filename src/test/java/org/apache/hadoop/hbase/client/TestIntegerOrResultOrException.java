/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.IntegerOrResultOrException.Type;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestIntegerOrResultOrException {

  /**
   * This is testing the constructor of {@link IntegerOrResultOrException} which
   * is getting Object as a parameter. This is needed in case we are reading an
   * object which is serialized with hadoop serialization with VERSION_0 where
   * we are serializing Object, but we are expecting to work with
   * IntegerOrResultOrException
   */
  @Test
  public void testIntegerOrResultOrExceptionObjectConstructor() {
    // Object is an Integer
    Object obj1 =  new Integer(1);
    IntegerOrResultOrException value = new IntegerOrResultOrException(obj1);
    assertEquals(value.getType(), Type.INTEGER);
    assertEquals(obj1, value.getInteger());
    assertEquals(null, value.getEx());
    assertEquals(null, value.getResults());

    //Object is an Exception
    Object obj2 = new Exception("dummy exception");
    value = new IntegerOrResultOrException(obj2);
    assertEquals(value.getType(), Type.EXCEPTION);
    assertEquals(((Exception) obj2).getMessage(), value.getEx().getServerJavaException().getMessage());
    assertEquals(null, value.getInteger());
    assertEquals(null, value.getResults());

    //Object is Result[]
    KeyValue[] kvs = new KeyValue[1];
    KeyValue kv = new KeyValue(Bytes.toBytes("myRow"), Bytes.toBytes("myCF"),
        Bytes.toBytes("myQualifier"), 12345L, Bytes.toBytes("myValue"));
    kvs[0] = kv;
    Result[] res = new Result[1];
    res[0] = new Result(kvs);
    Object obj3 = res;
    value = new IntegerOrResultOrException(obj3);
    assertEquals(value.getType(), Type.LIST_OF_RESULTS);
    List<Result> expected = new ArrayList<Result>();
    Collections.addAll(expected, new Result(kvs));
    assertEquals(expected, value.getResults());
    assertEquals(null, value.getEx());
    assertEquals(null, value.getInteger());
  }

}
