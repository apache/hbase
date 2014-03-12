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
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestTMultiResponse {

  /**
   * Test if the {@link TMultiResponse} is correctly serialized and de-serialized
   * @throws Exception
   */
  @Test
  public void testThriftSerializeDeserialze() throws Exception {
    List<Map<byte[], IntegerOrResultOrException>> maps = new ArrayList<>();
    // create 3 dummy maps that should imitate the containers inside
    // MultiResponse: resultsForGet, resultsForPut and resultsForDelete
    for (int i = 0; i < 3; i++) {
      Map<byte[], IntegerOrResultOrException> map = new TreeMap<>(
          Bytes.BYTES_COMPARATOR);
      map.put(new byte[] { 1, 2, 3 }, new IntegerOrResultOrException(1));
      List<Result> results = new ArrayList<>();
      results.add(new Result(TestPut.createDummyKVs(Bytes.toBytes("row"))));
      map.put(new byte[] { 7, 8, 9 }, new IntegerOrResultOrException(results));
      map.put(new byte[] { 4, 5, 6 }, new IntegerOrResultOrException(
          new RuntimeException("runtime exception happened")));
      maps.add(map);
    }
    // construct the MultiResponse Object out of those maps
    TMultiResponse response = new TMultiResponse(maps.get(0), maps.get(1),
        maps.get(2));
    TMultiResponse responseCopy = Bytes.readThriftBytes(
        Bytes.writeThriftBytes(response, TMultiResponse.class),
        TMultiResponse.class);
    Assert.assertEquals(response, responseCopy);
  }

}
