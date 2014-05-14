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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMultiAction {
  /**
   * Test if the MultiResponse is correctly serialized and deserialized
   * @throws Exception
   */
  @Test
  public void testThriftSerializeDeserialze() throws Exception {
    // create a map of dummy byte array which maps to list of dummy gets
    Map<byte[], List<Get>> gets = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    gets.put(new byte[]{1,2,3}, TestGet.createDummyGets(7));

    // create a map of dummy byte array which maps to list of dummy puts
    Map<byte[], List<Put>> puts = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    puts.put(new byte[]{7,  8, 9}, TestPut.createDummyPuts(2));

    // create a map of dummy byte array which maps to list of dummy deletes
    Map<byte[], List<Delete>> deletes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    deletes.put(new byte[] {10, 11, 12}, TestDelete.createDummyDeletes(4));

    //construct the MultiAction object out of the previously created maps.
    MultiAction multiAction = new MultiAction(gets, puts, deletes);
    MultiAction multiActionCopy = Bytes.readThriftBytes(Bytes.writeThriftBytes(multiAction, MultiAction.class), MultiAction.class);

    Assert.assertEquals(multiAction, multiActionCopy);
  }
}
