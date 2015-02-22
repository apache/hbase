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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

/**
 * Test setting values in the descriptor
 */
@Category(SmallTests.class)
public class TestTableDescriptor {
  final static Log LOG = LogFactory.getLog(TestTableDescriptor.class);

  @Test
  public void testPb() throws DeserializationException, IOException {
    HTableDescriptor htd = new HTableDescriptor(TableName.META_TABLE_NAME);
    final int v = 123;
    htd.setMaxFileSize(v);
    htd.setDurability(Durability.ASYNC_WAL);
    htd.setReadOnly(true);
    htd.setRegionReplication(2);
    TableDescriptor td = new TableDescriptor(htd);
    byte[] bytes = td.toByteArray();
    TableDescriptor deserializedTd = TableDescriptor.parseFrom(bytes);
    assertEquals(td, deserializedTd);
    assertEquals(td.getHTableDescriptor(), deserializedTd.getHTableDescriptor());
  }
}
