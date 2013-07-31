/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.SmallTests;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for testing methods added in HBASE-9093. This set of tests is meant
 * to test the {@linkplain Mutation#setWriteToWal(boolean)}
 * and {@linkplain Mutation#getWriteToWal()} methods which provide
 * a compatibility layer with HBase versions < 95's client side WAL semantics.
 */
@Category(SmallTests.class)
public class TestPutWriteToWal {

  private Put put;
  @Before
  public void setUp() throws Exception {
    put = new Put("test".getBytes());
  }

  @Test
  public void testWriteToWal(){
    put.setWriteToWal(true);
    Assert.assertEquals(Durability.USE_DEFAULT, put.getDurability());
  }

  @Test
  public void testNoWriteToWal() {
    put.setWriteToWal(false);
    Assert.assertEquals(Durability.SKIP_WAL, put.getDurability());
  }

  @Test
  public void testWriteToWalSwitch() {
    put.setWriteToWal(false);
    Assert.assertEquals(Durability.SKIP_WAL, put.getDurability());
    put.setWriteToWal(true);
    Assert.assertEquals(Durability.USE_DEFAULT, put.getDurability());
  }

  @Test
  public void testPutCopy() {
    put.setWriteToWal(false);
    Put putCopy1 = new Put(put);
    Assert.assertEquals(Durability.SKIP_WAL, putCopy1.getDurability());

    put.setWriteToWal(true);
    Put putCopy2 = new Put(put);
    Assert.assertEquals(Durability.USE_DEFAULT, putCopy2.getDurability());
  }
}
