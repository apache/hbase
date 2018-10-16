/**
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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.util.PoolMap.PoolType;
import org.junit.After;
import org.junit.Before;

public abstract class PoolMapTestBase {

  protected PoolMap<String, String> poolMap;

  protected static final int POOL_SIZE = 3;

  @Before
  public void setUp() throws Exception {
    this.poolMap = new PoolMap<>(getPoolType(), POOL_SIZE);
  }

  @After
  public void tearDown() throws Exception {
    this.poolMap.clear();
  }

  protected abstract PoolType getPoolType();

  protected void runThread(final String randomKey, final String randomValue,
      final String expectedValue) throws InterruptedException {
    final AtomicBoolean matchFound = new AtomicBoolean(false);
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        poolMap.put(randomKey, randomValue);
        String actualValue = poolMap.get(randomKey);
        matchFound
            .set(expectedValue == null ? actualValue == null : expectedValue.equals(actualValue));
      }
    });
    thread.start();
    thread.join();
    assertTrue(matchFound.get());
  }
}
