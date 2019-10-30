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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.locks.ReentrantReadWriteLock;

@Category({ SmallTests.class })
public class TestIdReadWriteLockStrongRef {

  private static final Logger LOG = LoggerFactory.getLogger(TestIdReadWriteLockStrongRef.class);

  public IdReadWriteLockStrongRef<Long> idLock = new IdReadWriteLockStrongRef<>();

  @Test
  public void testGetLock() throws Exception {
    Long offset_1 = 1L;
    Long offset_2 = 2L;
    ReentrantReadWriteLock offsetLock_1 = idLock.getLock(offset_1);
    ReentrantReadWriteLock offsetLock_2 = idLock.getLock(offset_1);
    Assert.assertEquals(offsetLock_1,offsetLock_2);
    ReentrantReadWriteLock offsetLock_3 = idLock.getLock(offset_2);
    Assert.assertNotEquals(offsetLock_1,offsetLock_3);
  }

}

