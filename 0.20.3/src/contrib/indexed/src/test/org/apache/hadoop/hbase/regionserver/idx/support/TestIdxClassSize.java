/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver.idx.support;

import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.ObjectArrayList;
import org.apache.hadoop.hbase.util.ClassSize;

import java.util.HashMap;

public class TestIdxClassSize extends HBaseTestCase {

  /**
   * Tests that the class sizes matches the estimate.
   */
  public void testClassSizes() {
    assertEquals(IdxClassSize.HASHMAP,
      ClassSize.estimateBase(HashMap.class, false));

    assertEquals(IdxClassSize.OBJECT_ARRAY_LIST,
      ClassSize.estimateBase(ObjectArrayList.class, false));

  }
}
