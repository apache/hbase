/*
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

package org.apache.hadoop.hbase.regionserver;

import junit.framework.TestCase;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * Tests the {@link org.apache.hadoop.hbase.regionserver.IdxRegionMBeanImpl} class.
 */
public class TestIdxRegionMBeanImpl extends TestCase {
  /**
   * Ensures that the special bytes potentially contained in the start and end
   * rows are encoded.
   */
  public void testGenerateObjectNameWithInvalidValueInKey() {
    HRegionInfo info = new HRegionInfo(
        new HTableDescriptor("foo"),
        new byte[] { '"' },
        new byte[] { 0, ',' }
    );
    IdxRegionMBeanImpl.generateObjectName(info);
  }

  /**
   * Ensures that the HTableDescriptor doesn't allow special chars in the table
   * name.  This is redundant but it's here just incase the HTableDescriptor
   * changes.
   */
  public void testGenerateObjectNameWithInvalidValueName() {
    try {
      HRegionInfo info = new HRegionInfo(
          new HTableDescriptor("foo,%="),
          new byte[] { '"' },
          new byte[] { 0, ',' }
      );
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}