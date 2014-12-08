/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameters;

/**
 * Runs a load test on a mini HBase cluster with data block encoding turned on.
 * Compared to other load-test-style unit tests, this one writes a smaller
 * amount of data, but goes through all available data block encoding
 * algorithms.
 */
@Category(LargeTests.class)
public class TestMiniClusterLoadEncoded extends TestMiniClusterLoadParallel {

  /** We do not alternate the multi-put flag in this test. */
  private static final boolean USE_MULTI_PUT = true;

  @Parameters
  public static Collection<Object[]> parameters() {
    List<Object[]> parameters = new ArrayList<Object[]>();
    for (DataBlockEncoding dataBlockEncoding : DataBlockEncoding.values() ) {
      parameters.add(new Object[]{dataBlockEncoding});
    }
    return parameters;
  }

  public TestMiniClusterLoadEncoded(DataBlockEncoding encoding) {
    super(USE_MULTI_PUT, encoding);
  }
}
