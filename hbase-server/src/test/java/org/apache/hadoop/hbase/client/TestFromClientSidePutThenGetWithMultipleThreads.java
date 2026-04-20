/*
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

import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

@Tag(LargeTests.TAG)
@Tag(ClientTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: registryImpl={0}, numHedgedReqs={1}")
public class TestFromClientSidePutThenGetWithMultipleThreads
  extends FromClientSideTestPutThenGetWithMultipleThreads {

  public TestFromClientSidePutThenGetWithMultipleThreads(
    Class<? extends ConnectionRegistry> registryImpl, int numHedgedReqs) {
    super(registryImpl, numHedgedReqs);
  }

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    initialize(MultiRowMutationEndpoint.class);
  }
}
