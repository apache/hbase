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
package org.apache.hadoop.hbase.replication;

import org.junit.BeforeClass;
import org.junit.jupiter.api.BeforeAll;

/**
 * This class is only a base for other integration-level replication tests. Do not add tests here.
 * TestReplicationSmallTests is where tests that don't require bring machines up/down should go All
 * other tests should have their own classes and extend this one
 */
public class TestReplicationBase extends TestReplicationBaseNoBeforeAll {

  @BeforeAll
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    configureClusters(UTIL1, UTIL2);
    startClusters();
  }
}
