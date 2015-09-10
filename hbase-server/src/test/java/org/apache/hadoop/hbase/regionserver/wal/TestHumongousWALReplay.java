/**
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
package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.hbase.fs.layout.FsLayout;
import org.apache.hadoop.hbase.fs.layout.HierarchicalFsLayout;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, MediumTests.class})
public class TestHumongousWALReplay extends TestWALReplay {
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    FsLayout.setLayoutForTesting(HierarchicalFsLayout.get());
    TestWALReplay.setUpBeforeClass();
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    try {
      TestWALReplay.tearDownAfterClass();
    } finally {
      FsLayout.reset();
    }
  }
}
