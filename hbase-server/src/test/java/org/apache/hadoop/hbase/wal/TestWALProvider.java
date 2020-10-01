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
package org.apache.hadoop.hbase.wal;

import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.Comparator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class})
public class TestWALProvider {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWALProvider.class);

  /**
   * Test start time comparator.
   */
  @Test
  public void testWALStartTimeComparator() throws IOException {
    Path metaPath1 = new Path("hdfs://localhost:59875/user/stack/test-data/" +
      "f4cb8ffa-6ff7-59a6-f167-6cc00f24899a/WALs/localhost,59908,1600304600425/" +
      "localhost%2C59908%2C1600304600425.meta.1600304604319.meta");
    Path metaPath2 = new Path("hdfs://localhost:59875/user/stack/test-data/" +
      "f4cb8ffa-6ff7-59a6-f167-6cc00f24899a/WALs/localhost,59908,1600304600425/" +
      "localhost%2C59908%2C1600304600425.meta.1600304604320.meta");
    Path path3 = new Path("hdfs://localhost:59875/user/stack/test-data/" +
      "f4cb8ffa-6ff7-59a6-f167-6cc00f24899a/WALs/localhost,59908,1600304600425/" +
      "localhost%2C59908%2C1600304600425.1600304604321");
    Path metaPath4 = new Path("hdfs://localhost:59875/user/stack/test-data/" +
      "f4cb8ffa-6ff7-59a6-f167-6cc00f24899a/WALs/localhost,59908,1600304600425/" +
      "localhost%2C59908%2C1600304600425.meta.1600304604321.meta");
    Comparator c = new AbstractFSWALProvider.WALStartTimeComparator();
    assertTrue(c.compare(metaPath1, metaPath1) == 0);
    assertTrue(c.compare(metaPath2, metaPath2) == 0);
    assertTrue(c.compare(metaPath1, metaPath2) < 0);
    assertTrue(c.compare(metaPath2, metaPath1) > 0);
    assertTrue(c.compare(metaPath2, path3) < 0);
    assertTrue(c.compare(path3, metaPath4) == 0);
  }
}
