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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ MapReduceTests.class, SmallTests.class})
public class TestWALInputFormat {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWALInputFormat.class);

  /**
   * Test the primitive start/end time filtering.
   */
  @Test
  public void testAddFile() {
    List<FileStatus> lfss = new ArrayList<>();
    LocatedFileStatus lfs = Mockito.mock(LocatedFileStatus.class);
    long now = System.currentTimeMillis();
    Mockito.when(lfs.getPath()).thenReturn(new Path("/name." + now));
    WALInputFormat.addFile(lfss, lfs, now, now);
    assertEquals(1, lfss.size());
    WALInputFormat.addFile(lfss, lfs, now - 1, now - 1);
    assertEquals(1, lfss.size());
    WALInputFormat.addFile(lfss, lfs, now - 2, now - 1);
    assertEquals(1, lfss.size());
    WALInputFormat.addFile(lfss, lfs, now - 2, now);
    assertEquals(2, lfss.size());
    WALInputFormat.addFile(lfss, lfs, Long.MIN_VALUE, now);
    assertEquals(3, lfss.size());
    WALInputFormat.addFile(lfss, lfs, Long.MIN_VALUE, Long.MAX_VALUE);
    assertEquals(4, lfss.size());
    WALInputFormat.addFile(lfss, lfs, now, now + 2);
    assertEquals(5, lfss.size());
    WALInputFormat.addFile(lfss, lfs, now + 1, now + 2);
    assertEquals(5, lfss.size());
    Mockito.when(lfs.getPath()).thenReturn(new Path("/name"));
    WALInputFormat.addFile(lfss, lfs, Long.MIN_VALUE, Long.MAX_VALUE);
    assertEquals(6, lfss.size());
    Mockito.when(lfs.getPath()).thenReturn(new Path("/name.123"));
    WALInputFormat.addFile(lfss, lfs, Long.MIN_VALUE, Long.MAX_VALUE);
    assertEquals(7, lfss.size());
    Mockito.when(lfs.getPath()).thenReturn(new Path("/name." + now + ".meta"));
    WALInputFormat.addFile(lfss, lfs, now, now);
    assertEquals(8, lfss.size());
  }
}
