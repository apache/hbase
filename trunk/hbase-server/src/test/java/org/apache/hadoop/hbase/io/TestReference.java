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
package org.apache.hadoop.hbase.io;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Reference tests that run on local fs.
 */
@Category(SmallTests.class)
public class TestReference {
  private final HBaseTestingUtility HTU = new HBaseTestingUtility();

  /**
   * See if we can parse a Reference that was written pre-0.96, i.e. a serialized Writable.
   * Exercises the code path that parses Writables.
   * @throws IOException
   */
  @Test
  public void testParsingWritableReference() throws IOException {
    // Read a Reference written w/ 0.94 out of the test data dir.
    final String datafile = System.getProperty("project.build.testSourceDirectory", "src/test") +
      File.separator + "data" + File.separator +
      "a6a6562b777440fd9c34885428f5cb61.21e75333ada3d5bafb34bb918f29576c";
    FileSystem fs = FileSystem.get(HTU.getConfiguration());
    Reference.read(fs, new Path(datafile));
  }
}