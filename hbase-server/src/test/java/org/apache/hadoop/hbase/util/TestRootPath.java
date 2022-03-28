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

import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test requirement that root directory must be a URI
 */
@Category({MiscTests.class, SmallTests.class})
public class TestRootPath {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRootPath.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRootPath.class);

  /** The test */
  @Test
  public void testRootPath() {
    try {
      // Try good path
      CommonFSUtils.validateRootPath(new Path("file:///tmp/hbase/hbase"));
    } catch (IOException e) {
      LOG.error(HBaseMarkers.FATAL, "Unexpected exception checking valid path:", e);
      fail();
    }
    try {
      // Try good path
      CommonFSUtils.validateRootPath(new Path("hdfs://a:9000/hbase"));
    } catch (IOException e) {
      LOG.error(HBaseMarkers.FATAL, "Unexpected exception checking valid path:", e);
      fail();
    }
    try {
      // bad path
      CommonFSUtils.validateRootPath(new Path("/hbase"));
      fail();
    } catch (IOException e) {
      // Expected.
      LOG.info("Got expected exception when checking invalid path:", e);
    }
  }

}

