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
package org.apache.hadoop.hbase.procedure2.store.region;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.util.ToolRunner;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, SmallTests.class })
public class TestWALProcedurePrettyPrinter extends RegionProcedureStoreTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWALProcedurePrettyPrinter.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestWALProcedurePrettyPrinter.class);

  @Test
  public void test() throws Exception {
    List<RegionProcedureStoreTestProcedure> procs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      RegionProcedureStoreTestProcedure proc = new RegionProcedureStoreTestProcedure();
      store.insert(proc, null);
      procs.add(proc);
    }
    store.region.flush(true);
    for (int i = 0; i < 5; i++) {
      store.delete(procs.get(i).getProcId());
    }
    store.cleanup();
    Path walParentDir = new Path(htu.getDataTestDir(),
      MasterRegionFactory.MASTER_STORE_DIR + "/" + HConstants.HREGION_LOGDIR_NAME);
    FileSystem fs = walParentDir.getFileSystem(htu.getConfiguration());
    Path walDir = fs.listStatus(walParentDir)[0].getPath();
    Path walFile = fs.listStatus(walDir)[0].getPath();
    store.region.requestRollAll();
    store.region.waitUntilWalRollFinished();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bos);
    WALProcedurePrettyPrinter printer = new WALProcedurePrettyPrinter(out);
    assertEquals(0, ToolRunner.run(htu.getConfiguration(), printer,
      new String[] { fs.makeQualified(walFile).toString() }));
    try (BufferedReader reader = new BufferedReader(
      new InputStreamReader(new ByteArrayInputStream(bos.toByteArray()), StandardCharsets.UTF_8))) {
      long inserted = 0;
      long markedDeleted = 0;
      long deleted = 0;
      for (;;) {
        String line = reader.readLine();
        LOG.info(line);
        if (line == null) {
          break;
        }
        if (line.startsWith("\t")) {
          if (line.startsWith("\tpid=")) {
            inserted++;
          } else {
            assertEquals("\tmark deleted", line);
            markedDeleted++;
          }
        } else if (line.contains("type=DeleteFamily")) {
          deleted++;
        }
      }
      assertEquals(10, inserted);
      assertEquals(5, markedDeleted);
      assertEquals(5, deleted);
    }
  }
}
