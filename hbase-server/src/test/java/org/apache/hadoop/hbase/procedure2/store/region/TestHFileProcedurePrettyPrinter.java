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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, SmallTests.class })
public class TestHFileProcedurePrettyPrinter extends RegionProcedureStoreTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHFileProcedurePrettyPrinter.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHFileProcedurePrettyPrinter.class);

  private List<String> checkOutput(BufferedReader reader, MutableLong putCount,
    MutableLong deleteCount, MutableLong markDeletedCount) throws IOException {
    putCount.setValue(0);
    deleteCount.setValue(0);
    markDeletedCount.setValue(0);
    List<String> fileScanned = new ArrayList<>();
    for (;;) {
      String line = reader.readLine();
      if (line == null) {
        return fileScanned;
      }
      LOG.info(line);
      if (line.contains("V: mark deleted")) {
        markDeletedCount.increment();
      } else if (line.contains("/Put/")) {
        putCount.increment();
      } else if (line.contains("/DeleteFamily/")) {
        deleteCount.increment();
      } else if (line.startsWith("Scanning -> ")) {
        fileScanned.add(line.split(" -> ")[1]);
      } else {
        fail("Unrecognized output: " + line);
      }
    }
  }

  @Test
  public void test() throws Exception {
    HFileProcedurePrettyPrinter printer = new HFileProcedurePrettyPrinter();
    // -a or -f is required so passing empty args will cause an error and return a non-zero value.
    assertNotEquals(0, ToolRunner.run(htu.getConfiguration(), printer, new String[0]));
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
    store.region.flush(true);
    store.cleanup();
    store.region.flush(true);
    Path tableDir = CommonFSUtils.getTableDir(
      new Path(htu.getDataTestDir(), MasterRegionFactory.MASTER_STORE_DIR), MasterRegionFactory.TABLE_NAME);
    FileSystem fs = tableDir.getFileSystem(htu.getConfiguration());
    Path regionDir =
      fs.listStatus(tableDir, p -> RegionInfo.isEncodedRegionName(Bytes.toBytes(p.getName())))[0]
        .getPath();
    List<Path> storefiles = HFile.getStoreFiles(fs, regionDir);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bos);
    MutableLong putCount = new MutableLong();
    MutableLong deleteCount = new MutableLong();
    MutableLong markDeletedCount = new MutableLong();
    for (Path file : storefiles) {
      bos.reset();
      printer = new HFileProcedurePrettyPrinter(out);
      assertEquals(0,
        ToolRunner.run(htu.getConfiguration(), printer, new String[] { "-f", file.toString() }));
      try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bos.toByteArray()),
          StandardCharsets.UTF_8))) {
        List<String> fileScanned = checkOutput(reader, putCount, deleteCount, markDeletedCount);
        assertEquals(1, fileScanned.size());
        assertEquals(file.toString(), fileScanned.get(0));
        if (putCount.longValue() == 10) {
          assertEquals(0, deleteCount.longValue());
          assertEquals(0, markDeletedCount.longValue());
        } else if (deleteCount.longValue() == 5) {
          assertEquals(0, putCount.longValue());
          assertEquals(0, markDeletedCount.longValue());
        } else if (markDeletedCount.longValue() == 5) {
          assertEquals(0, putCount.longValue());
          assertEquals(0, deleteCount.longValue());
        } else {
          fail("Should have entered one of the above 3 branches");
        }
      }
    }
    bos.reset();
    printer = new HFileProcedurePrettyPrinter(out);
    assertEquals(0, ToolRunner.run(htu.getConfiguration(), printer, new String[] { "-a" }));
    try (BufferedReader reader = new BufferedReader(
      new InputStreamReader(new ByteArrayInputStream(bos.toByteArray()), StandardCharsets.UTF_8))) {
      List<String> fileScanned = checkOutput(reader, putCount, deleteCount, markDeletedCount);
      assertEquals(3, fileScanned.size());
      assertEquals(10, putCount.longValue());
      assertEquals(5, deleteCount.longValue());
      assertEquals(5, markDeletedCount.longValue());
    }
  }
}
