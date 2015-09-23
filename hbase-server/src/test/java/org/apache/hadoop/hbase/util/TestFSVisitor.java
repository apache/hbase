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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.UUID;
import java.util.Set;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.junit.*;
import org.junit.experimental.categories.Category;

/**
 * Test {@link FSUtils}.
 */
@Category(MediumTests.class)
public class TestFSVisitor {
  private static final Log LOG = LogFactory.getLog(TestFSVisitor.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final String TABLE_NAME = "testtb";

  private Set<String> tableFamilies;
  private Set<String> tableRegions;
  private Set<String> tableHFiles;

  private FileSystem fs;
  private Path tableDir;
  private Path rootDir;

  @Before
  public void setUp() throws Exception {
    fs = FileSystem.get(TEST_UTIL.getConfiguration());
    rootDir = TEST_UTIL.getDataTestDir("hbase");

    tableFamilies = new HashSet<String>();
    tableRegions = new HashSet<String>();
    tableHFiles = new HashSet<String>();
    tableDir = createTableFiles(rootDir, TABLE_NAME, tableRegions, tableFamilies, tableHFiles);
    FSUtils.logFileSystemState(fs, rootDir, LOG);
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(rootDir, true);
  }

  @Test
  public void testVisitStoreFiles() throws IOException {
    final Set<String> regions = new HashSet<String>();
    final Set<String> families = new HashSet<String>();
    final Set<String> hfiles = new HashSet<String>();
    FSVisitor.visitTableStoreFiles(fs, tableDir, new FSVisitor.StoreFileVisitor() {
      public void storeFile(final String region, final String family, final String hfileName)
          throws IOException {
        regions.add(region);
        families.add(family);
        hfiles.add(hfileName);
      }
    });
    assertEquals(tableRegions, regions);
    assertEquals(tableFamilies, families);
    assertEquals(tableHFiles, hfiles);
  }

  /*
   * |-testtb/
   * |----f1d3ff8443297732862df21dc4e57262/
   * |-------f1/
   * |----------d0be84935ba84b66b1e866752ec5d663
   * |----------9fc9d481718f4878b29aad0a597ecb94
   * |-------f2/
   * |----------4b0fe6068c564737946bcf4fd4ab8ae1
   */
  private Path createTableFiles(final Path rootDir, final String tableName,
      final Set<String> tableRegions, final Set<String> tableFamilies,
      final Set<String> tableHFiles) throws IOException {
    Path tableDir = new Path(rootDir, tableName);
    for (int r = 0; r < 10; ++r) {
      String regionName = MD5Hash.getMD5AsHex(Bytes.toBytes(r));
      tableRegions.add(regionName);
      Path regionDir = new Path(tableDir, regionName);
      for (int f = 0; f < 3; ++f) {
        String familyName = "f" + f;
        tableFamilies.add(familyName);
        Path familyDir = new Path(regionDir, familyName);
        fs.mkdirs(familyDir);
        for (int h = 0; h < 5; ++h) {
         String hfileName = UUID.randomUUID().toString().replaceAll("-", "");
         tableHFiles.add(hfileName);
         fs.createNewFile(new Path(familyDir, hfileName));
        }
      }
    }
    return tableDir;
  }
}
