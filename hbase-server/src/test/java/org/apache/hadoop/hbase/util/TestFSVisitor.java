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
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hadoop.hbase.fs.layout.FsLayout;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.*;
import org.junit.experimental.categories.Category;

/**
 * Test {@link FSUtils}.
 */
@Category({MiscTests.class, MediumTests.class})
public class TestFSVisitor {
  private static final Log LOG = LogFactory.getLog(TestFSVisitor.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final String TABLE_NAME = "testtb";

  private Set<String> tableFamilies;
  private Set<String> tableRegions;
  private Set<String> recoveredEdits;
  private Set<String> tableHFiles;
  private Set<String> regionServers;
  private Set<String> serverLogs;

  private FileSystem fs;
  private Path tableDir;
  private Path logsDir;
  private Path rootDir;

  @Before
  public void setUp() throws Exception {
    fs = FileSystem.get(TEST_UTIL.getConfiguration());
    rootDir = TEST_UTIL.getDataTestDir("hbase");
    logsDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);

    tableFamilies = new HashSet<String>();
    tableRegions = new HashSet<String>();
    recoveredEdits = new HashSet<String>();
    tableHFiles = new HashSet<String>();
    regionServers = new HashSet<String>();
    serverLogs = new HashSet<String>();
    tableDir = createTableFiles(rootDir, TABLE_NAME, tableRegions, tableFamilies, tableHFiles);
    createRecoverEdits(tableDir, tableRegions, recoveredEdits);
    createLogs(logsDir, regionServers, serverLogs);
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

  @Test
  public void testVisitRecoveredEdits() throws IOException {
    final Set<String> regions = new HashSet<String>();
    final Set<String> edits = new HashSet<String>();
    FSVisitor.visitTableRecoveredEdits(fs, tableDir, new FSVisitor.RecoveredEditsVisitor() {
      public void recoveredEdits (final String region, final String logfile)
          throws IOException {
        regions.add(region);
        edits.add(logfile);
      }
    });
    assertEquals(tableRegions, regions);
    assertEquals(recoveredEdits, edits);
  }

  @Test
  public void testVisitLogFiles() throws IOException {
    final Set<String> servers = new HashSet<String>();
    final Set<String> logs = new HashSet<String>();
    FSVisitor.visitLogFiles(fs, rootDir, new FSVisitor.LogFileVisitor() {
      public void logFile (final String server, final String logfile) throws IOException {
        servers.add(server);
        logs.add(logfile);
      }
    });
    assertEquals(regionServers, servers);
    assertEquals(serverLogs, logs);
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
      Path regionDir = FsLayout.getRegionDir(tableDir, regionName);
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

  /*
   * |-testtb/
   * |----f1d3ff8443297732862df21dc4e57262/
   * |-------recovered.edits/
   * |----------0000001351969633479
   * |----------0000001351969633481
   */
  private void createRecoverEdits(final Path tableDir, final Set<String> tableRegions,
      final Set<String> recoverEdits) throws IOException {
    for (String region: tableRegions) {
      Path regionEditsDir = WALSplitter.getRegionDirRecoveredEditsDir(FsLayout.getRegionDir(tableDir, region));
      long seqId = System.currentTimeMillis();
      for (int i = 0; i < 3; ++i) {
        String editName = String.format("%019d", seqId + i);
        recoverEdits.add(editName);
        FSDataOutputStream stream = fs.create(new Path(regionEditsDir, editName));
        stream.write(Bytes.toBytes("test"));
        stream.close();
      }
    }
  }

  /*
   * Old style
   * |-.logs/
   * |----server5,5,1351969633508/
   * |-------server5,5,1351969633508.0
   * |----server6,6,1351969633512/
   * |-------server6,6,1351969633512.0
   * |-------server6,6,1351969633512.3
   * New style
   * |-.logs/
   * |----server3,5,1351969633508/
   * |-------server3,5,1351969633508.default.0
   * |----server4,6,1351969633512/
   * |-------server4,6,1351969633512.default.0
   * |-------server4,6,1351969633512.some_provider.3
   */
  private void createLogs(final Path logDir, final Set<String> servers,
      final Set<String> logs) throws IOException {
    for (int s = 0; s < 7; ++s) {
      String server = String.format("server%d,%d,%d", s, s, System.currentTimeMillis());
      servers.add(server);
      Path serverLogDir = new Path(logDir, server);
      if (s % 2 == 0) {
        if (s % 3 == 0) {
          server += ".default";
        } else {
          server += "." + s;
        }
      }
      fs.mkdirs(serverLogDir);
      for (int i = 0; i < 5; ++i) {
        String logfile = server + '.' + i;
        logs.add(logfile);
        FSDataOutputStream stream = fs.create(new Path(serverLogDir, logfile));
        stream.write(Bytes.toBytes("test"));
        stream.close();
      }
    }
  }
}
