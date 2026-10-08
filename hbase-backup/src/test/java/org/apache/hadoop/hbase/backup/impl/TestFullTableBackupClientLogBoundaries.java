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
package org.apache.hadoop.hbase.backup.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(SmallTests.TAG)
public class TestFullTableBackupClientLogBoundaries {

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private FileSystem fs;
  private Path walRootDir;

  @BeforeEach
  public void setUp() throws IOException {
    fs = TEST_UTIL.getTestFileSystem();
    walRootDir = TEST_UTIL.getDataTestDirOnTestFS("walRoot");
  }

  @AfterEach
  public void tearDown() throws IOException {
    fs.delete(walRootDir, true);
  }

  @Test
  public void testLiveServerWithoutRollResultIsCappedBelowItsOldestWAL() throws IOException {
    ServerName joined = ServerName.valueOf("joined", 16020, 1L);
    createWAL(walDir(joined), joined, 500);
    createWAL(walDir(joined), joined, 600);
    Path metaWAL =
      new Path(walDir(joined), walName(joined, 450) + AbstractFSWALProvider.META_WAL_PROVIDER_ID);
    fs.create(metaWAL).close();

    assertEquals(Map.of("joined:16020", 499L), computeLogBoundaries(Map.of(), Set.of(joined)));
  }

  @Test
  public void testDeadServerGetsItsNewestWAL() throws IOException {
    ServerName dead = ServerName.valueOf("dead", 16020, 2L);
    createWAL(new Path(walDir(dead).toString() + AbstractFSWALProvider.SPLITTING_EXT), dead, 300);
    createWAL(new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME), dead, 350);

    assertEquals(Map.of("dead:16020", 350L), computeLogBoundaries(Map.of(), Set.of()));
  }

  @Test
  public void testRestartedServerCoversOldInstanceOnly() throws IOException {
    ServerName oldInstance = ServerName.valueOf("restarted", 16020, 3L);
    ServerName newInstance = ServerName.valueOf("restarted", 16020, 4L);
    createWAL(walDir(oldInstance), oldInstance, 700);
    createWAL(walDir(newInstance), newInstance, 800);

    assertEquals(Map.of("restarted:16020", 700L),
      computeLogBoundaries(Map.of(), Set.of(newInstance)));
  }

  @Test
  public void testRolledServerKeepsItsRollResult() throws IOException {
    ServerName rolled = ServerName.valueOf("rolled", 16020, 5L);
    createWAL(walDir(rolled), rolled, 900);
    createWAL(new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME), rolled, 950);

    assertEquals(Map.of("rolled:16020", 850L),
      computeLogBoundaries(Map.of("rolled:16020", 850L), Set.of(rolled)));
  }

  @Test
  public void testServerRegisteringAfterTheListingGetsNoBoundary() throws IOException {
    ServerName joined = ServerName.valueOf("joined", 16020, 6L);
    ServerName late = ServerName.valueOf("late", 16020, 7L);
    createWAL(walDir(joined), joined, 500);
    Admin admin = mock(Admin.class);
    when(admin.getRegionServers()).thenAnswer(invocation -> {
      createWAL(walDir(late), late, 600);
      return List.of(joined);
    });

    assertEquals(Map.of("joined:16020", 499L),
      FullTableBackupClient.computeLogBoundaries(fs, walRootDir, Map.of(), admin));
  }

  @Test
  public void testIncompleteLiveServerListFailsTheBackup() throws IOException {
    ServerName rolled = ServerName.valueOf("rolled", 16020, 8L);
    ServerName joined = ServerName.valueOf("joined", 16020, 9L);
    createWAL(walDir(rolled), rolled, 900);
    createWAL(walDir(joined), joined, 500);

    assertThrows(IOException.class,
      () -> computeLogBoundaries(Map.of("rolled:16020", 850L), Set.of(joined)));
  }

  private Map<String, Long> computeLogBoundaries(Map<String, Long> rolledHosts,
    Set<ServerName> liveServers) throws IOException {
    Admin admin = mock(Admin.class);
    when(admin.getRegionServers()).thenReturn(liveServers);
    return FullTableBackupClient.computeLogBoundaries(fs, walRootDir, rolledHosts, admin);
  }

  private Path walDir(ServerName serverName) {
    return new Path(walRootDir, AbstractFSWALProvider.getWALDirectoryName(serverName.toString()));
  }

  private void createWAL(Path dir, ServerName serverName, long ts) throws IOException {
    fs.mkdirs(dir);
    fs.create(new Path(dir, walName(serverName, ts))).close();
  }

  private static String walName(ServerName serverName, long ts) {
    return serverName.toString().replace(",", "%2C") + "." + ts;
  }
}
