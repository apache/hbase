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
package org.apache.hadoop.hbase.replication.master;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyIterable;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerManager;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationQueueData;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

@Category({ MasterTests.class, SmallTests.class })
public class TestReplicationLogCleaner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationLogCleaner.class);

  private static final Configuration CONF = HBaseConfiguration.create();

  private MasterServices services;

  private ReplicationLogCleaner cleaner;

  private ReplicationPeerManager rpm;

  @Before
  public void setUp() throws ReplicationException {
    services = mock(MasterServices.class);
    when(services.getReplicationLogCleanerBarrier()).thenReturn(new ReplicationLogCleanerBarrier());
    AsyncClusterConnection asyncClusterConnection = mock(AsyncClusterConnection.class);
    when(services.getAsyncClusterConnection()).thenReturn(asyncClusterConnection);
    when(asyncClusterConnection.isClosed()).thenReturn(false);
    rpm = mock(ReplicationPeerManager.class);
    when(services.getReplicationPeerManager()).thenReturn(rpm);
    when(rpm.listPeers(null)).thenReturn(new ArrayList<>());
    ReplicationQueueStorage rqs = mock(ReplicationQueueStorage.class);
    when(rpm.getQueueStorage()).thenReturn(rqs);
    when(rqs.hasData()).thenReturn(true);
    when(rqs.listAllQueues()).thenReturn(new ArrayList<>());
    ServerManager sm = mock(ServerManager.class);
    when(services.getServerManager()).thenReturn(sm);
    when(sm.getOnlineServersList()).thenReturn(new ArrayList<>());
    @SuppressWarnings("unchecked")
    ProcedureExecutor<MasterProcedureEnv> procExec = mock(ProcedureExecutor.class);
    when(services.getMasterProcedureExecutor()).thenReturn(procExec);
    when(procExec.getProcedures()).thenReturn(new ArrayList<>());

    cleaner = new ReplicationLogCleaner();
    cleaner.setConf(CONF);
    Map<String, Object> params = ImmutableMap.of(HMaster.MASTER, services);
    cleaner.init(params);
  }

  @After
  public void tearDown() {
    cleaner.postClean();
  }

  private static Iterable<FileStatus> runCleaner(ReplicationLogCleaner cleaner,
    Iterable<FileStatus> files) {
    cleaner.preClean();
    return cleaner.getDeletableFiles(files);
  }

  private static FileStatus createFileStatus(Path path) {
    return new FileStatus(100, false, 3, 256, EnvironmentEdgeManager.currentTime(), path);
  }

  private static FileStatus createFileStatus(ServerName sn, int number) {
    Path path = new Path(sn.toString() + "." + number);
    return createFileStatus(path);
  }

  private static ReplicationPeerDescription createPeer(String peerId) {
    return new ReplicationPeerDescription(peerId, true, null, null, null);
  }

  private void addServer(ServerName serverName) {
    services.getServerManager().getOnlineServersList().add(serverName);
  }

  private void addSCP(ServerName serverName, boolean finished) {
    ServerCrashProcedure scp = mock(ServerCrashProcedure.class);
    when(scp.getServerName()).thenReturn(serverName);
    when(scp.isFinished()).thenReturn(finished);
    services.getMasterProcedureExecutor().getProcedures().add(scp);
  }

  private void addPeer(String... peerIds) {
    services.getReplicationPeerManager().listPeers(null).addAll(
      Stream.of(peerIds).map(TestReplicationLogCleaner::createPeer).collect(Collectors.toList()));
  }

  private void addQueueData(ReplicationQueueData... datas) throws ReplicationException {
    services.getReplicationPeerManager().getQueueStorage().listAllQueues()
      .addAll(Arrays.asList(datas));
  }

  @Test
  public void testNoConf() {
    ReplicationLogCleaner cleaner = new ReplicationLogCleaner();
    List<FileStatus> files = Arrays.asList(new FileStatus());
    assertSame(files, runCleaner(cleaner, files));
    cleaner.postClean();
  }

  @Test
  public void testCanNotFilter() {
    assertTrue(services.getReplicationLogCleanerBarrier().disable());
    List<FileStatus> files = Arrays.asList(new FileStatus());
    assertSame(Collections.emptyList(), runCleaner(cleaner, files));
  }

  @Test
  public void testNoPeer() {
    Path path = new Path("/wal." + EnvironmentEdgeManager.currentTime());
    assertTrue(AbstractFSWALProvider.validateWALFilename(path.getName()));
    FileStatus file = createFileStatus(path);
    Iterator<FileStatus> iter = runCleaner(cleaner, Arrays.asList(file)).iterator();
    assertSame(file, iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  public void testNotValidWalFile() {
    addPeer("1");
    Path path = new Path("/whatever");
    assertFalse(AbstractFSWALProvider.validateWALFilename(path.getName()));
    FileStatus file = createFileStatus(path);
    Iterator<FileStatus> iter = runCleaner(cleaner, Arrays.asList(file)).iterator();
    assertSame(file, iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  public void testMetaWalFile() {
    addPeer("1");
    Path path = new Path(
      "/wal." + EnvironmentEdgeManager.currentTime() + AbstractFSWALProvider.META_WAL_PROVIDER_ID);
    assertTrue(AbstractFSWALProvider.validateWALFilename(path.getName()));
    assertTrue(AbstractFSWALProvider.isMetaFile(path));
    FileStatus file = createFileStatus(path);
    Iterator<FileStatus> iter = runCleaner(cleaner, Arrays.asList(file)).iterator();
    assertSame(file, iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  public void testLiveRegionServerNoQueues() {
    addPeer("1");
    ServerName sn = ServerName.valueOf("server,123," + EnvironmentEdgeManager.currentTime());
    addServer(sn);
    List<FileStatus> files = Arrays.asList(createFileStatus(sn, 1));
    assertThat(runCleaner(cleaner, files), emptyIterable());
  }

  @Test
  public void testLiveRegionServerWithSCPNoQueues() {
    addPeer("1");
    ServerName sn = ServerName.valueOf("server,123," + EnvironmentEdgeManager.currentTime());
    addSCP(sn, false);
    List<FileStatus> files = Arrays.asList(createFileStatus(sn, 1));
    assertThat(runCleaner(cleaner, files), emptyIterable());
  }

  @Test
  public void testDeadRegionServerNoQueues() {
    addPeer("1");
    ServerName sn = ServerName.valueOf("server,123," + EnvironmentEdgeManager.currentTime());
    FileStatus file = createFileStatus(sn, 1);
    Iterator<FileStatus> iter = runCleaner(cleaner, Arrays.asList(file)).iterator();
    assertSame(file, iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  public void testDeadRegionServerWithSCPNoQueues() {
    addPeer("1");
    ServerName sn = ServerName.valueOf("server,123," + EnvironmentEdgeManager.currentTime());
    addSCP(sn, true);
    FileStatus file = createFileStatus(sn, 1);
    Iterator<FileStatus> iter = runCleaner(cleaner, Arrays.asList(file)).iterator();
    assertSame(file, iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  public void testLiveRegionServerMissingQueue() throws ReplicationException {
    String peerId1 = "1";
    String peerId2 = "2";
    addPeer(peerId1, peerId2);
    ServerName sn = ServerName.valueOf("server,123," + EnvironmentEdgeManager.currentTime());
    addServer(sn);
    FileStatus file = createFileStatus(sn, 1);
    ReplicationQueueData data1 = new ReplicationQueueData(new ReplicationQueueId(sn, peerId1),
      ImmutableMap.of(sn.toString(), new ReplicationGroupOffset(file.getPath().getName(), -1)));
    addQueueData(data1);
    assertThat(runCleaner(cleaner, Arrays.asList(file)), emptyIterable());
  }

  @Test
  public void testLiveRegionServerShouldNotDelete() throws ReplicationException {
    String peerId = "1";
    addPeer(peerId);
    ServerName sn = ServerName.valueOf("server,123," + EnvironmentEdgeManager.currentTime());
    addServer(sn);
    FileStatus file = createFileStatus(sn, 1);
    ReplicationQueueData data = new ReplicationQueueData(new ReplicationQueueId(sn, peerId),
      ImmutableMap.of(sn.toString(), new ReplicationGroupOffset(file.getPath().getName(), 0)));
    addQueueData(data);
    assertThat(runCleaner(cleaner, Arrays.asList(file)), emptyIterable());
  }

  @Test
  public void testLiveRegionServerShouldNotDeleteTwoPeers() throws ReplicationException {
    String peerId1 = "1";
    String peerId2 = "2";
    addPeer(peerId1, peerId2);
    ServerName sn = ServerName.valueOf("server,123," + EnvironmentEdgeManager.currentTime());
    addServer(sn);
    FileStatus file = createFileStatus(sn, 1);
    ReplicationQueueData data1 = new ReplicationQueueData(new ReplicationQueueId(sn, peerId1),
      ImmutableMap.of(sn.toString(), new ReplicationGroupOffset(file.getPath().getName(), -1)));
    ReplicationQueueData data2 = new ReplicationQueueData(new ReplicationQueueId(sn, peerId2),
      ImmutableMap.of(sn.toString(), new ReplicationGroupOffset(file.getPath().getName(), 0)));
    addQueueData(data1, data2);
    assertThat(runCleaner(cleaner, Arrays.asList(file)), emptyIterable());
  }

  @Test
  public void testLiveRegionServerShouldDelete() throws ReplicationException {
    String peerId = "1";
    addPeer(peerId);
    ServerName sn = ServerName.valueOf("server,123," + EnvironmentEdgeManager.currentTime());
    addServer(sn);
    FileStatus file = createFileStatus(sn, 1);
    ReplicationQueueData data = new ReplicationQueueData(new ReplicationQueueId(sn, peerId),
      ImmutableMap.of(sn.toString(), new ReplicationGroupOffset(file.getPath().getName(), -1)));
    services.getReplicationPeerManager().getQueueStorage().listAllQueues().add(data);
    Iterator<FileStatus> iter = runCleaner(cleaner, Arrays.asList(file)).iterator();
    assertSame(file, iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  public void testLiveRegionServerShouldDeleteTwoPeers() throws ReplicationException {
    String peerId1 = "1";
    String peerId2 = "2";
    addPeer(peerId1, peerId2);
    ServerName sn = ServerName.valueOf("server,123," + EnvironmentEdgeManager.currentTime());
    addServer(sn);
    FileStatus file = createFileStatus(sn, 1);
    ReplicationQueueData data1 = new ReplicationQueueData(new ReplicationQueueId(sn, peerId1),
      ImmutableMap.of(sn.toString(), new ReplicationGroupOffset(file.getPath().getName(), -1)));
    ReplicationQueueData data2 = new ReplicationQueueData(new ReplicationQueueId(sn, peerId2),
      ImmutableMap.of(sn.toString(), new ReplicationGroupOffset(file.getPath().getName(), -1)));
    addQueueData(data1, data2);
    Iterator<FileStatus> iter = runCleaner(cleaner, Arrays.asList(file)).iterator();
    assertSame(file, iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  public void testDeadRegionServerMissingQueue() throws ReplicationException {
    String peerId1 = "1";
    String peerId2 = "2";
    addPeer(peerId1, peerId2);
    ServerName sn = ServerName.valueOf("server,123," + EnvironmentEdgeManager.currentTime());
    FileStatus file = createFileStatus(sn, 1);
    ReplicationQueueData data1 = new ReplicationQueueData(new ReplicationQueueId(sn, peerId1),
      ImmutableMap.of(sn.toString(), new ReplicationGroupOffset(file.getPath().getName(), -1)));
    addQueueData(data1);
    Iterator<FileStatus> iter = runCleaner(cleaner, Arrays.asList(file)).iterator();
    assertSame(file, iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  public void testDeadRegionServerShouldNotDelete() throws ReplicationException {
    String peerId = "1";
    addPeer(peerId);
    ServerName sn = ServerName.valueOf("server,123," + EnvironmentEdgeManager.currentTime());
    FileStatus file = createFileStatus(sn, 1);
    ReplicationQueueData data = new ReplicationQueueData(new ReplicationQueueId(sn, peerId),
      ImmutableMap.of(sn.toString(), new ReplicationGroupOffset(file.getPath().getName(), 0)));
    addQueueData(data);
    assertThat(runCleaner(cleaner, Arrays.asList(file)), emptyIterable());
  }

  @Test
  public void testDeadRegionServerShouldNotDeleteTwoPeers() throws ReplicationException {
    String peerId1 = "1";
    String peerId2 = "2";
    addPeer(peerId1, peerId2);
    ServerName sn = ServerName.valueOf("server,123," + EnvironmentEdgeManager.currentTime());
    FileStatus file = createFileStatus(sn, 1);
    ReplicationQueueData data1 = new ReplicationQueueData(new ReplicationQueueId(sn, peerId1),
      ImmutableMap.of(sn.toString(), new ReplicationGroupOffset(file.getPath().getName(), -1)));
    ReplicationQueueData data2 = new ReplicationQueueData(new ReplicationQueueId(sn, peerId2),
      ImmutableMap.of(sn.toString(), new ReplicationGroupOffset(file.getPath().getName(), 0)));
    addQueueData(data1, data2);
    assertThat(runCleaner(cleaner, Arrays.asList(file)), emptyIterable());
  }

  @Test
  public void testDeadRegionServerShouldDelete() throws ReplicationException {
    String peerId = "1";
    addPeer(peerId);
    ServerName sn = ServerName.valueOf("server,123," + EnvironmentEdgeManager.currentTime());
    FileStatus file = createFileStatus(sn, 1);
    ReplicationQueueData data = new ReplicationQueueData(new ReplicationQueueId(sn, peerId),
      ImmutableMap.of(sn.toString(), new ReplicationGroupOffset(file.getPath().getName(), -1)));
    services.getReplicationPeerManager().getQueueStorage().listAllQueues().add(data);
    Iterator<FileStatus> iter = runCleaner(cleaner, Arrays.asList(file)).iterator();
    assertSame(file, iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  public void testDeadRegionServerShouldDeleteTwoPeers() throws ReplicationException {
    String peerId1 = "1";
    String peerId2 = "2";
    addPeer(peerId1, peerId2);
    ServerName sn = ServerName.valueOf("server,123," + EnvironmentEdgeManager.currentTime());
    FileStatus file = createFileStatus(sn, 1);
    ReplicationQueueData data1 = new ReplicationQueueData(new ReplicationQueueId(sn, peerId1),
      ImmutableMap.of(sn.toString(), new ReplicationGroupOffset(file.getPath().getName(), -1)));
    ReplicationQueueData data2 = new ReplicationQueueData(new ReplicationQueueId(sn, peerId2),
      ImmutableMap.of(sn.toString(), new ReplicationGroupOffset(file.getPath().getName(), -1)));
    addQueueData(data1, data2);
    Iterator<FileStatus> iter = runCleaner(cleaner, Arrays.asList(file)).iterator();
    assertSame(file, iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  public void testPreCleanWhenAsyncClusterConnectionClosed() throws ReplicationException {
    assertFalse(services.getAsyncClusterConnection().isClosed());
    verify(services.getAsyncClusterConnection(), Mockito.times(1)).isClosed();
    cleaner.preClean();
    verify(services.getAsyncClusterConnection(), Mockito.times(2)).isClosed();
    verify(rpm.getQueueStorage(), Mockito.times(1)).hasData();

    when(services.getAsyncClusterConnection().isClosed()).thenReturn(true);
    assertTrue(services.getAsyncClusterConnection().isClosed());
    verify(services.getAsyncClusterConnection(), Mockito.times(3)).isClosed();
    cleaner.preClean();
    verify(services.getAsyncClusterConnection(), Mockito.times(4)).isClosed();
    // rpm.getQueueStorage().hasData() was not executed, indicating an early return.
    verify(rpm.getQueueStorage(), Mockito.times(1)).hasData();
  }

  @Test
  public void testGetDeletableFilesWhenAsyncClusterConnectionClosed() throws ReplicationException {
    List<FileStatus> files = List.of(new FileStatus());
    assertFalse(services.getAsyncClusterConnection().isClosed());
    verify(services.getAsyncClusterConnection(), Mockito.times(1)).isClosed();
    cleaner.getDeletableFiles(files);
    verify(services.getAsyncClusterConnection(), Mockito.times(2)).isClosed();
    verify(rpm.getQueueStorage(), Mockito.times(1)).hasData();

    when(services.getAsyncClusterConnection().isClosed()).thenReturn(true);
    assertTrue(services.getAsyncClusterConnection().isClosed());
    verify(services.getAsyncClusterConnection(), Mockito.times(3)).isClosed();
    cleaner.getDeletableFiles(files);
    verify(services.getAsyncClusterConnection(), Mockito.times(4)).isClosed();
    // rpm.getQueueStorage().hasData() was not executed, indicating an early return.
    verify(rpm.getQueueStorage(), Mockito.times(1)).hasData();
  }
}
