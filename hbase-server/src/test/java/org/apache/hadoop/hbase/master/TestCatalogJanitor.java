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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.util.HFileArchiveTestingUtil.assertArchiveEqualToOriginal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaMockingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.HConnectionTestingUtility;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coordination.BaseCoordinatedStateManager;
import org.apache.hadoop.hbase.coordination.SplitLogManagerCoordination;
import org.apache.hadoop.hbase.coordination.SplitLogManagerCoordination.SplitLogManagerDetails;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.fs.MasterStorage;
import org.apache.hadoop.hbase.master.CatalogJanitor.SplitParentFirstComparator;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionActionResult;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ResultOrException;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.Triple;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

@Category({MasterTests.class, SmallTests.class})
public class TestCatalogJanitor {
  private static final Log LOG = LogFactory.getLog(TestCatalogJanitor.class);

  /**
   * Mock MasterServices for tests below.
   */
  class MockMasterServices extends MockNoopMasterServices {
    private final ClusterConnection connection;
    private final MasterStorage mfs;
    private final AssignmentManager asm;
    private final ServerManager sm;

    MockMasterServices(final HBaseTestingUtility htu) throws IOException {
      super(htu.getConfiguration());

      ClientProtos.ClientService.BlockingInterface ri =
        Mockito.mock(ClientProtos.ClientService.BlockingInterface.class);
      MutateResponse.Builder builder = MutateResponse.newBuilder();
      builder.setProcessed(true);
      try {
        Mockito.when(ri.mutate(
          (RpcController)Mockito.any(), (MutateRequest)Mockito.any())).
            thenReturn(builder.build());
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      }
      try {
        Mockito.when(ri.multi(
          (RpcController)Mockito.any(), (MultiRequest)Mockito.any())).
            thenAnswer(new Answer<MultiResponse>() {
              @Override
              public MultiResponse answer(InvocationOnMock invocation) throws Throwable {
                return buildMultiResponse( (MultiRequest)invocation.getArguments()[1]);
              }
            });
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      }
      // Mock an ClusterConnection and a AdminProtocol implementation.  Have the
      // ClusterConnection return the HRI.  Have the HRI return a few mocked up responses
      // to make our test work.
      this.connection =
        HConnectionTestingUtility.getMockedConnectionAndDecorate(getConfiguration(),
          Mockito.mock(AdminProtos.AdminService.BlockingInterface.class), ri,
            ServerName.valueOf("example.org,12345,6789"),
          HRegionInfo.FIRST_META_REGIONINFO);
      // Set hbase.rootdir into test dir.
      FileSystem.get(getConfiguration());
      Path rootdir = FSUtils.getRootDir(getConfiguration());
      FSUtils.setRootDir(getConfiguration(), rootdir);
      Mockito.mock(AdminProtos.AdminService.BlockingInterface.class);

      this.mfs = new MasterStorage(this);
      this.asm = Mockito.mock(AssignmentManager.class);
      this.sm = Mockito.mock(ServerManager.class);
    }

    @Override
    public AssignmentManager getAssignmentManager() {
      return this.asm;
    }

    @Override
    public MasterStorage getMasterStorage() {
      return this.mfs;
    }

    @Override
    public ClusterConnection getConnection() {
      return this.connection;
    }

    @Override
    public ServerName getServerName() {
      return ServerName.valueOf("mockserver.example.org", 1234, -1L);
    }

    @Override
    public ServerManager getServerManager() {
      return this.sm;
    }

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
      BaseCoordinatedStateManager m = Mockito.mock(BaseCoordinatedStateManager.class);
      SplitLogManagerCoordination c = Mockito.mock(SplitLogManagerCoordination.class);
      Mockito.when(m.getSplitLogManagerCoordination()).thenReturn(c);
      SplitLogManagerDetails d = Mockito.mock(SplitLogManagerDetails.class);
      Mockito.when(c.getDetails()).thenReturn(d);
      return m;
    }

    @Override
    public TableDescriptors getTableDescriptors() {
      return new TableDescriptors() {
        @Override
        public HTableDescriptor remove(TableName tablename) throws IOException {
          // noop
          return null;
        }

        @Override
        public Map<String, HTableDescriptor> getAll() throws IOException {
          // noop
          return null;
        }

        @Override public Map<String, HTableDescriptor> getAllDescriptors() throws IOException {
          // noop
          return null;
        }

        @Override
        public HTableDescriptor get(TableName tablename)
            throws IOException {
          return createHTableDescriptor();
        }

        @Override
        public Map<String, HTableDescriptor> getByNamespace(String name) throws IOException {
          return null;
        }

        @Override
        public void add(HTableDescriptor htd) throws IOException {
          // noop
        }

        @Override
        public void setCacheOn() throws IOException {
        }

        @Override
        public void setCacheOff() throws IOException {
        }
      };
    }
  }

  @Test
  public void testCleanParent() throws IOException, InterruptedException {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    setRootDirAndCleanIt(htu, "testCleanParent");
    MasterServices services = new MockMasterServices(htu);
    try {
      CatalogJanitor janitor = new CatalogJanitor(services);
      // Create regions.
      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("table"));
      htd.addFamily(new HColumnDescriptor("f"));
      HRegionInfo parent =
        new HRegionInfo(htd.getTableName(), Bytes.toBytes("aaa"),
            Bytes.toBytes("eee"));
      HRegionInfo splita =
        new HRegionInfo(htd.getTableName(), Bytes.toBytes("aaa"),
            Bytes.toBytes("ccc"));
      HRegionInfo splitb =
        new HRegionInfo(htd.getTableName(), Bytes.toBytes("ccc"),
            Bytes.toBytes("eee"));
      // Test that when both daughter regions are in place, that we do not
      // remove the parent.
      Result r = createResult(parent, splita, splitb);
      // Add a reference under splitA directory so we don't clear out the parent.
      Path rootdir = services.getMasterStorage().getRootDir();
      Path tabledir =
        FSUtils.getTableDir(rootdir, htd.getTableName());
      Path storedir = HStore.getStoreHomedir(tabledir, splita,
          htd.getColumnFamilies()[0].getName());
      Reference ref = Reference.createTopReference(Bytes.toBytes("ccc"));
      long now = System.currentTimeMillis();
      // Reference name has this format: StoreFile#REF_NAME_PARSER
      Path p = new Path(storedir, Long.toString(now) + "." + parent.getEncodedName());
      FileSystem fs = services.getMasterStorage().getFileSystem();
      Path path = ref.write(fs, p);
      assertTrue(fs.exists(path));
      assertFalse(janitor.cleanParent(parent, r));
      // Remove the reference file and try again.
      assertTrue(fs.delete(p, true));
      assertTrue(janitor.cleanParent(parent, r));
    } finally {
      services.stop("shutdown");
    }
  }

  /**
   * Make sure parent gets cleaned up even if daughter is cleaned up before it.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testParentCleanedEvenIfDaughterGoneFirst()
  throws IOException, InterruptedException {
    parentWithSpecifiedEndKeyCleanedEvenIfDaughterGoneFirst(
      "testParentCleanedEvenIfDaughterGoneFirst", Bytes.toBytes("eee"));
  }

  /**
   * Make sure last parent with empty end key gets cleaned up even if daughter is cleaned up before it.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testLastParentCleanedEvenIfDaughterGoneFirst()
  throws IOException, InterruptedException {
    parentWithSpecifiedEndKeyCleanedEvenIfDaughterGoneFirst(
      "testLastParentCleanedEvenIfDaughterGoneFirst", new byte[0]);
  }

  /**
   * Make sure parent with specified end key gets cleaned up even if daughter is cleaned up before it.
   *
   * @param rootDir the test case name, used as the HBase testing utility root
   * @param lastEndKey the end key of the split parent
   * @throws IOException
   * @throws InterruptedException
   */
  private void parentWithSpecifiedEndKeyCleanedEvenIfDaughterGoneFirst(
  final String rootDir, final byte[] lastEndKey)
  throws IOException, InterruptedException {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    setRootDirAndCleanIt(htu, rootDir);
    MasterServices services = new MockMasterServices(htu);
    CatalogJanitor janitor = new CatalogJanitor(services);
    final HTableDescriptor htd = createHTableDescriptor();

    // Create regions: aaa->{lastEndKey}, aaa->ccc, aaa->bbb, bbb->ccc, etc.

    // Parent
    HRegionInfo parent = new HRegionInfo(htd.getTableName(), Bytes.toBytes("aaa"),
      lastEndKey);
    // Sleep a second else the encoded name on these regions comes out
    // same for all with same start key and made in same second.
    Thread.sleep(1001);

    // Daughter a
    HRegionInfo splita = new HRegionInfo(htd.getTableName(), Bytes.toBytes("aaa"),
      Bytes.toBytes("ccc"));
    Thread.sleep(1001);
    // Make daughters of daughter a; splitaa and splitab.
    HRegionInfo splitaa = new HRegionInfo(htd.getTableName(), Bytes.toBytes("aaa"),
      Bytes.toBytes("bbb"));
    HRegionInfo splitab = new HRegionInfo(htd.getTableName(), Bytes.toBytes("bbb"),
      Bytes.toBytes("ccc"));

    // Daughter b
    HRegionInfo splitb = new HRegionInfo(htd.getTableName(), Bytes.toBytes("ccc"),
      lastEndKey);
    Thread.sleep(1001);
    // Make Daughters of daughterb; splitba and splitbb.
    HRegionInfo splitba = new HRegionInfo(htd.getTableName(), Bytes.toBytes("ccc"),
      Bytes.toBytes("ddd"));
    HRegionInfo splitbb = new HRegionInfo(htd.getTableName(), Bytes.toBytes("ddd"),
    lastEndKey);

    // First test that our Comparator works right up in CatalogJanitor.
    // Just fo kicks.
    SortedMap<HRegionInfo, Result> regions =
      new TreeMap<HRegionInfo, Result>(new CatalogJanitor.SplitParentFirstComparator());
    // Now make sure that this regions map sorts as we expect it to.
    regions.put(parent, createResult(parent, splita, splitb));
    regions.put(splitb, createResult(splitb, splitba, splitbb));
    regions.put(splita, createResult(splita, splitaa, splitab));
    // Assert its properly sorted.
    int index = 0;
    for (Map.Entry<HRegionInfo, Result> e: regions.entrySet()) {
      if (index == 0) {
        assertTrue(e.getKey().getEncodedName().equals(parent.getEncodedName()));
      } else if (index == 1) {
        assertTrue(e.getKey().getEncodedName().equals(splita.getEncodedName()));
      } else if (index == 2) {
        assertTrue(e.getKey().getEncodedName().equals(splitb.getEncodedName()));
      }
      index++;
    }

    // Now play around with the cleanParent function.  Create a ref from splita
    // up to the parent.
    Path splitaRef =
      createReferences(services, htd, parent, splita, Bytes.toBytes("ccc"), false);
    // Make sure actual super parent sticks around because splita has a ref.
    assertFalse(janitor.cleanParent(parent, regions.get(parent)));

    //splitba, and split bb, do not have dirs in fs.  That means that if
    // we test splitb, it should get cleaned up.
    assertTrue(janitor.cleanParent(splitb, regions.get(splitb)));

    // Now remove ref from splita to parent... so parent can be let go and so
    // the daughter splita can be split (can't split if still references).
    // BUT make the timing such that the daughter gets cleaned up before we
    // can get a chance to let go of the parent.
    FileSystem fs = FileSystem.get(htu.getConfiguration());
    assertTrue(fs.delete(splitaRef, true));
    // Create the refs from daughters of splita.
    Path splitaaRef =
      createReferences(services, htd, splita, splitaa, Bytes.toBytes("bbb"), false);
    Path splitabRef =
      createReferences(services, htd, splita, splitab, Bytes.toBytes("bbb"), true);

    // Test splita.  It should stick around because references from splitab, etc.
    assertFalse(janitor.cleanParent(splita, regions.get(splita)));

    // Now clean up parent daughter first.  Remove references from its daughters.
    assertTrue(fs.delete(splitaaRef, true));
    assertTrue(fs.delete(splitabRef, true));
    assertTrue(janitor.cleanParent(splita, regions.get(splita)));

    // Super parent should get cleaned up now both splita and splitb are gone.
    assertTrue(janitor.cleanParent(parent, regions.get(parent)));

    services.stop("test finished");
    janitor.cancel(true);
  }

  /**
   * CatalogJanitor.scan() should not clean parent regions if their own
   * parents are still referencing them. This ensures that grandfather regions
   * do not point to deleted parent regions.
   */
  @Test
  public void testScanDoesNotCleanRegionsWithExistingParents() throws Exception {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    setRootDirAndCleanIt(htu, "testScanDoesNotCleanRegionsWithExistingParents");
    MasterServices services = new MockMasterServices(htu);

    final HTableDescriptor htd = createHTableDescriptor();

    // Create regions: aaa->{lastEndKey}, aaa->ccc, aaa->bbb, bbb->ccc, etc.

    // Parent
    HRegionInfo parent = new HRegionInfo(htd.getTableName(), Bytes.toBytes("aaa"),
      new byte[0], true);
    // Sleep a second else the encoded name on these regions comes out
    // same for all with same start key and made in same second.
    Thread.sleep(1001);

    // Daughter a
    HRegionInfo splita = new HRegionInfo(htd.getTableName(), Bytes.toBytes("aaa"),
      Bytes.toBytes("ccc"), true);
    Thread.sleep(1001);
    // Make daughters of daughter a; splitaa and splitab.
    HRegionInfo splitaa = new HRegionInfo(htd.getTableName(), Bytes.toBytes("aaa"),
      Bytes.toBytes("bbb"), false);
    HRegionInfo splitab = new HRegionInfo(htd.getTableName(), Bytes.toBytes("bbb"),
      Bytes.toBytes("ccc"), false);

    // Daughter b
    HRegionInfo splitb = new HRegionInfo(htd.getTableName(), Bytes.toBytes("ccc"),
        new byte[0]);
    Thread.sleep(1001);

    final Map<HRegionInfo, Result> splitParents =
        new TreeMap<HRegionInfo, Result>(new SplitParentFirstComparator());
    splitParents.put(parent, createResult(parent, splita, splitb));
    splita.setOffline(true); //simulate that splita goes offline when it is split
    splitParents.put(splita, createResult(splita, splitaa,splitab));

    final Map<HRegionInfo, Result> mergedRegions = new TreeMap<HRegionInfo, Result>();
    CatalogJanitor janitor = spy(new CatalogJanitor(services));
    doReturn(new Triple<Integer, Map<HRegionInfo, Result>, Map<HRegionInfo, Result>>(
            10, mergedRegions, splitParents)).when(janitor)
        .getMergedRegionsAndSplitParents();

    //create ref from splita to parent
    Path splitaRef =
        createReferences(services, htd, parent, splita, Bytes.toBytes("ccc"), false);

    //parent and A should not be removed
    assertEquals(0, janitor.scan());

    //now delete the ref
    FileSystem fs = FileSystem.get(htu.getConfiguration());
    assertTrue(fs.delete(splitaRef, true));

    //now, both parent, and splita can be deleted
    assertEquals(2, janitor.scan());

    services.stop("test finished");
    janitor.cancel(true);
  }

  /**
   * Test that we correctly archive all the storefiles when a region is deleted
   * @throws Exception
   */
  @Test
  public void testSplitParentFirstComparator() {
    SplitParentFirstComparator comp = new SplitParentFirstComparator();
    final HTableDescriptor htd = createHTableDescriptor();

    /*  Region splits:
     *
     *  rootRegion --- firstRegion --- firstRegiona
     *              |               |- firstRegionb
     *              |
     *              |- lastRegion --- lastRegiona  --- lastRegionaa
     *                             |                |- lastRegionab
     *                             |- lastRegionb
     *
     *  rootRegion   :   []  - []
     *  firstRegion  :   []  - bbb
     *  lastRegion   :   bbb - []
     *  firstRegiona :   []  - aaa
     *  firstRegionb :   aaa - bbb
     *  lastRegiona  :   bbb - ddd
     *  lastRegionb  :   ddd - []
     */

    // root region
    HRegionInfo rootRegion = new HRegionInfo(htd.getTableName(),
      HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW, true);
    HRegionInfo firstRegion = new HRegionInfo(htd.getTableName(),
      HConstants.EMPTY_START_ROW,
      Bytes.toBytes("bbb"), true);
    HRegionInfo lastRegion = new HRegionInfo(htd.getTableName(),
      Bytes.toBytes("bbb"),
      HConstants.EMPTY_END_ROW, true);

    assertTrue(comp.compare(rootRegion, rootRegion) == 0);
    assertTrue(comp.compare(firstRegion, firstRegion) == 0);
    assertTrue(comp.compare(lastRegion, lastRegion) == 0);
    assertTrue(comp.compare(rootRegion, firstRegion) < 0);
    assertTrue(comp.compare(rootRegion, lastRegion) < 0);
    assertTrue(comp.compare(firstRegion, lastRegion) < 0);

    //first region split into a, b
    HRegionInfo firstRegiona = new HRegionInfo(htd.getTableName(),
      HConstants.EMPTY_START_ROW,
      Bytes.toBytes("aaa"), true);
    HRegionInfo firstRegionb = new HRegionInfo(htd.getTableName(),
        Bytes.toBytes("aaa"),
      Bytes.toBytes("bbb"), true);
    //last region split into a, b
    HRegionInfo lastRegiona = new HRegionInfo(htd.getTableName(),
      Bytes.toBytes("bbb"),
      Bytes.toBytes("ddd"), true);
    HRegionInfo lastRegionb = new HRegionInfo(htd.getTableName(),
      Bytes.toBytes("ddd"),
      HConstants.EMPTY_END_ROW, true);

    assertTrue(comp.compare(firstRegiona, firstRegiona) == 0);
    assertTrue(comp.compare(firstRegionb, firstRegionb) == 0);
    assertTrue(comp.compare(rootRegion, firstRegiona) < 0);
    assertTrue(comp.compare(rootRegion, firstRegionb) < 0);
    assertTrue(comp.compare(firstRegion, firstRegiona) < 0);
    assertTrue(comp.compare(firstRegion, firstRegionb) < 0);
    assertTrue(comp.compare(firstRegiona, firstRegionb) < 0);

    assertTrue(comp.compare(lastRegiona, lastRegiona) == 0);
    assertTrue(comp.compare(lastRegionb, lastRegionb) == 0);
    assertTrue(comp.compare(rootRegion, lastRegiona) < 0);
    assertTrue(comp.compare(rootRegion, lastRegionb) < 0);
    assertTrue(comp.compare(lastRegion, lastRegiona) < 0);
    assertTrue(comp.compare(lastRegion, lastRegionb) < 0);
    assertTrue(comp.compare(lastRegiona, lastRegionb) < 0);

    assertTrue(comp.compare(firstRegiona, lastRegiona) < 0);
    assertTrue(comp.compare(firstRegiona, lastRegionb) < 0);
    assertTrue(comp.compare(firstRegionb, lastRegiona) < 0);
    assertTrue(comp.compare(firstRegionb, lastRegionb) < 0);

    HRegionInfo lastRegionaa = new HRegionInfo(htd.getTableName(),
      Bytes.toBytes("bbb"),
      Bytes.toBytes("ccc"), false);
    HRegionInfo lastRegionab = new HRegionInfo(htd.getTableName(),
      Bytes.toBytes("ccc"),
      Bytes.toBytes("ddd"), false);

    assertTrue(comp.compare(lastRegiona, lastRegionaa) < 0);
    assertTrue(comp.compare(lastRegiona, lastRegionab) < 0);
    assertTrue(comp.compare(lastRegionaa, lastRegionab) < 0);

  }

  @Test
  public void testArchiveOldRegion() throws Exception {
    String table = "table";
    HBaseTestingUtility htu = new HBaseTestingUtility();
    setRootDirAndCleanIt(htu, "testCleanParent");
    MasterServices services = new MockMasterServices(htu);

    // create the janitor
    CatalogJanitor janitor = new CatalogJanitor(services);

    // Create regions.
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(table));
    htd.addFamily(new HColumnDescriptor("f"));
    HRegionInfo parent = new HRegionInfo(htd.getTableName(),
        Bytes.toBytes("aaa"), Bytes.toBytes("eee"));
    HRegionInfo splita = new HRegionInfo(htd.getTableName(),
        Bytes.toBytes("aaa"), Bytes.toBytes("ccc"));
    HRegionInfo splitb = new HRegionInfo(htd.getTableName(),
        Bytes.toBytes("ccc"),
        Bytes.toBytes("eee"));

    // Test that when both daughter regions are in place, that we do not
    // remove the parent.
    Result parentMetaRow = createResult(parent, splita, splitb);
    FileSystem fs = FileSystem.get(htu.getConfiguration());
    Path rootdir = services.getMasterStorage().getRootDir();
    // have to set the root directory since we use it in HFileDisposer to figure out to get to the
    // archive directory. Otherwise, it just seems to pick the first root directory it can find (so
    // the single test passes, but when the full suite is run, things get borked).
    FSUtils.setRootDir(fs.getConf(), rootdir);
    Path tabledir = FSUtils.getTableDir(rootdir, htd.getTableName());
    Path storedir = HStore.getStoreHomedir(tabledir, parent, htd.getColumnFamilies()[0].getName());
    Path storeArchive = HFileArchiveUtil.getStoreArchivePath(services.getConfiguration(), parent,
      tabledir, htd.getColumnFamilies()[0].getName());
    LOG.debug("Table dir:" + tabledir);
    LOG.debug("Store dir:" + storedir);
    LOG.debug("Store archive dir:" + storeArchive);

    // add a couple of store files that we can check for
    FileStatus[] mockFiles = addMockStoreFiles(2, services, storedir);
    // get the current store files for comparison
    FileStatus[] storeFiles = fs.listStatus(storedir);
    int index = 0;
    for (FileStatus file : storeFiles) {
      LOG.debug("Have store file:" + file.getPath());
      assertEquals("Got unexpected store file", mockFiles[index].getPath(),
        storeFiles[index].getPath());
      index++;
    }

    // do the cleaning of the parent
    assertTrue(janitor.cleanParent(parent, parentMetaRow));
    LOG.debug("Finished cleanup of parent region");

    // and now check to make sure that the files have actually been archived
    FileStatus[] archivedStoreFiles = fs.listStatus(storeArchive);
    logFiles("archived files", storeFiles);
    logFiles("archived files", archivedStoreFiles);

    assertArchiveEqualToOriginal(storeFiles, archivedStoreFiles, fs);

    // cleanup
    FSUtils.delete(fs, rootdir, true);
    services.stop("Test finished");
    janitor.cancel(true);
  }

  /**
   * @param description description of the files for logging
   * @param storeFiles the status of the files to log
   */
  private void logFiles(String description, FileStatus[] storeFiles) {
    LOG.debug("Current " + description + ": ");
    for (FileStatus file : storeFiles) {
      LOG.debug(file.getPath());
    }
  }

  /**
   * Test that if a store file with the same name is present as those already backed up cause the
   * already archived files to be timestamped backup
   */
  @Test
  public void testDuplicateHFileResolution() throws Exception {
    String table = "table";
    HBaseTestingUtility htu = new HBaseTestingUtility();
    setRootDirAndCleanIt(htu, "testCleanParent");
    MasterServices services = new MockMasterServices(htu);

    // create the janitor

    CatalogJanitor janitor = new CatalogJanitor(services);

    // Create regions.
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(table));
    htd.addFamily(new HColumnDescriptor("f"));
    HRegionInfo parent = new HRegionInfo(htd.getTableName(),
        Bytes.toBytes("aaa"), Bytes.toBytes("eee"));
    HRegionInfo splita = new HRegionInfo(htd.getTableName(),
        Bytes.toBytes("aaa"), Bytes.toBytes("ccc"));
    HRegionInfo splitb = new HRegionInfo(htd.getTableName(),
        Bytes.toBytes("ccc"), Bytes.toBytes("eee"));
    // Test that when both daughter regions are in place, that we do not
    // remove the parent.
    Result r = createResult(parent, splita, splitb);

    FileSystem fs = FileSystem.get(htu.getConfiguration());

    Path rootdir = services.getMasterStorage().getRootDir();
    // have to set the root directory since we use it in HFileDisposer to figure out to get to the
    // archive directory. Otherwise, it just seems to pick the first root directory it can find (so
    // the single test passes, but when the full suite is run, things get borked).
    FSUtils.setRootDir(fs.getConf(), rootdir);
    Path tabledir = FSUtils.getTableDir(rootdir, parent.getTable());
    Path storedir = HStore.getStoreHomedir(tabledir, parent, htd.getColumnFamilies()[0].getName());
    System.out.println("Old root:" + rootdir);
    System.out.println("Old table:" + tabledir);
    System.out.println("Old store:" + storedir);

    Path storeArchive = HFileArchiveUtil.getStoreArchivePath(services.getConfiguration(), parent,
      tabledir, htd.getColumnFamilies()[0].getName());
    System.out.println("Old archive:" + storeArchive);

    // enable archiving, make sure that files get archived
    addMockStoreFiles(2, services, storedir);
    // get the current store files for comparison
    FileStatus[] storeFiles = fs.listStatus(storedir);
    // do the cleaning of the parent
    assertTrue(janitor.cleanParent(parent, r));

    // and now check to make sure that the files have actually been archived
    FileStatus[] archivedStoreFiles = fs.listStatus(storeArchive);
    assertArchiveEqualToOriginal(storeFiles, archivedStoreFiles, fs);

    // now add store files with the same names as before to check backup
    // enable archiving, make sure that files get archived
    addMockStoreFiles(2, services, storedir);

    // do the cleaning of the parent
    assertTrue(janitor.cleanParent(parent, r));

    // and now check to make sure that the files have actually been archived
    archivedStoreFiles = fs.listStatus(storeArchive);
    assertArchiveEqualToOriginal(storeFiles, archivedStoreFiles, fs, true);

    // cleanup
    services.stop("Test finished");
    janitor.cancel(true);
  }

  private FileStatus[] addMockStoreFiles(int count, MasterServices services, Path storedir)
      throws IOException {
    // get the existing store files
    FileSystem fs = services.getMasterStorage().getFileSystem();
    fs.mkdirs(storedir);
    // create the store files in the parent
    for (int i = 0; i < count; i++) {
      Path storeFile = new Path(storedir, "_store" + i);
      FSDataOutputStream dos = fs.create(storeFile, true);
      dos.writeBytes("Some data: " + i);
      dos.close();
    }
    LOG.debug("Adding " + count + " store files to the storedir:" + storedir);
    // make sure the mock store files are there
    FileStatus[] storeFiles = fs.listStatus(storedir);
    assertEquals("Didn't have expected store files", count, storeFiles.length);
    return storeFiles;
  }

  private String setRootDirAndCleanIt(final HBaseTestingUtility htu,
      final String subdir)
  throws IOException {
    Path testdir = htu.getDataTestDir(subdir);
    FileSystem fs = FileSystem.get(htu.getConfiguration());
    if (fs.exists(testdir)) assertTrue(fs.delete(testdir, true));
    FSUtils.setRootDir(htu.getConfiguration(), testdir);
    return FSUtils.getRootDir(htu.getConfiguration()).toString();
  }

  /**
   * @param services Master services instance.
   * @param htd
   * @param parent
   * @param daughter
   * @param midkey
   * @param top True if we are to write a 'top' reference.
   * @return Path to reference we created.
   * @throws IOException
   */
  private Path createReferences(final MasterServices services,
      final HTableDescriptor htd, final HRegionInfo parent,
      final HRegionInfo daughter, final byte [] midkey, final boolean top)
  throws IOException {
    Path rootdir = services.getMasterStorage().getRootDir();
    Path tabledir = FSUtils.getTableDir(rootdir, parent.getTable());
    Path storedir = HStore.getStoreHomedir(tabledir, daughter,
      htd.getColumnFamilies()[0].getName());
    Reference ref =
      top? Reference.createTopReference(midkey): Reference.createBottomReference(midkey);
    long now = System.currentTimeMillis();
    // Reference name has this format: StoreFile#REF_NAME_PARSER
    Path p = new Path(storedir, Long.toString(now) + "." + parent.getEncodedName());
    FileSystem fs = services.getMasterStorage().getFileSystem();
    ref.write(fs, p);
    return p;
  }

  private Result createResult(final HRegionInfo parent, final HRegionInfo a,
      final HRegionInfo b)
  throws IOException {
    return MetaMockingUtil.getMetaTableRowResult(parent, null, a, b);
  }

  private HTableDescriptor createHTableDescriptor() {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("t"));
    htd.addFamily(new HColumnDescriptor("f"));
    return htd;
  }

  private MultiResponse buildMultiResponse(MultiRequest req) {
    MultiResponse.Builder builder = MultiResponse.newBuilder();
    RegionActionResult.Builder regionActionResultBuilder =
        RegionActionResult.newBuilder();
    ResultOrException.Builder roeBuilder = ResultOrException.newBuilder();
    for (RegionAction regionAction: req.getRegionActionList()) {
      regionActionResultBuilder.clear();
      for (ClientProtos.Action action: regionAction.getActionList()) {
        roeBuilder.clear();
        roeBuilder.setResult(ClientProtos.Result.getDefaultInstance());
        roeBuilder.setIndex(action.getIndex());
        regionActionResultBuilder.addResultOrException(roeBuilder.build());
      }
      builder.addRegionActionResult(regionActionResultBuilder.build());
    }
    return builder.build();
  }

}

