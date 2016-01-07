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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HConnectionTestingUtility;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.CatalogJanitor.SplitParentFirstComparator;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(SmallTests.class)
public class TestCatalogJanitor {
  /**
   * Pseudo server for below tests.
   * Be sure to call stop on the way out else could leave some mess around.
   */
  class MockServer implements Server {
    private final HConnection connection;
    private final Configuration c;
    private final CatalogTracker ct;

    MockServer(final HBaseTestingUtility htu)
    throws NotAllMetaRegionsOnlineException, IOException, InterruptedException {
      this.c = htu.getConfiguration();
      // Mock an HConnection and a HRegionInterface implementation.  Have the
      // HConnection return the HRI.  Have the HRI return a few mocked up responses
      // to make our test work.
      this.connection =
        HConnectionTestingUtility.getMockedConnectionAndDecorate(this.c,
          Mockito.mock(HRegionInterface.class),
          new ServerName("example.org,12345,6789"),
          HRegionInfo.FIRST_META_REGIONINFO);
      // Set hbase.rootdir into test dir.
      FileSystem fs = FileSystem.get(this.c);
      Path rootdir = fs.makeQualified(new Path(this.c.get(HConstants.HBASE_DIR)));
      this.c.set(HConstants.HBASE_DIR, rootdir.toString());
      this.ct = Mockito.mock(CatalogTracker.class);
      HRegionInterface hri = Mockito.mock(HRegionInterface.class);
      Mockito.when(this.ct.getConnection()).thenReturn(this.connection);
      Mockito.when(ct.waitForMetaServerConnection(Mockito.anyLong())).thenReturn(hri);
    }

    @Override
    public CatalogTracker getCatalogTracker() {
      return this.ct;
    }

    @Override
    public Configuration getConfiguration() {
      return this.c;
    }

    @Override
    public ServerName getServerName() {
      return new ServerName("mockserver.example.org", 1234, -1L);
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return null;
    }

    @Override
    public void abort(String why, Throwable e) {
      //no-op
    }

    @Override
    public boolean isAborted() {
      return false;
    }

    @Override
    public boolean isStopped() {
      return false;
    }

    @Override
    public void stop(String why) {
      if (this.ct != null) {
        this.ct.stop();
      }
      if (this.connection != null) {
        HConnectionManager.deleteConnection(this.connection.getConfiguration());
      }
    }
  }

  /**
   * Mock MasterServices for tests below.
   */
  class MockMasterServices implements MasterServices {
    private final MasterFileSystem mfs;
    private final AssignmentManager asm;

    MockMasterServices(final Server server) throws IOException {
      this.mfs = new MasterFileSystem(server, this, null, false);
      this.asm = Mockito.mock(AssignmentManager.class);
    }

    @Override
    public void checkTableModifiable(byte[] tableName) throws IOException {
      //no-op
    }

    @Override
    public void createTable(HTableDescriptor desc, byte[][] splitKeys)
        throws IOException {
      // no-op
    }

    @Override
    public AssignmentManager getAssignmentManager() {
      return this.asm;
    }

    @Override
    public ExecutorService getExecutorService() {
      return null;
    }

    @Override
    public MasterFileSystem getMasterFileSystem() {
      return this.mfs;
    }

    @Override
    public ServerManager getServerManager() {
      return null;
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return null;
    }

    @Override
    public CatalogTracker getCatalogTracker() {
      return null;
    }

    @Override
    public Configuration getConfiguration() {
      return mfs.conf;
    }

    @Override
    public ServerName getServerName() {
      return null;
    }

    @Override
    public void abort(String why, Throwable e) {
      //no-op
    }

    @Override
    public boolean isAborted() {
      return false;
    }

    private boolean stopped = false;

    @Override
    public void stop(String why) {
      stopped = true;
    }

    @Override
    public boolean isStopped() {
      return stopped;
    }

    @Override
    public TableDescriptors getTableDescriptors() {
      return new TableDescriptors() {
        @Override
        public HTableDescriptor remove(String tablename) throws IOException {
          return null;
        }

        @Override
        public Map<String, HTableDescriptor> getAll() throws IOException {
          return null;
        }

        @Override
        public HTableDescriptor get(byte[] tablename)
        throws IOException {
          return get(Bytes.toString(tablename));
        }

        @Override
        public HTableDescriptor get(String tablename)
        throws IOException {
          return createHTableDescriptor();
        }

        @Override
        public void add(HTableDescriptor htd) throws IOException {
        }
      };
    }

    @Override
    public boolean isServerShutdownHandlerEnabled() {
      return true;
    }

    @Override
    public MasterCoprocessorHost getCoprocessorHost() {
      return null;
    }

    @Override
    public <T extends CoprocessorProtocol> boolean registerProtocol(Class<T> protocol, T handler) {
      return false;
    }

    @Override
    public void deleteTable(byte[] tableName) throws IOException {
    }

    @Override
    public void modifyTable(byte[] tableName, HTableDescriptor descriptor) throws IOException {
    }

    @Override
    public void enableTable(byte[] tableName) throws IOException {
    }

    @Override
    public void disableTable(byte[] tableName) throws IOException {
    }

    @Override
    public void addColumn(byte[] tableName, HColumnDescriptor column) throws IOException {
    }

    @Override
    public void modifyColumn(byte[] tableName, HColumnDescriptor descriptor) throws IOException {
    }

    @Override
    public void deleteColumn(byte[] tableName, byte[] columnName) throws IOException {
    }

    @Override
    public boolean shouldSplitMetaSeparately() {
      return false;
    }
  }

  @Test
  public void testGetHRegionInfo() throws IOException {
    assertNull(CatalogJanitor.getHRegionInfo(new Result()));
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    Result r = new Result(kvs);
    assertNull(CatalogJanitor.getHRegionInfo(r));
    byte [] f = HConstants.CATALOG_FAMILY;
    // Make a key value that doesn't have the expected qualifier.
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY, f,
      HConstants.SERVER_QUALIFIER, f));
    r = new Result(kvs);
    assertNull(CatalogJanitor.getHRegionInfo(r));
    // Make a key that does not have a regioninfo value.
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY, f,
      HConstants.REGIONINFO_QUALIFIER, f));
    HRegionInfo hri = CatalogJanitor.getHRegionInfo(new Result(kvs));
    assertTrue(hri == null);
    // OK, give it what it expects
    kvs.clear();
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY, f,
      HConstants.REGIONINFO_QUALIFIER,
      Writables.getBytes(HRegionInfo.FIRST_META_REGIONINFO)));
    hri = CatalogJanitor.getHRegionInfo(new Result(kvs));
    assertNotNull(hri);
    assertTrue(hri.equals(HRegionInfo.FIRST_META_REGIONINFO));
  }

  @Test
  public void testCleanParent() throws IOException, InterruptedException {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    setRootDirAndCleanIt(htu, "testCleanParent");
    Server server = new MockServer(htu);
    try {
      MasterServices services = new MockMasterServices(server);
      CatalogJanitor janitor = new CatalogJanitor(server, services);
      // Create regions.
      HTableDescriptor htd = new HTableDescriptor("table");
      htd.addFamily(new HColumnDescriptor("f"));
      HRegionInfo parent =
        new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"),
            Bytes.toBytes("eee"));
      HRegionInfo splita =
        new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"),
            Bytes.toBytes("ccc"));
      HRegionInfo splitb =
        new HRegionInfo(htd.getName(), Bytes.toBytes("ccc"),
            Bytes.toBytes("eee"));
      // Test that when both daughter regions are in place, that we do not
      // remove the parent.
      List<KeyValue> kvs = new ArrayList<KeyValue>();
      kvs.add(new KeyValue(parent.getRegionName(), HConstants.CATALOG_FAMILY,
          HConstants.SPLITA_QUALIFIER, Writables.getBytes(splita)));
      kvs.add(new KeyValue(parent.getRegionName(), HConstants.CATALOG_FAMILY,
          HConstants.SPLITB_QUALIFIER, Writables.getBytes(splitb)));
      Result r = new Result(kvs);
      // Add a reference under splitA directory so we don't clear out the parent.
      Path rootdir = services.getMasterFileSystem().getRootDir();
      Path tabledir =
        HTableDescriptor.getTableDir(rootdir, htd.getName());
      Path storedir = Store.getStoreHomedir(tabledir, splita.getEncodedName(),
          htd.getColumnFamilies()[0].getName());
      Reference ref = new Reference(Bytes.toBytes("ccc"), Reference.Range.top);
      long now = System.currentTimeMillis();
      // Reference name has this format: StoreFile#REF_NAME_PARSER
      Path p = new Path(storedir, Long.toString(now) + "." + parent.getEncodedName());
      FileSystem fs = services.getMasterFileSystem().getFileSystem();
      Path path = ref.write(fs, p);
      assertTrue(fs.exists(path));
      assertFalse(janitor.cleanParent(parent, r));
      // Remove the reference file and try again.
      assertTrue(fs.delete(p, true));
      assertTrue(janitor.cleanParent(parent, r));
    } finally {
      server.stop("shutdown");
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
    Server server = new MockServer(htu);
    MasterServices services = new MockMasterServices(server);
    CatalogJanitor janitor = new CatalogJanitor(server, services);
    final HTableDescriptor htd = createHTableDescriptor();

    // Create regions: aaa->{lastEndKey}, aaa->ccc, aaa->bbb, bbb->ccc, etc.

    // Parent
    HRegionInfo parent = new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"),
      lastEndKey);
    // Sleep a second else the encoded name on these regions comes out
    // same for all with same start key and made in same second.
    Thread.sleep(1001);

    // Daughter a
    HRegionInfo splita = new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"),
      Bytes.toBytes("ccc"));
    Thread.sleep(1001);
    // Make daughters of daughter a; splitaa and splitab.
    HRegionInfo splitaa = new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"),
      Bytes.toBytes("bbb"));
    HRegionInfo splitab = new HRegionInfo(htd.getName(), Bytes.toBytes("bbb"),
      Bytes.toBytes("ccc"));

    // Daughter b
    HRegionInfo splitb = new HRegionInfo(htd.getName(), Bytes.toBytes("ccc"),
      lastEndKey);
    Thread.sleep(1001);
    // Make Daughters of daughterb; splitba and splitbb.
    HRegionInfo splitba = new HRegionInfo(htd.getName(), Bytes.toBytes("ccc"),
      Bytes.toBytes("ddd"));
    HRegionInfo splitbb = new HRegionInfo(htd.getName(), Bytes.toBytes("ddd"),
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
    janitor.join();
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
    Server server = new MockServer(htu);
    MasterServices services = new MockMasterServices(server);

    final HTableDescriptor htd = createHTableDescriptor();

    // Create regions: aaa->{lastEndKey}, aaa->ccc, aaa->bbb, bbb->ccc, etc.

    // Parent
    HRegionInfo parent = new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"),
      new byte[0], true);
    // Sleep a second else the encoded name on these regions comes out
    // same for all with same start key and made in same second.
    Thread.sleep(1001);

    // Daughter a
    HRegionInfo splita = new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"),
      Bytes.toBytes("ccc"), true);
    Thread.sleep(1001);
    // Make daughters of daughter a; splitaa and splitab.
    HRegionInfo splitaa = new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"),
      Bytes.toBytes("bbb"), false);
    HRegionInfo splitab = new HRegionInfo(htd.getName(), Bytes.toBytes("bbb"),
      Bytes.toBytes("ccc"), false);

    // Daughter b
    HRegionInfo splitb = new HRegionInfo(htd.getName(), Bytes.toBytes("ccc"),
        new byte[0]);
    Thread.sleep(1001);

    final Map<HRegionInfo, Result> splitParents =
        new TreeMap<HRegionInfo, Result>(new SplitParentFirstComparator());
    splitParents.put(parent, makeResultFromHRegionInfo(parent, splita, splitb));
    splita.setOffline(true);//simulate that splita goes offline when it is split
    splitParents.put(splita, makeResultFromHRegionInfo(splita, splitaa, splitab));

    CatalogJanitor janitor = spy(new CatalogJanitor(server, services));
    doReturn(new Pair<Integer, Map<HRegionInfo, Result>>(
        10, splitParents)).when(janitor).getSplitParents();

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
    janitor.join();
  }

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
    HRegionInfo rootRegion = new HRegionInfo(htd.getName(), HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW, true);
    HRegionInfo firstRegion = new HRegionInfo(htd.getName(), HConstants.EMPTY_START_ROW,
      Bytes.toBytes("bbb"), true);
    HRegionInfo lastRegion = new HRegionInfo(htd.getName(), Bytes.toBytes("bbb"),
      HConstants.EMPTY_END_ROW, true);

    assertTrue(comp.compare(rootRegion, rootRegion) == 0);
    assertTrue(comp.compare(firstRegion, firstRegion) == 0);
    assertTrue(comp.compare(lastRegion, lastRegion) == 0);
    assertTrue(comp.compare(rootRegion, firstRegion) < 0);
    assertTrue(comp.compare(rootRegion, lastRegion) < 0);
    assertTrue(comp.compare(firstRegion, lastRegion) < 0);

    //first region split into a, b
    HRegionInfo firstRegiona = new HRegionInfo(htd.getName(), HConstants.EMPTY_START_ROW,
      Bytes.toBytes("aaa"), true);
    HRegionInfo firstRegionb = new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"),
      Bytes.toBytes("bbb"), true);
    //last region split into a, b
    HRegionInfo lastRegiona = new HRegionInfo(htd.getName(), Bytes.toBytes("bbb"),
      Bytes.toBytes("ddd"), true);
    HRegionInfo lastRegionb = new HRegionInfo(htd.getName(), Bytes.toBytes("ddd"),
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

    HRegionInfo lastRegionaa = new HRegionInfo(htd.getName(), Bytes.toBytes("bbb"),
      Bytes.toBytes("ccc"), false);
    HRegionInfo lastRegionab = new HRegionInfo(htd.getName(), Bytes.toBytes("ccc"),
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
    Server server = new MockServer(htu);
    MasterServices services = new MockMasterServices(server);

    // create the janitor
    CatalogJanitor janitor = new CatalogJanitor(server, services);

    // Create regions.
    HTableDescriptor htd = new HTableDescriptor(table);
    htd.addFamily(new HColumnDescriptor("f"));
    HRegionInfo parent = new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"), Bytes.toBytes("eee"));
    HRegionInfo splita = new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"), Bytes.toBytes("ccc"));
    HRegionInfo splitb = new HRegionInfo(htd.getName(), Bytes.toBytes("ccc"), Bytes.toBytes("eee"));
    // Test that when both daughter regions are in place, that we do not
    // remove the parent.
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(new KeyValue(parent.getRegionName(), HConstants.CATALOG_FAMILY,
        HConstants.SPLITA_QUALIFIER, Writables.getBytes(splita)));
    kvs.add(new KeyValue(parent.getRegionName(), HConstants.CATALOG_FAMILY,
        HConstants.SPLITB_QUALIFIER, Writables.getBytes(splitb)));
    Result r = new Result(kvs);

    FileSystem fs = FileSystem.get(htu.getConfiguration());
    Path rootdir = services.getMasterFileSystem().getRootDir();
    // have to set the root directory since we use it in HFileDisposer to figure out to get to the
    // archive directory. Otherwise, it just seems to pick the first root directory it can find (so
    // the single test passes, but when the full suite is run, things get borked).
    FSUtils.setRootDir(fs.getConf(), rootdir);
    Path tabledir = HTableDescriptor.getTableDir(rootdir, htd.getName());
    Path storedir = Store.getStoreHomedir(tabledir, parent.getEncodedName(),
      htd.getColumnFamilies()[0].getName());

    // delete the file and ensure that the files have been archived
    Path storeArchive = HFileArchiveUtil.getStoreArchivePath(services.getConfiguration(), parent,
      tabledir, htd.getColumnFamilies()[0].getName());

    // enable archiving, make sure that files get archived
    addMockStoreFiles(2, services, storedir);
    // get the current store files for comparison
    FileStatus[] storeFiles = fs.listStatus(storedir);
    for (FileStatus file : storeFiles) {
      System.out.println("Have store file:" + file.getPath());
    }

    // do the cleaning of the parent
    assertTrue(janitor.cleanParent(parent, r));

    // and now check to make sure that the files have actually been archived
    FileStatus[] archivedStoreFiles = fs.listStatus(storeArchive);
    assertArchiveEqualToOriginal(storeFiles, archivedStoreFiles, fs);

    // cleanup
    services.stop("Test finished");
    server.stop("shutdown");
    janitor.join();
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
    Server server = new MockServer(htu);
    MasterServices services = new MockMasterServices(server);

    // create the janitor
    CatalogJanitor janitor = new CatalogJanitor(server, services);

    // Create regions.
    HTableDescriptor htd = new HTableDescriptor(table);
    htd.addFamily(new HColumnDescriptor("f"));
    HRegionInfo parent = new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"), Bytes.toBytes("eee"));
    HRegionInfo splita = new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"), Bytes.toBytes("ccc"));
    HRegionInfo splitb = new HRegionInfo(htd.getName(), Bytes.toBytes("ccc"), Bytes.toBytes("eee"));
    // Test that when both daughter regions are in place, that we do not
    // remove the parent.
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(new KeyValue(parent.getRegionName(), HConstants.CATALOG_FAMILY,
        HConstants.SPLITA_QUALIFIER, Writables.getBytes(splita)));
    kvs.add(new KeyValue(parent.getRegionName(), HConstants.CATALOG_FAMILY,
        HConstants.SPLITB_QUALIFIER, Writables.getBytes(splitb)));
    Result r = new Result(kvs);

    FileSystem fs = FileSystem.get(htu.getConfiguration());

    Path rootdir = services.getMasterFileSystem().getRootDir();
    // have to set the root directory since we use it in HFileDisposer to figure out to get to the
    // archive directory. Otherwise, it just seems to pick the first root directory it can find (so
    // the single test passes, but when the full suite is run, things get borked).
    FSUtils.setRootDir(fs.getConf(), rootdir);
    Path tabledir = HTableDescriptor.getTableDir(rootdir, parent.getTableName());
    Path storedir = Store.getStoreHomedir(tabledir, parent.getEncodedName(),
      htd.getColumnFamilies()[0].getName());
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
    server.stop("shutdown");
    janitor.join();
  }

  private void addMockStoreFiles(int count, MasterServices services, Path storedir)
      throws IOException {
    // get the existing store files
    FileSystem fs = services.getMasterFileSystem().getFileSystem();
    fs.mkdirs(storedir);
    // create the store files in the parent
    for (int i = 0; i < count; i++) {
      Path storeFile = new Path(storedir, "_store" + i);
      FSDataOutputStream dos = fs.create(storeFile, true);
      dos.writeBytes("Some data: " + i);
      dos.close();
    }
    // make sure the mock store files are there
    FileStatus[] storeFiles = fs.listStatus(storedir);
    assertEquals(count, storeFiles.length);
  }

  private Result makeResultFromHRegionInfo(HRegionInfo region, HRegionInfo splita,
      HRegionInfo splitb) throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(new KeyValue(
        region.getRegionName(),
        HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(region)));

    if (splita != null) {
      kvs.add(new KeyValue(
          region.getRegionName(),
          HConstants.CATALOG_FAMILY, HConstants.SPLITA_QUALIFIER,
          Writables.getBytes(splita)));
    }

    if (splitb != null) {
      kvs.add(new KeyValue(
          region.getRegionName(),
          HConstants.CATALOG_FAMILY, HConstants.SPLITB_QUALIFIER,
          Writables.getBytes(splitb)));
    }

    return new Result(kvs);
  }

  private String setRootDirAndCleanIt(final HBaseTestingUtility htu,
      final String subdir)
  throws IOException {
    Path testdir = htu.getDataTestDir(subdir);
    FileSystem fs = FileSystem.get(htu.getConfiguration());
    if (fs.exists(testdir)) assertTrue(fs.delete(testdir, true));
    htu.getConfiguration().set(HConstants.HBASE_DIR, testdir.toString());
    return htu.getConfiguration().get(HConstants.HBASE_DIR);
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
    Path rootdir = services.getMasterFileSystem().getRootDir();
    Path tabledir = HTableDescriptor.getTableDir(rootdir, parent.getTableName());
    Path storedir = Store.getStoreHomedir(tabledir, daughter.getEncodedName(),
      htd.getColumnFamilies()[0].getName());
    Reference ref = new Reference(midkey,
      top? Reference.Range.top: Reference.Range.bottom);
    long now = System.currentTimeMillis();
    // Reference name has this format: StoreFile#REF_NAME_PARSER
    Path p = new Path(storedir, Long.toString(now) + "." + parent.getEncodedName());
    FileSystem fs = services.getMasterFileSystem().getFileSystem();
    ref.write(fs, p);
    return p;
  }

  private Result createResult(final HRegionInfo parent, final HRegionInfo a,
      final HRegionInfo b)
  throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(new KeyValue(parent.getRegionName(), HConstants.CATALOG_FAMILY,
      HConstants.SPLITA_QUALIFIER, Writables.getBytes(a)));
    kvs.add(new KeyValue(parent.getRegionName(), HConstants.CATALOG_FAMILY,
      HConstants.SPLITB_QUALIFIER, Writables.getBytes(b)));
    return new Result(kvs);
  }

  private HTableDescriptor createHTableDescriptor() {
    HTableDescriptor htd = new HTableDescriptor("t");
    htd.addFamily(new HColumnDescriptor("f"));
    return htd;
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

