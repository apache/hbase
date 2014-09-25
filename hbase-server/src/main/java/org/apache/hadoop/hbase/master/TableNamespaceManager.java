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

package org.apache.hadoop.hbase.master;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZKNamespaceManager;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;

import com.google.common.collect.Sets;

/**
 * This is a helper class used to manage the namespace
 * metadata that is stored in TableName.NAMESPACE_TABLE_NAME
 * It also mirrors updates to the ZK store by forwarding updates to
 * {@link org.apache.hadoop.hbase.ZKNamespaceManager}
 */
@InterfaceAudience.Private
public class TableNamespaceManager {
  private static final Log LOG = LogFactory.getLog(TableNamespaceManager.class);

  private Configuration conf;
  private MasterServices masterServices;
  private HTable nsTable;
  private ZKNamespaceManager zkNamespaceManager;
  private boolean initialized;

  static final String NS_INIT_TIMEOUT = "hbase.master.namespace.init.timeout";
  static final int DEFAULT_NS_INIT_TIMEOUT = 60000;

  public TableNamespaceManager(MasterServices masterServices) {
    this.masterServices = masterServices;
    this.conf = masterServices.getConfiguration();
  }

  public void start() throws IOException {
    if (!MetaReader.tableExists(masterServices.getCatalogTracker(),
        TableName.NAMESPACE_TABLE_NAME)) {
      LOG.info("Namespace table not found. Creating...");
      createNamespaceTable(masterServices);
    }

    try {
      // Wait for the namespace table to be assigned.
      // If timed out, we will move ahead without initializing it.
      // So that it should be initialized later on lazily.
      long startTime = EnvironmentEdgeManager.currentTimeMillis();
      int timeout = conf.getInt(NS_INIT_TIMEOUT, DEFAULT_NS_INIT_TIMEOUT);
      while (!isTableAssigned()) {
        if (EnvironmentEdgeManager.currentTimeMillis() - startTime + 100 > timeout) {
          LOG.warn("Timedout waiting for namespace table to be assigned.");
          return;
        }
        Thread.sleep(100);
      }
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(e);
    }

    // initialize namespace table
    isTableAvailableAndInitialized();
  }

  private synchronized HTable getNamespaceTable() throws IOException {
    if (!isTableAvailableAndInitialized()) {
      throw new IOException(this.getClass().getName() + " isn't ready to serve");
    }
    return nsTable;
  }


  public synchronized NamespaceDescriptor get(String name) throws IOException {
    if (!isTableAvailableAndInitialized()) return null;
    return zkNamespaceManager.get(name);
  }

  public synchronized void create(NamespaceDescriptor ns) throws IOException {
    create(getNamespaceTable(), ns);
  }

  public synchronized void update(NamespaceDescriptor ns) throws IOException {
    HTable table = getNamespaceTable();
    if (get(table, ns.getName()) == null) {
      throw new NamespaceNotFoundException(ns.getName());
    }
    upsert(table, ns);
  }

  private NamespaceDescriptor get(HTable table, String name) throws IOException {
    Result res = table.get(new Get(Bytes.toBytes(name)));
    if (res.isEmpty()) {
      return null;
    }
    byte[] val = CellUtil.cloneValue(res.getColumnLatestCell(
        HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES, HTableDescriptor.NAMESPACE_COL_DESC_BYTES));
    return
        ProtobufUtil.toNamespaceDescriptor(
            HBaseProtos.NamespaceDescriptor.parseFrom(val));
  }

  private void create(HTable table, NamespaceDescriptor ns) throws IOException {
    if (get(table, ns.getName()) != null) {
      throw new NamespaceExistException(ns.getName());
    }
    FileSystem fs = masterServices.getMasterFileSystem().getFileSystem();
    fs.mkdirs(FSUtils.getNamespaceDir(
        masterServices.getMasterFileSystem().getRootDir(), ns.getName()));
    upsert(table, ns);
  }

  private void upsert(HTable table, NamespaceDescriptor ns) throws IOException {
    Put p = new Put(Bytes.toBytes(ns.getName()));
    p.addImmutable(HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES,
        HTableDescriptor.NAMESPACE_COL_DESC_BYTES,
        ProtobufUtil.toProtoNamespaceDescriptor(ns).toByteArray());
    table.put(p);
    try {
      zkNamespaceManager.update(ns);
    } catch(IOException ex) {
      String msg = "Failed to update namespace information in ZK. Aborting.";
      LOG.fatal(msg, ex);
      masterServices.abort(msg, ex);
    }
  }

  public synchronized void remove(String name) throws IOException {
    if (get(name) == null) {
      throw new NamespaceNotFoundException(name);
    }
    if (NamespaceDescriptor.RESERVED_NAMESPACES.contains(name)) {
      throw new ConstraintException("Reserved namespace "+name+" cannot be removed.");
    }
    int tableCount;
    try {
      tableCount = masterServices.listTableDescriptorsByNamespace(name).size();
    } catch (FileNotFoundException fnfe) {
      throw new NamespaceNotFoundException(name);
    }
    if (tableCount > 0) {
      throw new ConstraintException("Only empty namespaces can be removed. " +
          "Namespace "+name+" has "+tableCount+" tables");
    }
    Delete d = new Delete(Bytes.toBytes(name));
    getNamespaceTable().delete(d);
    //don't abort if cleanup isn't complete
    //it will be replaced on new namespace creation
    zkNamespaceManager.remove(name);
    FileSystem fs = masterServices.getMasterFileSystem().getFileSystem();
    for(FileStatus status :
            fs.listStatus(FSUtils.getNamespaceDir(
                masterServices.getMasterFileSystem().getRootDir(), name))) {
      if (!HConstants.HBASE_NON_TABLE_DIRS.contains(status.getPath().getName())) {
        throw new IOException("Namespace directory contains table dir: "+status.getPath());
      }
    }
    if (!fs.delete(FSUtils.getNamespaceDir(
        masterServices.getMasterFileSystem().getRootDir(), name), true)) {
      throw new IOException("Failed to remove namespace: "+name);
    }
  }

  public synchronized NavigableSet<NamespaceDescriptor> list() throws IOException {
    NavigableSet<NamespaceDescriptor> ret =
        Sets.newTreeSet(NamespaceDescriptor.NAMESPACE_DESCRIPTOR_COMPARATOR);
    ResultScanner scanner = getNamespaceTable().getScanner(HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES);
    try {
      for(Result r : scanner) {
        byte[] val = CellUtil.cloneValue(r.getColumnLatestCell(
          HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES,
          HTableDescriptor.NAMESPACE_COL_DESC_BYTES));
        ret.add(ProtobufUtil.toNamespaceDescriptor(
            HBaseProtos.NamespaceDescriptor.parseFrom(val)));
      }
    } finally {
      scanner.close();
    }
    return ret;
  }

  private void createNamespaceTable(MasterServices masterServices) throws IOException {
    HRegionInfo newRegions[] = new HRegionInfo[]{
        new HRegionInfo(HTableDescriptor.NAMESPACE_TABLEDESC.getTableName(), null, null)};

    //we need to create the table this way to bypass
    //checkInitialized
    masterServices.getExecutorService()
        .submit(new CreateTableHandler(masterServices,
            masterServices.getMasterFileSystem(),
            HTableDescriptor.NAMESPACE_TABLEDESC,
            masterServices.getConfiguration(),
            newRegions,
            masterServices).prepare());
  }

  /**
   * This method checks if the namespace table is assigned and then
   * tries to create its HTable. If it was already created before, it also makes
   * sure that the connection isn't closed.
   * @return true if the namespace table manager is ready to serve, false
   * otherwise
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  public synchronized boolean isTableAvailableAndInitialized() throws IOException {
    // Did we already get a table? If so, still make sure it's available
    if (initialized) {
      if (nsTable.getConnection().isClosed()) {
        nsTable = new HTable(conf, TableName.NAMESPACE_TABLE_NAME);
      }
      return true;
    }

    // Now check if the table is assigned, if not then fail fast
    if (isTableAssigned()) {
      try {
        nsTable = new HTable(conf, TableName.NAMESPACE_TABLE_NAME);
        zkNamespaceManager = new ZKNamespaceManager(masterServices.getZooKeeper());
        zkNamespaceManager.start();

        if (get(nsTable, NamespaceDescriptor.DEFAULT_NAMESPACE.getName()) == null) {
          create(nsTable, NamespaceDescriptor.DEFAULT_NAMESPACE);
        }
        if (get(nsTable, NamespaceDescriptor.SYSTEM_NAMESPACE.getName()) == null) {
          create(nsTable, NamespaceDescriptor.SYSTEM_NAMESPACE);
        }

        ResultScanner scanner = nsTable.getScanner(HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES);
        try {
          for (Result result : scanner) {
            byte[] val =  CellUtil.cloneValue(result.getColumnLatest(
                HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES,
                HTableDescriptor.NAMESPACE_COL_DESC_BYTES));
            NamespaceDescriptor ns =
                ProtobufUtil.toNamespaceDescriptor(
                    HBaseProtos.NamespaceDescriptor.parseFrom(val));
            zkNamespaceManager.update(ns);
          }
        } finally {
          scanner.close();
        }
        initialized = true;
        return true;
      } catch (IOException ie) {
        LOG.warn("Caught exception in initializing namespace table manager", ie);
        if (nsTable != null) {
          nsTable.close();
        }
        throw ie;
      }
    }
    return false;
  }

  private boolean isTableAssigned() {
    return !masterServices.getAssignmentManager()
        .getRegionStates().getRegionsOfTable(TableName.NAMESPACE_TABLE_NAME).isEmpty();
  }
}
