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

package org.apache.hadoop.hbase.fs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.fs.legacy.LegacyMasterFileSystem;
import org.apache.hadoop.hbase.fs.RegionStorage.StoreFileVisitor;
import org.apache.hadoop.hbase.fs.legacy.LegacyPathIdentifier;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

@InterfaceAudience.Private
public abstract class MasterStorage<IDENTIFIER extends StorageIdentifier> {
  private static Log LOG = LogFactory.getLog(MasterStorage.class);

  // Persisted unique cluster ID
  private ClusterId clusterId;


  private Configuration conf;
  private FileSystem fs;  // TODO: definitely remove
  private IDENTIFIER rootContainer;

  protected MasterStorage(Configuration conf, FileSystem fs, IDENTIFIER rootContainer) {
    this.rootContainer = rootContainer;
    this.conf = conf;
    this.fs = fs;
  }

  public Configuration getConfiguration() { return conf; }
  public FileSystem getFileSystem() { return fs; }  // TODO: definitely remove
  public IDENTIFIER getRootContainer() { return rootContainer; }

  // ==========================================================================
  //  PUBLIC Interfaces - Visitors
  // ==========================================================================
  public interface NamespaceVisitor {
    void visitNamespace(String namespace) throws IOException;
  }

  public interface TableVisitor {
    void visitTable(TableName tableName) throws IOException;
  }

  public interface RegionVisitor {
    void visitRegion(HRegionInfo regionInfo) throws IOException;
  }

  // ==========================================================================
  //  PUBLIC Methods - Namespace related
  // ==========================================================================
  public abstract void createNamespace(NamespaceDescriptor nsDescriptor) throws IOException;
  public abstract void deleteNamespace(String namespaceName) throws IOException;
  public abstract Collection<String> getNamespaces(StorageContext ctx) throws IOException;

  public Collection<String> getNamespaces() throws IOException {
    return getNamespaces(StorageContext.DATA);
  }
  // should return or get a NamespaceDescriptor? how is that different from HTD?

  // ==========================================================================
  //  PUBLIC Methods - Table Descriptor related
  // ==========================================================================
  public HTableDescriptor getTableDescriptor(TableName tableName)
      throws IOException {
    return getTableDescriptor(StorageContext.DATA, tableName);
  }

  public boolean createTableDescriptor(HTableDescriptor tableDesc, boolean force)
      throws IOException {
    return createTableDescriptor(StorageContext.DATA, tableDesc, force);
  }

  public void updateTableDescriptor(HTableDescriptor tableDesc) throws IOException {
    updateTableDescriptor(StorageContext.DATA, tableDesc);
  }

  public abstract HTableDescriptor getTableDescriptor(StorageContext ctx, TableName tableName)
      throws IOException;
  public abstract boolean createTableDescriptor(StorageContext ctx, HTableDescriptor tableDesc,
                                                boolean force) throws IOException;
  public abstract void updateTableDescriptor(StorageContext ctx, HTableDescriptor tableDesc)
      throws IOException;

  // ==========================================================================
  //  PUBLIC Methods - Table related
  // ==========================================================================
  public void deleteTable(TableName tableName) throws IOException {
    deleteTable(StorageContext.DATA, tableName);
  }

  public Collection<TableName> getTables(String namespace) throws IOException {
    return getTables(StorageContext.DATA, namespace);
  }

  public abstract void deleteTable(StorageContext ctx, TableName tableName) throws IOException;

  public abstract Collection<TableName> getTables(StorageContext ctx, String namespace)
    throws IOException;

  public Collection<TableName> getTables() throws IOException {
    ArrayList<TableName> tables = new ArrayList<TableName>();
    for (String ns: getNamespaces()) {
      tables.addAll(getTables(ns));
    }
    return tables;
  }

  // ==========================================================================
  //  PUBLIC Methods - Table Region related
  // ==========================================================================
  public void deleteRegion(HRegionInfo regionInfo) throws IOException {
    RegionStorage.destroy(conf, regionInfo);
  }

  public Collection<HRegionInfo> getRegions(TableName tableName) throws IOException {
    return getRegions(StorageContext.DATA, tableName);
  }

  public abstract Collection<HRegionInfo> getRegions(StorageContext ctx, TableName tableName)
    throws IOException;

  // TODO: Move in HRegionStorage
  public void deleteFamilyFromStorage(HRegionInfo regionInfo, byte[] familyName, boolean hasMob)
      throws IOException {
    getRegionStorage(regionInfo).deleteFamily(Bytes.toString(familyName), hasMob);
  }

  public RegionStorage getRegionStorage(HRegionInfo regionInfo) throws IOException {
    return RegionStorage.open(conf, regionInfo, false);
  }

  // ==========================================================================
  //  PUBLIC Methods - visitors
  // ==========================================================================
  public void visitStoreFiles(StoreFileVisitor visitor)
      throws IOException {
    visitStoreFiles(StorageContext.DATA, visitor);
  }

  public void visitStoreFiles(String namespace, StoreFileVisitor visitor)
      throws IOException {
    visitStoreFiles(StorageContext.DATA, namespace, visitor);
  }

  public void visitStoreFiles(TableName table, StoreFileVisitor visitor)
      throws IOException {
    visitStoreFiles(StorageContext.DATA, table, visitor);
  }

  public void visitStoreFiles(StorageContext ctx, StoreFileVisitor visitor)
      throws IOException {
    for (String namespace: getNamespaces()) {
      visitStoreFiles(ctx, namespace, visitor);
    }
  }

  public void visitStoreFiles(StorageContext ctx, String namespace, StoreFileVisitor visitor)
      throws IOException {
    for (TableName tableName: getTables(namespace)) {
      visitStoreFiles(ctx, tableName, visitor);
    }
  }

  public void visitStoreFiles(StorageContext ctx, TableName table, StoreFileVisitor visitor)
      throws IOException {
    for (HRegionInfo hri: getRegions(ctx, table)) {
      RegionStorage.open(conf, hri, false).visitStoreFiles(visitor);
    }
  }

  // ==========================================================================
  //  PUBLIC Methods - bootstrap
  // ==========================================================================
  public abstract IDENTIFIER getTempContainer();

  public abstract void logStorageState(Log log) throws IOException;

  /**
   * @return The unique identifier generated for this cluster
   */
  public ClusterId getClusterId() {
    return clusterId;
  }

  /**
   * Bootstrap MasterStorage
   * @throws IOException
   */
  protected void bootstrap() throws IOException {
    // Initialize
    clusterId = startup();

    // Make sure the meta region exists!
    bootstrapMeta();

    // check if temp directory exists and clean it
    startupCleanup();
  }

  protected abstract ClusterId startup() throws IOException;
  protected abstract void bootstrapMeta() throws IOException;
  protected abstract void startupCleanup() throws IOException;

  // ==========================================================================
  //  PUBLIC
  // ==========================================================================
  public static MasterStorage open(Configuration conf, boolean bootstrap)
      throws IOException {
    return open(conf, FSUtils.getRootDir(conf), bootstrap);
  }

  public static MasterStorage open(Configuration conf, Path rootDir, boolean bootstrap)
      throws IOException {
    // Cover both bases, the old way of setting default fs and the new.
    // We're supposed to run on 0.20 and 0.21 anyways.
    FileSystem fs = rootDir.getFileSystem(conf);
    FSUtils.setFsDefault(conf, new Path(fs.getUri()));
    // make sure the fs has the same conf
    fs.setConf(conf);

    MasterStorage ms = getInstance(conf, fs, rootDir);
    if (bootstrap) {
      ms.bootstrap();
    }
    HFileSystem.addLocationsOrderInterceptor(conf);
    return ms;
  }

  private static MasterStorage getInstance(Configuration conf, final FileSystem fs,
                                           Path rootDir) throws IOException {
    String storageType = conf.get("hbase.storage.type", "legacy").toLowerCase();
    switch (storageType) {
      case "legacy":
        return new LegacyMasterFileSystem(conf, fs, new LegacyPathIdentifier(rootDir));
      default:
        throw new IOException("Invalid filesystem type " + storageType);
    }
  }
}
