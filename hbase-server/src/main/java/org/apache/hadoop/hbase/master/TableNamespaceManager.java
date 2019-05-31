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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.NavigableSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZKNamespaceManager;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.procedure.CreateNamespaceProcedure;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * This is a helper class used to manage the namespace
 * metadata that is stored in TableName.NAMESPACE_TABLE_NAME
 * It also mirrors updates to the ZK store by forwarding updates to
 * {@link org.apache.hadoop.hbase.ZKNamespaceManager}
 */
@InterfaceAudience.Private
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="IS2_INCONSISTENT_SYNC",
  justification="TODO: synchronize access on nsTable but it is done in tiers above and this " +
    "class is going away/shrinking")
public class TableNamespaceManager {
  private static final Log LOG = LogFactory.getLog(TableNamespaceManager.class);

  private Configuration conf;
  private MasterServices masterServices;
  private Table nsTable = null; // FindBugs: IS2_INCONSISTENT_SYNC TODO: Access is not synchronized
  private ZKNamespaceManager zkNamespaceManager;
  private boolean initialized;

  public static final String KEY_MAX_REGIONS = "hbase.namespace.quota.maxregions";
  public static final String KEY_MAX_TABLES = "hbase.namespace.quota.maxtables";
  static final String NS_INIT_TIMEOUT = "hbase.master.namespace.init.timeout";
  static final int DEFAULT_NS_INIT_TIMEOUT = 1800000; // default is 30 minutes
  private static final int WAIT_MESSAGE_TO_PRINTOUT = 300000; // print out message every 5 minutes

  public TableNamespaceManager(MasterServices masterServices) {
    this.masterServices = masterServices;
    this.conf = masterServices.getConfiguration();
  }

  public void start() throws IOException {
    if (!MetaTableAccessor.tableExists(masterServices.getConnection(),
      TableName.NAMESPACE_TABLE_NAME)) {
      LOG.info("Namespace table not found. Creating...");
      createNamespaceTable(masterServices);
    }

    try {
      // Wait for the namespace table to be assigned.
      // If timed out, we will move ahead without initializing it.
      // So that it should be initialized later on lazily.
      long startTime = EnvironmentEdgeManager.currentTime();
      long msgCount = 0;
      long waitTime;
      int timeout = conf.getInt(NS_INIT_TIMEOUT, DEFAULT_NS_INIT_TIMEOUT);
      while (!isTableAvailableAndInitialized(false)) {
        waitTime = EnvironmentEdgeManager.currentTime() - startTime;
        if (waitTime > timeout) {
          // We can't do anything if ns is not online.
          throw new IOException("Timedout " + timeout + "ms waiting for namespace table to " +
            "be assigned");
        } else if (waitTime > msgCount * WAIT_MESSAGE_TO_PRINTOUT) {
          LOG.info("Waiting for namespace table to be online. Time waited = " + waitTime + " ms.");
          msgCount++;
        }
        Thread.sleep(100);
      }
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException().initCause(e);
    }
  }

  @VisibleForTesting
  public boolean isTableNamespaceManagerStarted() {
    return initialized;
  }

  private synchronized Table getNamespaceTable() throws IOException {
    if (!isTableNamespaceManagerInitialized()) {
      throw new IOException(this.getClass().getName() + " isn't ready to serve");
    }
    return nsTable;
  }

  /*
   * check whether a namespace has already existed.
   */
  public boolean doesNamespaceExist(final String namespaceName) throws IOException {
    if (nsTable == null) {
      throw new IOException(this.getClass().getName() + " isn't ready to serve");
    }
    return (get(nsTable, namespaceName) != null);
  }

  public synchronized NamespaceDescriptor get(String name) throws IOException {
    if (!isTableNamespaceManagerInitialized()) {
      return null;
    }
    return zkNamespaceManager.get(name);
  }

  private NamespaceDescriptor get(Table table, String name) throws IOException {
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

  public void insertIntoNSTable(final NamespaceDescriptor ns) throws IOException {
    if (nsTable == null) {
      throw new IOException(this.getClass().getName() + " isn't ready to serve");
    }
    Put p = new Put(Bytes.toBytes(ns.getName()));
    p.addImmutable(HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES,
        HTableDescriptor.NAMESPACE_COL_DESC_BYTES,
        ProtobufUtil.toProtoNamespaceDescriptor(ns).toByteArray());
    nsTable.put(p);
  }

  public void updateZKNamespaceManager(final NamespaceDescriptor ns) throws IOException {
    try {
      zkNamespaceManager.update(ns);
    } catch (IOException ex) {
      String msg = "Failed to update namespace information in ZK.";
      LOG.error(msg, ex);
      throw new IOException(msg, ex);
    }
  }

  public void removeFromNSTable(final String namespaceName) throws IOException {
    if (nsTable == null) {
      throw new IOException(this.getClass().getName() + " isn't ready to serve");
    }
    Delete d = new Delete(Bytes.toBytes(namespaceName));
    nsTable.delete(d);
  }

  public void removeFromZKNamespaceManager(final String namespaceName) throws IOException {
    zkNamespaceManager.remove(namespaceName);
  }

  public synchronized NavigableSet<NamespaceDescriptor> list() throws IOException {
    NavigableSet<NamespaceDescriptor> ret =
        Sets.newTreeSet(NamespaceDescriptor.NAMESPACE_DESCRIPTOR_COMPARATOR);
    ResultScanner scanner =
        getNamespaceTable().getScanner(HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES);
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
    masterServices.createSystemTable(HTableDescriptor.NAMESPACE_TABLEDESC);
  }

  @SuppressWarnings("deprecation")
  private boolean isTableNamespaceManagerInitialized() throws IOException {
    if (initialized) {
      this.nsTable = this.masterServices.getConnection().getTable(TableName.NAMESPACE_TABLE_NAME);
      return true;
    }
    return false;
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
  public synchronized boolean isTableAvailableAndInitialized(
      final boolean createNamespaceAync) throws IOException {
    // Did we already get a table? If so, still make sure it's available
    if (isTableNamespaceManagerInitialized()) {
      return true;
    }

    if (zkNamespaceManager == null) {
      zkNamespaceManager = new ZKNamespaceManager(masterServices.getZooKeeper());
      zkNamespaceManager.start();
    }

    // Now check if the table is assigned, if not then fail fast
    if (isTableAssigned()) {
      try {
        boolean initGoodSofar = true;
        nsTable = this.masterServices.getConnection().getTable(TableName.NAMESPACE_TABLE_NAME);
        if (get(nsTable, NamespaceDescriptor.DEFAULT_NAMESPACE.getName()) == null) {
          if (createNamespaceAync) {
            masterServices.getMasterProcedureExecutor().submitProcedure(
              new CreateNamespaceProcedure(
                masterServices.getMasterProcedureExecutor().getEnvironment(),
                NamespaceDescriptor.DEFAULT_NAMESPACE));
            initGoodSofar = false;
          }
          else {
            masterServices.createNamespaceSync(
              NamespaceDescriptor.DEFAULT_NAMESPACE,
              HConstants.NO_NONCE,
              HConstants.NO_NONCE,
              false);
          }
        }
        if (get(nsTable, NamespaceDescriptor.SYSTEM_NAMESPACE.getName()) == null) {
          if (createNamespaceAync) {
            masterServices.getMasterProcedureExecutor().submitProcedure(
              new CreateNamespaceProcedure(
                masterServices.getMasterProcedureExecutor().getEnvironment(),
                NamespaceDescriptor.SYSTEM_NAMESPACE));
            initGoodSofar = false;
          }
          else {
            masterServices.createNamespaceSync(
              NamespaceDescriptor.SYSTEM_NAMESPACE,
              HConstants.NO_NONCE,
              HConstants.NO_NONCE,
              false);
          }
        }

        if (!initGoodSofar) {
          // some required namespace is created asynchronized. We should complete init later.
          return false;
        }

        ResultScanner scanner = nsTable.getScanner(HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES);
        try {
          for (Result result : scanner) {
            byte[] val =  CellUtil.cloneValue(result.getColumnLatestCell(
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
    return !masterServices.getAssignmentManager().getRegionStates().
        getRegionsOfTable(TableName.NAMESPACE_TABLE_NAME).isEmpty();
  }

  public void validateTableAndRegionCount(NamespaceDescriptor desc) throws IOException {
    if (getMaxRegions(desc) <= 0) {
      throw new ConstraintException("The max region quota for " + desc.getName()
          + " is less than or equal to zero.");
    }
    if (getMaxTables(desc) <= 0) {
      throw new ConstraintException("The max tables quota for " + desc.getName()
          + " is less than or equal to zero.");
    }
  }

  public static long getMaxTables(NamespaceDescriptor ns) throws IOException {
    String value = ns.getConfigurationValue(KEY_MAX_TABLES);
    long maxTables = 0;
    if (StringUtils.isNotEmpty(value)) {
      try {
        maxTables = Long.parseLong(value);
      } catch (NumberFormatException exp) {
        throw new DoNotRetryIOException("NumberFormatException while getting max tables.", exp);
      }
    } else {
      // The property is not set, so assume its the max long value.
      maxTables = Long.MAX_VALUE;
    }
    return maxTables;
  }

  public static long getMaxRegions(NamespaceDescriptor ns) throws IOException {
    String value = ns.getConfigurationValue(KEY_MAX_REGIONS);
    long maxRegions = 0;
    if (StringUtils.isNotEmpty(value)) {
      try {
        maxRegions = Long.parseLong(value);
      } catch (NumberFormatException exp) {
        throw new DoNotRetryIOException("NumberFormatException while getting max regions.", exp);
      }
    } else {
      // The property is not set, so assume its the max long value.
      maxRegions = Long.MAX_VALUE;
    }
    return maxRegions;
  }
}
