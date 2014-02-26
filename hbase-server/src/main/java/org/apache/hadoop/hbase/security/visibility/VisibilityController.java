/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.security.visibility;

import static org.apache.hadoop.hbase.HConstants.OperationStatusCode.SANITY_CHECK_FAILURE;
import static org.apache.hadoop.hbase.HConstants.OperationStatusCode.SUCCESS;
import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_FAMILY;
import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_NAME;
import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABEL_QUALIFIER;
import static org.apache.hadoop.hbase.security.visibility.VisibilityUtils.SYSTEM_LABEL;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.protobuf.HBaseZeroCopyByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionActionResult;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.GetAuthsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.GetAuthsResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.SetAuthsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabel;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsService;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.visibility.expression.ExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.LeafExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.NonLeafExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.Operator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * Coprocessor that has both the MasterObserver and RegionObserver implemented that supports in
 * visibility labels
 */
@InterfaceAudience.Private
public class VisibilityController extends BaseRegionObserver implements MasterObserver,
    RegionObserver, VisibilityLabelsService.Interface, CoprocessorService {

  private static final Log LOG = LogFactory.getLog(VisibilityController.class);
  private static final byte[] DUMMY_VALUE = new byte[0];
  // "system" label is having an ordinal value 1.
  private static final int SYSTEM_LABEL_ORDINAL = 1;
  private static final Tag[] LABELS_TABLE_TAGS = new Tag[1];

  private final ExpressionParser expressionParser = new ExpressionParser();
  private final ExpressionExpander expressionExpander = new ExpressionExpander();
  private VisibilityLabelsManager visibilityManager;
  // defined only for Endpoint implementation, so it can have way to access region services.
  private RegionCoprocessorEnvironment regionEnv;
  private ScanLabelGenerator scanLabelGenerator;
  
  private volatile int ordinalCounter = -1;
  // flags if we are running on a region of the 'labels' table
  private boolean labelsRegion = false;
  // Flag denoting whether AcessController is available or not.
  private boolean acOn = false;
  private Configuration conf;
  private volatile boolean initialized = false;

  /** Mapping of scanner instances to the user who created them */
  private Map<InternalScanner,String> scannerOwners =
      new MapMaker().weakKeys().makeMap();

  static {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    try {
      StreamUtils.writeRawVInt32(dos, SYSTEM_LABEL_ORDINAL);
    } catch (IOException e) {
      // We write to a byte array. No Exception can happen.
    }
    LABELS_TABLE_TAGS[0] = new Tag(VisibilityUtils.VISIBILITY_TAG_TYPE, baos.toByteArray());
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    this.conf = env.getConfiguration();
    if (HFile.getFormatVersion(conf) < HFile.MIN_FORMAT_VERSION_WITH_TAGS) {
      throw new RuntimeException("A minimum HFile version of " + HFile.MIN_FORMAT_VERSION_WITH_TAGS
        + " is required to persist visibility labels. Consider setting " + HFile.FORMAT_VERSION_KEY
        + " accordingly.");
    }
    ZooKeeperWatcher zk = null;
    if (env instanceof MasterCoprocessorEnvironment) {
      // if running on HMaster
      MasterCoprocessorEnvironment mEnv = (MasterCoprocessorEnvironment) env;
      zk = mEnv.getMasterServices().getZooKeeper();
    } else if (env instanceof RegionCoprocessorEnvironment) {
      // if running at region
      regionEnv = (RegionCoprocessorEnvironment) env;
      zk = regionEnv.getRegionServerServices().getZooKeeper();
    } else if (env instanceof RegionServerCoprocessorEnvironment) {
      throw new RuntimeException(
          "Visibility controller should not be configured as " +
          "'hbase.coprocessor.regionserver.classes'.");
    }

    // If zk is null or IOException while obtaining auth manager,
    // throw RuntimeException so that the coprocessor is unloaded.
    if (zk == null) {
      throw new RuntimeException("Error obtaining VisibilityLabelsManager, zk found null.");
    }
    try {
      this.visibilityManager = VisibilityLabelsManager.get(zk, this.conf);
    } catch (IOException ioe) {
      throw new RuntimeException("Error obtaining VisibilityLabelsManager", ioe);
    }
    if (env instanceof RegionCoprocessorEnvironment) {
      // ScanLabelGenerator to be instantiated only with Region Observer.
      scanLabelGenerator = VisibilityUtils.getScanLabelGenerator(this.conf);
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {

  }

  /********************************* Master related hooks **********************************/

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
    // Need to create the new system table for labels here
    MasterServices master = ctx.getEnvironment().getMasterServices();
    if (!MetaReader.tableExists(master.getCatalogTracker(), LABELS_TABLE_NAME)) {
      HTableDescriptor labelsTable = new HTableDescriptor(LABELS_TABLE_NAME);
      HColumnDescriptor labelsColumn = new HColumnDescriptor(LABELS_TABLE_FAMILY);
      labelsColumn.setBloomFilterType(BloomType.NONE);
      labelsColumn.setBlockCacheEnabled(false); // We will cache all the labels. No need of normal
                                                 // table block cache.
      labelsTable.addFamily(labelsColumn);
      // Let the "labels" table having only one region always. We are not expecting too many labels in
      // the system.
      labelsTable.setValue(HTableDescriptor.SPLIT_POLICY,
          DisabledRegionSplitPolicy.class.getName());
      labelsTable.setValue(Bytes.toBytes(HConstants.DISALLOW_WRITES_IN_RECOVERING),
          Bytes.toBytes(true));
      master.createTable(labelsTable, null);
    }
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void preCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void postCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void preDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void postDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HTableDescriptor htd) throws IOException {
    if (LABELS_TABLE_NAME.equals(tableName)) {
      throw new ConstraintException("Cannot alter " + LABELS_TABLE_NAME);
    }
  }

  @Override
  public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HTableDescriptor htd) throws IOException {
  }

  @Override
  public void preModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HTableDescriptor htd) throws IOException {
  }

  @Override
  public void postModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HTableDescriptor htd) throws IOException {
  }

  @Override
  public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      HColumnDescriptor column) throws IOException {
    if (LABELS_TABLE_NAME.equals(tableName)) {
      throw new ConstraintException("Cannot alter " + LABELS_TABLE_NAME);
    }
  }

  @Override
  public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      HColumnDescriptor column) throws IOException {
  }

  @Override
  public void preAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor column) throws IOException {
  }

  @Override
  public void postAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor column) throws IOException {
  }

  @Override
  public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor descriptor) throws IOException {
    if (LABELS_TABLE_NAME.equals(tableName)) {
      throw new ConstraintException("Cannot alter " + LABELS_TABLE_NAME);
    }
  }

  @Override
  public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor descriptor) throws IOException {
  }

  @Override
  public void preModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor descriptor) throws IOException {
  }

  @Override
  public void postModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor descriptor) throws IOException {
  }

  @Override
  public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, byte[] c) throws IOException {
    if (LABELS_TABLE_NAME.equals(tableName)) {
      throw new ConstraintException("Cannot alter " + LABELS_TABLE_NAME);
    }
  }

  @Override
  public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, byte[] c) throws IOException {
  }

  @Override
  public void preDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, byte[] c) throws IOException {
  }

  @Override
  public void postDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, byte[] c) throws IOException {
  }

  @Override
  public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void preEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void postEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
    if (LABELS_TABLE_NAME.equals(tableName)) {
      throw new ConstraintException("Cannot disable " + LABELS_TABLE_NAME);
    }
  }

  @Override
  public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void preDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void postDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void preMove(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo region,
      ServerName srcServer, ServerName destServer) throws IOException {
  }

  @Override
  public void postMove(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo region,
      ServerName srcServer, ServerName destServer) throws IOException {
  }

  @Override
  public void preAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo)
      throws IOException {
  }

  @Override
  public void postAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo)
      throws IOException {
  }

  @Override
  public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo, boolean force) throws IOException {
  }

  @Override
  public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo, boolean force) throws IOException {
  }

  @Override
  public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo) throws IOException {
  }

  @Override
  public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo) throws IOException {
  }

  @Override
  public void preBalance(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
  }

  @Override
  public void postBalance(ObserverContext<MasterCoprocessorEnvironment> ctx, List<RegionPlan> plans)
      throws IOException {
  }

  @Override
  public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx,
      boolean newValue) throws IOException {
    return false;
  }

  @Override
  public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx,
      boolean oldValue, boolean newValue) throws IOException {
  }

  @Override
  public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
  }

  @Override
  public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
  }

  @Override
  public void preSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
  }

  @Override
  public void postSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
  }

  @Override
  public void preCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
  }

  @Override
  public void postCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
  }

  @Override
  public void preRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
  }

  @Override
  public void postRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
  }

  @Override
  public void preDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot) throws IOException {
  }

  @Override
  public void postDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot) throws IOException {
  }

  @Override
  public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<TableName> tableNamesList, List<HTableDescriptor> descriptors) throws IOException {
  }

  @Override
  public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<HTableDescriptor> descriptors) throws IOException {
  }

  @Override
  public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
  }

  @Override
  public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
  }

  @Override
  public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, 
      String namespace) throws IOException {
  }

  @Override
  public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      String namespace) throws IOException {
  }

  @Override
  public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
  }

  @Override
  public void postModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
  }

  @Override
  public void preMasterInitialization(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {

  }

  /****************************** Region related hooks ******************************/

  @Override
  public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
    // Read the entire labels table and populate the zk
    if (e.getEnvironment().getRegion().getRegionInfo().getTable().equals(LABELS_TABLE_NAME)) {
      this.labelsRegion = true;
      this.acOn = CoprocessorHost.getLoadedCoprocessors().contains(AccessController.class.getName());
      if (!e.getEnvironment().getRegion().isRecovering()) {
        initialize(e);
      }
    } else {
      this.initialized = true;
    }
  }

  @Override
  public void postLogReplay(ObserverContext<RegionCoprocessorEnvironment> e) {
    if (this.labelsRegion) {
      initialize(e);
    }
  }

  private void initialize(ObserverContext<RegionCoprocessorEnvironment> e) {
    try {
      Pair<Map<String, Integer>, Map<String, List<Integer>>> labelsAndUserAuths = 
          extractLabelsAndAuths(getExistingLabelsWithAuths());
      Map<String, Integer> labels = labelsAndUserAuths.getFirst();
      Map<String, List<Integer>> userAuths = labelsAndUserAuths.getSecond();
      // Add the "system" label if it is not added into the system yet
      addSystemLabel(e.getEnvironment().getRegion(), labels, userAuths);
      int ordinal = 1; // Ordinal 1 is reserved for "system" label.
      for (Integer i : labels.values()) {
        if (i > ordinal) {
          ordinal = i;
        }
      }
      this.ordinalCounter = ordinal + 1;
      if (labels.size() > 0) {
        // If there is no data need not write to zk
        byte[] serialized = VisibilityUtils.getDataToWriteToZooKeeper(labels);
        this.visibilityManager.writeToZookeeper(serialized, true);
      }
      if (userAuths.size() > 0) {
        byte[] serialized = VisibilityUtils.getUserAuthsDataToWriteToZooKeeper(userAuths);
        this.visibilityManager.writeToZookeeper(serialized, false);
      }
      initialized = true;
    } catch (IOException ioe) {
      LOG.error("Error while updating the zk with the exisiting labels data", ioe);
    }
  }

  private void addSystemLabel(HRegion region, Map<String, Integer> labels,
      Map<String, List<Integer>> userAuths) throws IOException {
    if (!labels.containsKey(SYSTEM_LABEL)) {
      Put p = new Put(Bytes.toBytes(SYSTEM_LABEL_ORDINAL));
      p.addImmutable(LABELS_TABLE_FAMILY, LABEL_QUALIFIER, Bytes.toBytes(SYSTEM_LABEL));
      // Set auth for "system" label for all super users.
      List<String> superUsers = getSystemAndSuperUsers();
      for (String superUser : superUsers) {
        p.addImmutable(
            LABELS_TABLE_FAMILY, Bytes.toBytes(superUser), DUMMY_VALUE, LABELS_TABLE_TAGS);
      }
      region.put(p);
      labels.put(SYSTEM_LABEL, SYSTEM_LABEL_ORDINAL);
      for (String superUser : superUsers) {
        List<Integer> auths = userAuths.get(superUser);
        if (auths == null) {
          auths = new ArrayList<Integer>(1);
          userAuths.put(superUser, auths);
        }
        auths.add(SYSTEM_LABEL_ORDINAL);
      }
    }
  }

  @Override
  public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    if (c.getEnvironment().getRegion().getRegionInfo().getTable().isSystemTable()) {
      return;
    }
    // TODO this can be made as a global LRU cache at HRS level?
    Map<String, List<Tag>> labelCache = new HashMap<String, List<Tag>>();
    for (int i = 0; i < miniBatchOp.size(); i++) {
      Mutation m = miniBatchOp.getOperation(i);
      CellVisibility cellVisibility = null;
      try {
        cellVisibility = m.getCellVisibility();
      } catch (DeserializationException de) {
        miniBatchOp.setOperationStatus(i,
            new OperationStatus(SANITY_CHECK_FAILURE, de.getMessage()));
        continue;
      }
      if (m instanceof Put) {
        Put p = (Put) m;
        boolean sanityFailure = false;
        for (CellScanner cellScanner = p.cellScanner(); cellScanner.advance();) {
          if (!checkForReservedVisibilityTagPresence(cellScanner.current())) {
            miniBatchOp.setOperationStatus(i, new OperationStatus(SANITY_CHECK_FAILURE,
                "Mutation contains cell with reserved type tag"));
            sanityFailure = true;
            break;
          }
        }
        if (!sanityFailure) {
          if (cellVisibility != null) {
            String labelsExp = cellVisibility.getExpression();
            List<Tag> visibilityTags = labelCache.get(labelsExp);
            if (visibilityTags == null) {
              try {
                visibilityTags = createVisibilityTags(labelsExp);
              } catch (ParseException e) {
                miniBatchOp.setOperationStatus(i,
                    new OperationStatus(SANITY_CHECK_FAILURE, e.getMessage()));
              } catch (InvalidLabelException e) {
                miniBatchOp.setOperationStatus(i,
                    new OperationStatus(SANITY_CHECK_FAILURE, e.getMessage()));
              }
            }
            if (visibilityTags != null) {
              labelCache.put(labelsExp, visibilityTags);
              List<Cell> updatedCells = new ArrayList<Cell>();
              for (CellScanner cellScanner = p.cellScanner(); cellScanner.advance();) {
                Cell cell = cellScanner.current();
                List<Tag> tags = Tag.asList(cell.getTagsArray(), cell.getTagsOffset(),
                    cell.getTagsLength());
                tags.addAll(visibilityTags);
                Cell updatedCell = new KeyValue(cell.getRowArray(), cell.getRowOffset(),
                    cell.getRowLength(), cell.getFamilyArray(), cell.getFamilyOffset(),
                    cell.getFamilyLength(), cell.getQualifierArray(), cell.getQualifierOffset(),
                    cell.getQualifierLength(), cell.getTimestamp(), Type.codeToType(cell
                        .getTypeByte()), cell.getValueArray(), cell.getValueOffset(),
                    cell.getValueLength(), tags);
                updatedCells.add(updatedCell);
              }
              p.getFamilyCellMap().clear();
              // Clear and add new Cells to the Mutation.
              for (Cell cell : updatedCells) {
                p.add(cell);
              }
            }
          }
        }
      } else if (cellVisibility != null) {
        // CellVisibility in a Delete is not legal! Fail the operation
        miniBatchOp.setOperationStatus(i, new OperationStatus(SANITY_CHECK_FAILURE,
            "CellVisibility cannot be set on Delete mutation"));
      }
    }
  }

  @Override
  public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    if (this.labelsRegion) {
      // We will add to zookeeper here.
      Pair<Map<String, Integer>, Map<String, List<Integer>>> labelsAndUserAuths =
          extractLabelsAndAuths(getExistingLabelsWithAuths());
      Map<String, Integer> existingLabels = labelsAndUserAuths.getFirst();
      Map<String, List<Integer>> userAuths = labelsAndUserAuths.getSecond();
      boolean isNewLabels = false;
      boolean isUserAuthsChange = false;
      for (int i = 0; i < miniBatchOp.size(); i++) {
        Mutation m = miniBatchOp.getOperation(i);
        if (miniBatchOp.getOperationStatus(i).getOperationStatusCode() == SUCCESS) {
          for (List<Cell> cells : m.getFamilyCellMap().values()) {
            for (Cell cell : cells) {
              int labelOrdinal = Bytes.toInt(cell.getRowArray(), cell.getRowOffset());
              if (Bytes.equals(cell.getQualifierArray(), cell.getQualifierOffset(),
                      cell.getQualifierLength(), LABEL_QUALIFIER, 0,
                      LABEL_QUALIFIER.length)) {
                if (m instanceof Put) {
                  existingLabels.put(
                      Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                          cell.getValueLength()), labelOrdinal);
                  isNewLabels = true;
                }
              } else {
                String user = Bytes.toString(cell.getQualifierArray(),
                    cell.getQualifierOffset(), cell.getQualifierLength());
                List<Integer> auths = userAuths.get(user);
                if (auths == null) {
                  auths = new ArrayList<Integer>();
                  userAuths.put(user, auths);
                }
                if (m instanceof Delete) {
                  auths.remove(Integer.valueOf(labelOrdinal));
                } else {
                  auths.add(labelOrdinal);
                }
                isUserAuthsChange = true;
              }
            }
          }
        }
      }
      if (isNewLabels) {
        byte[] serialized = VisibilityUtils.getDataToWriteToZooKeeper(existingLabels);
        this.visibilityManager.writeToZookeeper(serialized, true);
      }
      if (isUserAuthsChange) {
        byte[] serialized = VisibilityUtils.getUserAuthsDataToWriteToZooKeeper(userAuths);
        this.visibilityManager.writeToZookeeper(serialized, false);
      }
    }
  }

  private Pair<Map<String, Integer>, Map<String, List<Integer>>> extractLabelsAndAuths(
      List<List<Cell>> labelDetails) {
    Map<String, Integer> labels = new HashMap<String, Integer>();
    Map<String, List<Integer>> userAuths = new HashMap<String, List<Integer>>();
    for (List<Cell> cells : labelDetails) {
      for (Cell cell : cells) {
        if (Bytes.equals(cell.getQualifierArray(), cell.getQualifierOffset(),
            cell.getQualifierLength(), LABEL_QUALIFIER, 0, LABEL_QUALIFIER.length)) {
          labels.put(
              Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()),
              Bytes.toInt(cell.getRowArray(), cell.getRowOffset()));
        } else {
          // These are user cells who has authorization for this label
          String user = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
              cell.getQualifierLength());
          List<Integer> auths = userAuths.get(user);
          if (auths == null) {
            auths = new ArrayList<Integer>();
            userAuths.put(user, auths);
          }
          auths.add(Bytes.toInt(cell.getRowArray(), cell.getRowOffset()));
        }
      }
    }
    return new Pair<Map<String, Integer>, Map<String, List<Integer>>>(labels, userAuths);
  }

  // Checks whether cell contains any tag with type as VISIBILITY_TAG_TYPE.
  // This tag type is reserved and should not be explicitly set by user.
  private boolean checkForReservedVisibilityTagPresence(Cell cell) throws IOException {
    if (cell.getTagsLength() > 0) {
      Iterator<Tag> tagsItr = CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(),
          cell.getTagsLength());
      while (tagsItr.hasNext()) {
        if (tagsItr.next().getType() == VisibilityUtils.VISIBILITY_TAG_TYPE) {
          return false;
        }
      }
    }
    return true;
  }

  private List<Tag> createVisibilityTags(String visibilityLabelsExp) throws IOException,
      ParseException, InvalidLabelException {
    ExpressionNode node = null;
    node = this.expressionParser.parse(visibilityLabelsExp);
    node = this.expressionExpander.expand(node);
    List<Tag> tags = new ArrayList<Tag>();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    if (node.isSingleNode()) {
      writeLabelOrdinalsToStream(node, dos);
      tags.add(new Tag(VisibilityUtils.VISIBILITY_TAG_TYPE, baos.toByteArray()));
      baos.reset();
    } else {
      NonLeafExpressionNode nlNode = (NonLeafExpressionNode) node;
      if (nlNode.getOperator() == Operator.OR) {
        for (ExpressionNode child : nlNode.getChildExps()) {
          writeLabelOrdinalsToStream(child, dos);
          tags.add(new Tag(VisibilityUtils.VISIBILITY_TAG_TYPE, baos.toByteArray()));
          baos.reset();
        }
      } else {
        writeLabelOrdinalsToStream(nlNode, dos);
        tags.add(new Tag(VisibilityUtils.VISIBILITY_TAG_TYPE, baos.toByteArray()));
        baos.reset();
      }
    }
    return tags;
  }

  private void writeLabelOrdinalsToStream(ExpressionNode node, DataOutputStream dos)
      throws IOException, InvalidLabelException {
    if (node.isSingleNode()) {
      String identifier = null;
      int labelOrdinal = 0;
      if (node instanceof LeafExpressionNode) {
        identifier = ((LeafExpressionNode) node)
            .getIdentifier();
        if (LOG.isTraceEnabled()) {
          LOG.trace("The identifier is "+identifier);
        }
        labelOrdinal = this.visibilityManager.getLabelOrdinal(identifier);
      } else {
        // This is a NOT node.
        LeafExpressionNode lNode = (LeafExpressionNode) ((NonLeafExpressionNode) node)
            .getChildExps().get(0);
        identifier = lNode.getIdentifier();
        labelOrdinal = this.visibilityManager.getLabelOrdinal(identifier);
        labelOrdinal = -1 * labelOrdinal; // Store NOT node as -ve ordinal.
      }
      if (labelOrdinal == 0) {
        throw new InvalidLabelException("Invalid visibility label " + identifier);
      }
      StreamUtils.writeRawVInt32(dos, labelOrdinal);
    } else {
      List<ExpressionNode> childExps = ((NonLeafExpressionNode) node).getChildExps();
      for (ExpressionNode child : childExps) {
        writeLabelOrdinalsToStream(child, dos);
      }
    }
  }
  
  @Override
  public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan,
      RegionScanner s) throws IOException {
    HRegion region = e.getEnvironment().getRegion();
    Authorizations authorizations = null;
    // If a super user issues a scan, he should be able to scan the cells
    // irrespective of the Visibility labels
    if (checkIfScanOrGetFromSuperUser()) {
      return s;
    }
    try {
      authorizations = scan.getAuthorizations();
    } catch (DeserializationException de) {
      throw new IOException(de);
    }
    Filter visibilityLabelFilter = createVisibilityLabelFilter(region, authorizations);
    if (visibilityLabelFilter != null) {
      Filter filter = scan.getFilter();
      if (filter != null) {
        scan.setFilter(new FilterList(filter, visibilityLabelFilter));
      } else {
        scan.setFilter(visibilityLabelFilter);
      }
    }
    return s;
  }

  private boolean checkIfScanOrGetFromSuperUser() throws IOException {
    User user = getActiveUser();
    if (user != null && user.getShortName() != null) {
      List<String> auths = this.visibilityManager.getAuths(user.getShortName());
      return (auths.contains(SYSTEM_LABEL));
    }
    return false;
  }

  @Override
  public RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Scan scan, final RegionScanner s) throws IOException {
    User user = getActiveUser();
    if (user != null && user.getShortName() != null) {
      scannerOwners.put(s, user.getShortName());
    }
    return s;
  }

  @Override
  public boolean preScannerNext(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s, final List<Result> result, final int limit, final boolean hasNext)
      throws IOException {
    requireScannerOwner(s);
    return hasNext;
  }

  @Override
  public void preScannerClose(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s) throws IOException {
    requireScannerOwner(s);
  }

  @Override
  public void postScannerClose(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s) throws IOException {
    // clean up any associated owner mapping
    scannerOwners.remove(s);
  }

  /**
   * Verify, when servicing an RPC, that the caller is the scanner owner. If so, we assume that
   * access control is correctly enforced based on the checks performed in preScannerOpen()
   */
  private void requireScannerOwner(InternalScanner s) throws AccessDeniedException {
    if (RequestContext.isInRequestContext()) {
      String requestUName = RequestContext.getRequestUserName();
      String owner = scannerOwners.get(s);
      if (owner != null && !owner.equals(requestUName)) {
        throw new AccessDeniedException("User '" + requestUName + "' is not the scanner owner!");
      }
    }
  }

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
      throws IOException {
    Authorizations authorizations = null;
    // If a super user issues a get, he should be able to scan the cells
    // irrespective of the Visibility labels
    if (checkIfScanOrGetFromSuperUser()) {
      return;
    }
    try {
      authorizations = get.getAuthorizations();
    } catch (DeserializationException de) {
      throw new IOException(de);
    }
    Filter visibilityLabelFilter = createVisibilityLabelFilter(e.getEnvironment().getRegion(),
        authorizations);
    if (visibilityLabelFilter != null) {
      Filter filter = get.getFilter();
      if (filter != null) {
        get.setFilter(new FilterList(filter, visibilityLabelFilter));
      } else {
        get.setFilter(visibilityLabelFilter);
      }
    }
  }

  private Filter createVisibilityLabelFilter(HRegion region, Authorizations authorizations) {
    if (authorizations == null) {
      // No Authorizations present for this scan/Get!
      // In case of "labels" table and user tables, create an empty auth set. In other system tables
      // just scan with out visibility check and filtering. Checking visibility labels for META and
      // NAMESPACE table is not needed.
      TableName table = region.getRegionInfo().getTable();
      if (table.isSystemTable() && !table.equals(LABELS_TABLE_NAME)) {
        return null;
      }
      return new VisibilityLabelFilter(new BitSet(0));
    }
    Filter visibilityLabelFilter = null;
    if (this.scanLabelGenerator != null) {
      List<String> labels = null;
      try {
        labels = this.scanLabelGenerator.getLabels(getActiveUser(), authorizations);
      } catch (Throwable t) {
        LOG.error(t);
      }
      int labelsCount = this.visibilityManager.getLabelsCount();
      BitSet bs = new BitSet(labelsCount + 1); // ordinal is index 1 based
      if (labels != null) {
        for (String label : labels) {
          int labelOrdinal = this.visibilityManager.getLabelOrdinal(label);
          if (labelOrdinal != 0) {
            bs.set(labelOrdinal);
          }
        }
      }
      visibilityLabelFilter = new VisibilityLabelFilter(bs);
    }
    return visibilityLabelFilter;
  }

  private User getActiveUser() throws IOException {
    User user = RequestContext.getRequestUser();
    if (!RequestContext.isInRequestContext()) {
      // for non-rpc handling, fallback to system user
      user = User.getCurrent();
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Current active user name is "+user.getShortName());
    }
    return user;
  }

  private List<String> getSystemAndSuperUsers() throws IOException {
    User user = User.getCurrent();
    if (user == null) {
      throw new IOException("Unable to obtain the current user, "
          + "authorization checks for internal operations will not work correctly!");
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Current user name is "+user.getShortName());
    }
    String currentUser = user.getShortName();
    List<String> superUsers = Lists.asList(currentUser,
        this.conf.getStrings(AccessControlLists.SUPERUSER_CONF_KEY, new String[0]));
    return superUsers;
  }

  private boolean isSystemOrSuperUser() throws IOException {
    List<String> superUsers = getSystemAndSuperUsers();
    User activeUser = getActiveUser();
    return superUsers.contains(activeUser.getShortName());
  }

  @Override
  public Cell postMutationBeforeWAL(ObserverContext<RegionCoprocessorEnvironment> ctx,
      MutationType opType, Mutation mutation, Cell oldCell, Cell newCell) throws IOException {
    List<Tag> tags = Lists.newArrayList();
    CellVisibility cellVisibility = null;
    try {
      cellVisibility = mutation.getCellVisibility();
    } catch (DeserializationException e) {
      throw new IOException(e);
    }
    if (cellVisibility == null) {
      return newCell;
    }
    // Adding all other tags
    Iterator<Tag> tagsItr = CellUtil.tagsIterator(newCell.getTagsArray(), newCell.getTagsOffset(),
        newCell.getTagsLength());
    while (tagsItr.hasNext()) {
      Tag tag = tagsItr.next();
      if (tag.getType() != VisibilityUtils.VISIBILITY_TAG_TYPE) {
        tags.add(tag);
      }
    }
    try {
      tags.addAll(createVisibilityTags(cellVisibility.getExpression()));
    } catch (ParseException e) {
      throw new IOException(e);
    }

    // We need to create another KV, unfortunately, because the current new KV
    // has no space for tags
    KeyValue newKv = KeyValueUtil.ensureKeyValue(newCell);
    byte[] bytes = newKv.getBuffer();
    KeyValue rewriteKv = new KeyValue(bytes, newKv.getRowOffset(), newKv.getRowLength(), bytes,
        newKv.getFamilyOffset(), newKv.getFamilyLength(), bytes, newKv.getQualifierOffset(),
        newKv.getQualifierLength(), newKv.getTimestamp(), KeyValue.Type.codeToType(newKv
            .getTypeByte()), bytes, newKv.getValueOffset(), newKv.getValueLength(), tags);
    // Preserve mvcc data
    rewriteKv.setMvccVersion(newKv.getMvccVersion());
    return rewriteKv;
  }

  @Override
  public Service getService() {
    return VisibilityLabelsProtos.VisibilityLabelsService.newReflectiveService(this);
  }

  /****************************** VisibilityEndpoint service related methods ******************************/
  @Override
  public synchronized void addLabels(RpcController controller, VisibilityLabelsRequest request,
      RpcCallback<VisibilityLabelsResponse> done) {
    VisibilityLabelsResponse.Builder response = VisibilityLabelsResponse.newBuilder();
    List<VisibilityLabel> labels = request.getVisLabelList();
    if (!initialized) {
      setExceptionResults(labels.size(), new CoprocessorException(
          "VisibilityController not yet initialized"), response);
    }
    try {
      checkCallingUserAuth();
      List<Mutation> puts = new ArrayList<Mutation>(labels.size());
      RegionActionResult successResult = RegionActionResult.newBuilder().build();
      for (VisibilityLabel visLabel : labels) {
        byte[] label = visLabel.getLabel().toByteArray();
        String labelStr = Bytes.toString(label);
        if (VisibilityLabelsValidator.isValidLabel(label)) {
          if (this.visibilityManager.getLabelOrdinal(labelStr) > 0) {
            RegionActionResult.Builder failureResultBuilder = RegionActionResult.newBuilder();
            failureResultBuilder.setException(ResponseConverter
                .buildException(new LabelAlreadyExistsException("Label '" + labelStr
                    + "' already exists")));
            response.addResult(failureResultBuilder.build());
          } else {
            Put p = new Put(Bytes.toBytes(ordinalCounter));
            p.addImmutable(
                LABELS_TABLE_FAMILY, LABEL_QUALIFIER, label, LABELS_TABLE_TAGS);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding the label "+labelStr);
            }
            puts.add(p);
            ordinalCounter++;
            response.addResult(successResult);
          }
        } else {
          RegionActionResult.Builder failureResultBuilder = RegionActionResult.newBuilder();
          failureResultBuilder.setException(ResponseConverter
              .buildException(new InvalidLabelException("Invalid visibility label '" + labelStr
                  + "'")));
          response.addResult(failureResultBuilder.build());
        }
      }
      OperationStatus[] opStatus = this.regionEnv.getRegion().batchMutate(
          puts.toArray(new Mutation[puts.size()]));
      int i = 0;
      for (OperationStatus status : opStatus) {
        if (status.getOperationStatusCode() != SUCCESS) {
          while (response.getResult(i) != successResult)
            i++;
          RegionActionResult.Builder failureResultBuilder = RegionActionResult.newBuilder();
          failureResultBuilder.setException(ResponseConverter
              .buildException(new DoNotRetryIOException(status.getExceptionMsg())));
          response.setResult(i, failureResultBuilder.build());
        }
        i++;
      }
    } catch (IOException e) {
      LOG.error(e);
      setExceptionResults(labels.size(), e, response);
    }
    done.run(response.build());
  }

  private void setExceptionResults(int size, IOException e,
      VisibilityLabelsResponse.Builder response) {
    RegionActionResult.Builder failureResultBuilder = RegionActionResult.newBuilder();
    failureResultBuilder.setException(ResponseConverter.buildException(e));
    RegionActionResult failureResult = failureResultBuilder.build();
    for (int i = 0; i < size; i++) {
      response.addResult(i, failureResult);
    }
  }

  private void performACLCheck() throws IOException {
    // Do ACL check only when the security is enabled.
    if (this.acOn && !isSystemOrSuperUser()) {
      User user = getActiveUser();
      throw new AccessDeniedException("User '" + (user != null ? user.getShortName() : "null")
          + " is not authorized to perform this action.");
    }
  }

  private List<List<Cell>> getExistingLabelsWithAuths() throws IOException {
    Scan scan = new Scan();
    RegionScanner scanner = this.regionEnv.getRegion().getScanner(scan);
    List<List<Cell>> existingLabels = new ArrayList<List<Cell>>();
    try {
      while (true) {
        List<Cell> cells = new ArrayList<Cell>();
        scanner.next(cells);
        if (cells.isEmpty()) {
          break;
        }
        existingLabels.add(cells);
      }
    } finally {
      scanner.close();
    }
    return existingLabels;
  }

  @Override
  public synchronized void setAuths(RpcController controller, SetAuthsRequest request,
      RpcCallback<VisibilityLabelsResponse> done) {
    VisibilityLabelsResponse.Builder response = VisibilityLabelsResponse.newBuilder();
    List<ByteString> auths = request.getAuthList();
    if (!initialized) {
      setExceptionResults(auths.size(), new CoprocessorException(
          "VisibilityController not yet initialized"), response);
    }
    byte[] user = request.getUser().toByteArray();
    try {
      checkCallingUserAuth();
      List<Mutation> puts = new ArrayList<Mutation>(auths.size());
      RegionActionResult successResult = RegionActionResult.newBuilder().build();
      for (ByteString authBS : auths) {
        byte[] auth = authBS.toByteArray();
        String authStr = Bytes.toString(auth);
        int labelOrdinal = this.visibilityManager.getLabelOrdinal(authStr);
        if (labelOrdinal == 0) {
          // This label is not yet added. 1st this should be added to the system
          RegionActionResult.Builder failureResultBuilder = RegionActionResult.newBuilder();
          failureResultBuilder.setException(ResponseConverter
              .buildException(new InvalidLabelException("Label '" + authStr + "' doesn't exist")));
          response.addResult(failureResultBuilder.build());
        } else {
          Put p = new Put(Bytes.toBytes(labelOrdinal));
          p.addImmutable(
              LABELS_TABLE_FAMILY, user, DUMMY_VALUE, LABELS_TABLE_TAGS);
          puts.add(p);
          response.addResult(successResult);
        }
      }
      OperationStatus[] opStatus = this.regionEnv.getRegion().batchMutate(
          puts.toArray(new Mutation[puts.size()]));
      int i = 0;
      for (OperationStatus status : opStatus) {
        if (status.getOperationStatusCode() != SUCCESS) {
          while (response.getResult(i) != successResult) i++;
          RegionActionResult.Builder failureResultBuilder = RegionActionResult.newBuilder();
          failureResultBuilder.setException(ResponseConverter
              .buildException(new DoNotRetryIOException(status.getExceptionMsg())));
          response.setResult(i, failureResultBuilder.build());
        }
        i++;
      }
    } catch (IOException e) {
      LOG.error(e);
      setExceptionResults(auths.size(), e, response);
    }
    done.run(response.build());
  }

  @Override
  public synchronized void getAuths(RpcController controller, GetAuthsRequest request,
      RpcCallback<GetAuthsResponse> done) {
    byte[] user = request.getUser().toByteArray();
    GetAuthsResponse.Builder response = GetAuthsResponse.newBuilder();
    response.setUser(request.getUser());
    try {
      List<String> labels = getUserAuthsFromLabelsTable(user);
      for (String label : labels) {
        response.addAuth(HBaseZeroCopyByteString.wrap(Bytes.toBytes(label)));
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response.build());
  }

  private List<String> getUserAuthsFromLabelsTable(byte[] user) throws IOException {
    Scan s = new Scan();
    s.addColumn(LABELS_TABLE_FAMILY, user);
    Filter filter = createVisibilityLabelFilter(this.regionEnv.getRegion(), new Authorizations(
        SYSTEM_LABEL));
    s.setFilter(filter);
    List<String> auths = new ArrayList<String>();
    // We do ACL check here as we create scanner directly on region. It will not make calls to
    // AccessController CP methods.
    performACLCheck();
    RegionScanner scanner = this.regionEnv.getRegion().getScanner(s);
    List<Cell> results = new ArrayList<Cell>(1);
    while (true) {
      scanner.next(results);
      if (results.isEmpty()) break;
      Cell cell = results.get(0);
      int ordinal = Bytes.toInt(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
      String label = this.visibilityManager.getLabel(ordinal);
      if (label != null) {
        auths.add(label);
      }
      results.clear();
    }
    return auths;
  }

  @Override
  public synchronized void clearAuths(RpcController controller, SetAuthsRequest request,
      RpcCallback<VisibilityLabelsResponse> done) {
    VisibilityLabelsResponse.Builder response = VisibilityLabelsResponse.newBuilder();
    List<ByteString> auths = request.getAuthList();
    if (!initialized) {
      setExceptionResults(auths.size(), new CoprocessorException(
          "VisibilityController not yet initialized"), response);
    }
    byte[] user = request.getUser().toByteArray();
    try {
      checkCallingUserAuth();
      List<String> currentAuths = this.getUserAuthsFromLabelsTable(user);
      List<Mutation> deletes = new ArrayList<Mutation>(auths.size());
      RegionActionResult successResult = RegionActionResult.newBuilder().build();
      for (ByteString authBS : auths) {
        byte[] auth = authBS.toByteArray();
        String authStr = Bytes.toString(auth);
        if (currentAuths.contains(authStr)) {
          int labelOrdinal = this.visibilityManager.getLabelOrdinal(authStr);
          assert labelOrdinal > 0;
          Delete d = new Delete(Bytes.toBytes(labelOrdinal));
          d.deleteColumns(LABELS_TABLE_FAMILY, user);
          deletes.add(d);
          response.addResult(successResult);
        } else {
          // This label is not set for the user.
          RegionActionResult.Builder failureResultBuilder = RegionActionResult.newBuilder();
          failureResultBuilder.setException(ResponseConverter
              .buildException(new InvalidLabelException("Label '" + authStr
                  + "' is not set for the user " + Bytes.toString(user))));
          response.addResult(failureResultBuilder.build());
        }
      }
      OperationStatus[] opStatus = this.regionEnv.getRegion().batchMutate(
          deletes.toArray(new Mutation[deletes.size()]));
      int i = 0;
      for (OperationStatus status : opStatus) {
        if (status.getOperationStatusCode() != SUCCESS) {
          while (response.getResult(i) != successResult) i++;
          RegionActionResult.Builder failureResultBuilder = RegionActionResult.newBuilder();
          failureResultBuilder.setException(ResponseConverter
              .buildException(new DoNotRetryIOException(status.getExceptionMsg())));
          response.setResult(i, failureResultBuilder.build());
        }
        i++;
      }
    } catch (IOException e) {
      LOG.error(e);
      setExceptionResults(auths.size(), e, response);
    }
    done.run(response.build());
  }

  private void checkCallingUserAuth() throws IOException {
    if (!this.acOn) {
      User user = getActiveUser();
      if (user == null) {
        throw new IOException("Unable to retrieve calling user");
      }
      List<String> auths = this.visibilityManager.getAuths(user.getShortName());
      if (LOG.isTraceEnabled()) {
        LOG.trace("The list of auths are "+auths);
      }
      if (!auths.contains(SYSTEM_LABEL)) {
        throw new AccessDeniedException("User '" + user.getShortName()
            + "' is not authorized to perform this action.");
      }
    }
  }
}
