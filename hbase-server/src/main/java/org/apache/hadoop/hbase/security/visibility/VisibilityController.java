/*
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

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionActionResult;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.GetAuthsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.GetAuthsResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.ListLabelsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.ListLabelsResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.SetAuthsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabel;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsService;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.MapMaker;

/**
 * Coprocessor that has both the MasterObserver and RegionObserver implemented that supports in
 * visibility labels
 */
@CoreCoprocessor
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
// TODO: break out Observer functions into separate class/sub-class.
public class VisibilityController implements MasterCoprocessor, RegionCoprocessor,
    VisibilityLabelsService.Interface, MasterObserver, RegionObserver {


  private static final Logger LOG = LoggerFactory.getLogger(VisibilityController.class);
  private static final Logger AUDITLOG = LoggerFactory.getLogger("SecurityLogger."
      + VisibilityController.class.getName());
  // flags if we are running on a region of the 'labels' table
  private boolean labelsRegion = false;
  // Flag denoting whether AcessController is available or not.
  private boolean accessControllerAvailable = false;
  private Configuration conf;
  private volatile boolean initialized = false;
  private boolean checkAuths = false;
  /** Mapping of scanner instances to the user who created them */
  private Map<InternalScanner,String> scannerOwners =
      new MapMaker().weakKeys().makeMap();

  private VisibilityLabelService visibilityLabelService;

  /** if we are active, usually false, only true if "hbase.security.authorization"
    has been set to true in site configuration */
  boolean authorizationEnabled;

  // Add to this list if there are any reserved tag types
  private static ArrayList<Byte> RESERVED_VIS_TAG_TYPES = new ArrayList<>();
  static {
    RESERVED_VIS_TAG_TYPES.add(TagType.VISIBILITY_TAG_TYPE);
    RESERVED_VIS_TAG_TYPES.add(TagType.VISIBILITY_EXP_SERIALIZATION_FORMAT_TAG_TYPE);
    RESERVED_VIS_TAG_TYPES.add(TagType.STRING_VIS_TAG_TYPE);
  }

  public static boolean isCellAuthorizationSupported(Configuration conf) {
    return AccessChecker.isAuthorizationSupported(conf);
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    this.conf = env.getConfiguration();

    authorizationEnabled = AccessChecker.isAuthorizationSupported(conf);
    if (!authorizationEnabled) {
      LOG.warn("The VisibilityController has been loaded with authorization checks disabled.");
    }

    if (HFile.getFormatVersion(conf) < HFile.MIN_FORMAT_VERSION_WITH_TAGS) {
      throw new RuntimeException("A minimum HFile version of " + HFile.MIN_FORMAT_VERSION_WITH_TAGS
        + " is required to persist visibility labels. Consider setting " + HFile.FORMAT_VERSION_KEY
        + " accordingly.");
    }

    // Do not create for master CPs
    if (!(env instanceof MasterCoprocessorEnvironment)) {
      visibilityLabelService = VisibilityLabelServiceManager.getInstance()
          .getVisibilityLabelService(this.conf);
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {

  }

  /**************************** Observer/Service Getters ************************************/
  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(
        VisibilityLabelsProtos.VisibilityLabelsService.newReflectiveService(this));
  }

  /********************************* Master related hooks **********************************/

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
    throws IOException {
    // Need to create the new system table for labels here
    try (Admin admin = ctx.getEnvironment().getConnection().getAdmin()) {
      if (!admin.tableExists(LABELS_TABLE_NAME)) {
        // We will cache all the labels. No need of normal table block cache.
        // Let the "labels" table having only one region always. We are not expecting too many
        // labels in the system.
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(LABELS_TABLE_NAME)
          .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(LABELS_TABLE_FAMILY)
            .setBloomFilterType(BloomType.NONE).setBlockCacheEnabled(false).build())
          .setValue(TableDescriptorBuilder.SPLIT_POLICY, DisabledRegionSplitPolicy.class.getName())
          .build();

        admin.createTable(tableDescriptor);
      }
    }
  }

  @Override
  public TableDescriptor preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, TableDescriptor currentDescriptor, TableDescriptor newDescriptor)
      throws IOException {
    if (authorizationEnabled) {
      if (LABELS_TABLE_NAME.equals(tableName)) {
        throw new ConstraintException("Cannot alter " + LABELS_TABLE_NAME);
      }
    }
    return newDescriptor;
  }

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
    if (!authorizationEnabled) {
      return;
    }
    if (LABELS_TABLE_NAME.equals(tableName)) {
      throw new ConstraintException("Cannot disable " + LABELS_TABLE_NAME);
    }
  }

  /****************************** Region related hooks ******************************/

  @Override
  public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
    // Read the entire labels table and populate the zk
    if (e.getEnvironment().getRegion().getRegionInfo().getTable().equals(LABELS_TABLE_NAME)) {
      this.labelsRegion = true;
      synchronized (this) {
        this.accessControllerAvailable = CoprocessorHost.getLoadedCoprocessors()
          .contains(AccessController.class.getName());
      }
      initVisibilityLabelService(e.getEnvironment());
    } else {
      checkAuths = e.getEnvironment().getConfiguration()
          .getBoolean(VisibilityConstants.CHECK_AUTHS_FOR_MUTATION, false);
      initVisibilityLabelService(e.getEnvironment());
    }
  }

  private void initVisibilityLabelService(RegionCoprocessorEnvironment env) {
    try {
      this.visibilityLabelService.init(env);
      this.initialized = true;
    } catch (IOException ioe) {
      LOG.error("Error while initializing VisibilityLabelService..", ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void postSetSplitOrMergeEnabled(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean newValue, final MasterSwitchType switchType) throws IOException {
  }

  @Override
  public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    if (c.getEnvironment().getRegion().getRegionInfo().getTable().isSystemTable()) {
      return;
    }
    // TODO this can be made as a global LRU cache at HRS level?
    Map<String, List<Tag>> labelCache = new HashMap<>();
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
      boolean sanityFailure = false;
      boolean modifiedTagFound = false;
      Pair<Boolean, Tag> pair = new Pair<>(false, null);
      for (CellScanner cellScanner = m.cellScanner(); cellScanner.advance();) {
        pair = checkForReservedVisibilityTagPresence(cellScanner.current(), pair);
        if (!pair.getFirst()) {
          // Don't disallow reserved tags if authorization is disabled
          if (authorizationEnabled) {
            miniBatchOp.setOperationStatus(i, new OperationStatus(SANITY_CHECK_FAILURE,
              "Mutation contains cell with reserved type tag"));
            sanityFailure = true;
          }
          break;
        } else {
          // Indicates that the cell has a the tag which was modified in the src replication cluster
          Tag tag = pair.getSecond();
          if (cellVisibility == null && tag != null) {
            // May need to store only the first one
            cellVisibility = new CellVisibility(Tag.getValueAsString(tag));
            modifiedTagFound = true;
          }
        }
      }
      if (!sanityFailure && (m instanceof Put || m instanceof Delete)) {
        if (cellVisibility != null) {
          String labelsExp = cellVisibility.getExpression();
          List<Tag> visibilityTags = labelCache.get(labelsExp);
          if (visibilityTags == null) {
            // Don't check user auths for labels with Mutations when the user is super user
            boolean authCheck = authorizationEnabled && checkAuths && !(isSystemOrSuperUser());
            try {
              visibilityTags = this.visibilityLabelService.createVisibilityExpTags(labelsExp, true,
                  authCheck);
            } catch (InvalidLabelException e) {
              miniBatchOp.setOperationStatus(i,
                  new OperationStatus(SANITY_CHECK_FAILURE, e.getMessage()));
            }
            if (visibilityTags != null) {
              labelCache.put(labelsExp, visibilityTags);
            }
          }
          if (visibilityTags != null) {
            List<Cell> updatedCells = new ArrayList<>();
            for (CellScanner cellScanner = m.cellScanner(); cellScanner.advance();) {
              Cell cell = cellScanner.current();
              List<Tag> tags = PrivateCellUtil.getTags(cell);
              if (modifiedTagFound) {
                // Rewrite the tags by removing the modified tags.
                removeReplicationVisibilityTag(tags);
              }
              tags.addAll(visibilityTags);
              Cell updatedCell = PrivateCellUtil.createCell(cell, tags);
              updatedCells.add(updatedCell);
            }
            m.getFamilyCellMap().clear();
            // Clear and add new Cells to the Mutation.
            for (Cell cell : updatedCells) {
              if (m instanceof Put) {
                Put p = (Put) m;
                p.add(cell);
              } else {
                Delete d = (Delete) m;
                d.add(cell);
              }
            }
          }
        }
      }
    }
  }

  @Override
  public void prePrepareTimeStampForDeleteVersion(
      ObserverContext<RegionCoprocessorEnvironment> ctx, Mutation delete, Cell cell,
      byte[] byteNow, Get get) throws IOException {
    // Nothing to do if we are not filtering by visibility
    if (!authorizationEnabled) {
      return;
    }

    CellVisibility cellVisibility = null;
    try {
      cellVisibility = delete.getCellVisibility();
    } catch (DeserializationException de) {
      throw new IOException("Invalid cell visibility specified " + delete, de);
    }
    // The check for checkForReservedVisibilityTagPresence happens in preBatchMutate happens.
    // It happens for every mutation and that would be enough.
    List<Tag> visibilityTags = new ArrayList<>();
    if (cellVisibility != null) {
      String labelsExp = cellVisibility.getExpression();
      try {
        visibilityTags = this.visibilityLabelService.createVisibilityExpTags(labelsExp, false,
            false);
      } catch (InvalidLabelException e) {
        throw new IOException("Invalid cell visibility specified " + labelsExp, e);
      }
    }
    get.setFilter(new DeleteVersionVisibilityExpressionFilter(visibilityTags,
        VisibilityConstants.SORTED_ORDINAL_SERIALIZATION_FORMAT));
    try (RegionScanner scanner = ctx.getEnvironment().getRegion().getScanner(new Scan(get))) {
      // NOTE: Please don't use HRegion.get() instead,
      // because it will copy cells to heap. See HBASE-26036
      List<Cell> result = new ArrayList<>();
      scanner.next(result);

      if (result.size() < get.getMaxVersions()) {
        // Nothing to delete
        PrivateCellUtil.updateLatestStamp(cell, byteNow);
        return;
      }
      if (result.size() > get.getMaxVersions()) {
        throw new RuntimeException("Unexpected size: " + result.size() +
          ". Results more than the max versions obtained.");
      }
      Cell getCell = result.get(get.getMaxVersions() - 1);
      PrivateCellUtil.setTimestamp(cell, getCell.getTimestamp());
    }
    // We are bypassing here because in the HRegion.updateDeleteLatestVersionTimeStamp we would
    // update with the current timestamp after again doing a get. As the hook as already determined
    // the needed timestamp we need to bypass here.
    // TODO : See if HRegion.updateDeleteLatestVersionTimeStamp() could be
    // called only if the hook is not called.
    ctx.bypass();
  }

  /**
   * Checks whether cell contains any tag with type as VISIBILITY_TAG_TYPE. This
   * tag type is reserved and should not be explicitly set by user.
   *
   * @param cell The cell under consideration
   * @param pair An optional pair of type {@code <Boolean, Tag>} which would be reused if already
   *     set and new one will be created if NULL is passed
   * @return If the boolean is false then it indicates that the cell has a RESERVERD_VIS_TAG and
   *     with boolean as true, not null tag indicates that a string modified tag was found.
   */
  private Pair<Boolean, Tag> checkForReservedVisibilityTagPresence(Cell cell,
      Pair<Boolean, Tag> pair) throws IOException {
    if (pair == null) {
      pair = new Pair<>(false, null);
    } else {
      pair.setFirst(false);
      pair.setSecond(null);
    }
    // Bypass this check when the operation is done by a system/super user.
    // This is done because, while Replication, the Cells coming to the peer cluster with reserved
    // typed tags and this is fine and should get added to the peer cluster table
    if (isSystemOrSuperUser()) {
      // Does the cell contain special tag which indicates that the replicated
      // cell visiblilty tags
      // have been modified
      Tag modifiedTag = null;
      Iterator<Tag> tagsIterator = PrivateCellUtil.tagsIterator(cell);
      while (tagsIterator.hasNext()) {
        Tag tag = tagsIterator.next();
        if (tag.getType() == TagType.STRING_VIS_TAG_TYPE) {
          modifiedTag = tag;
          break;
        }
      }
      pair.setFirst(true);
      pair.setSecond(modifiedTag);
      return pair;
    }
    Iterator<Tag> tagsItr = PrivateCellUtil.tagsIterator(cell);
    while (tagsItr.hasNext()) {
      if (RESERVED_VIS_TAG_TYPES.contains(tagsItr.next().getType())) {
        return pair;
      }
    }
    pair.setFirst(true);
    return pair;
  }

  private void removeReplicationVisibilityTag(List<Tag> tags) throws IOException {
    Iterator<Tag> iterator = tags.iterator();
    while (iterator.hasNext()) {
      Tag tag = iterator.next();
      if (tag.getType() == TagType.STRING_VIS_TAG_TYPE) {
        iterator.remove();
        break;
      }
    }
  }

  @Override
  public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan)
      throws IOException {
    if (!initialized) {
      throw new VisibilityControllerNotReadyException("VisibilityController not yet initialized!");
    }
    // Nothing to do if authorization is not enabled
    if (!authorizationEnabled) {
      return;
    }
    Region region = e.getEnvironment().getRegion();
    Authorizations authorizations = null;
    try {
      authorizations = scan.getAuthorizations();
    } catch (DeserializationException de) {
      throw new IOException(de);
    }
    if (authorizations == null) {
      // No Authorizations present for this scan/Get!
      // In case of system tables other than "labels" just scan with out visibility check and
      // filtering. Checking visibility labels for META and NAMESPACE table is not needed.
      TableName table = region.getRegionInfo().getTable();
      if (table.isSystemTable() && !table.equals(LABELS_TABLE_NAME)) {
        return;
      }
    }

    Filter visibilityLabelFilter = VisibilityUtils.createVisibilityLabelFilter(region,
        authorizations);
    if (visibilityLabelFilter != null) {
      Filter filter = scan.getFilter();
      if (filter != null) {
        scan.setFilter(new FilterList(filter, visibilityLabelFilter));
      } else {
        scan.setFilter(visibilityLabelFilter);
      }
    }
  }

  @Override
  public DeleteTracker postInstantiateDeleteTracker(
      ObserverContext<RegionCoprocessorEnvironment> ctx, DeleteTracker delTracker)
      throws IOException {
    // Nothing to do if we are not filtering by visibility
    if (!authorizationEnabled) {
      return delTracker;
    }
    Region region = ctx.getEnvironment().getRegion();
    TableName table = region.getRegionInfo().getTable();
    if (table.isSystemTable()) {
      return delTracker;
    }
    // We are creating a new type of delete tracker here which is able to track
    // the timestamps and also the
    // visibility tags per cell. The covering cells are determined not only
    // based on the delete type and ts
    // but also on the visibility expression matching.
    return new VisibilityScanDeleteTracker(delTracker.getCellComparator());
  }

  @Override
  public RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Scan scan, final RegionScanner s) throws IOException {
    User user = VisibilityUtils.getActiveUser();
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

  @Override
  @Deprecated // Removed in later versions by HBASE-25277
  public boolean postScannerFilterRow(final ObserverContext<RegionCoprocessorEnvironment> e,
      final InternalScanner s, final Cell curRowCell, final boolean hasMore) throws IOException {
    // 'default' in RegionObserver might do unnecessary copy for Off heap backed Cells.
    return hasMore;
  }

  /**
   * Verify, when servicing an RPC, that the caller is the scanner owner. If so, we assume that
   * access control is correctly enforced based on the checks performed in preScannerOpen()
   */
  private void requireScannerOwner(InternalScanner s) throws AccessDeniedException {
    if (!RpcServer.isInRpcCallContext())
      return;
    String requestUName = RpcServer.getRequestUserName().orElse(null);
    String owner = scannerOwners.get(s);
    if (authorizationEnabled && owner != null && !owner.equals(requestUName)) {
      throw new AccessDeniedException("User '" + requestUName + "' is not the scanner owner!");
    }
  }

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
      List<Cell> results) throws IOException {
    if (!initialized) {
      throw new VisibilityControllerNotReadyException("VisibilityController not yet initialized");
    }
    // Nothing useful to do if authorization is not enabled
    if (!authorizationEnabled) {
      return;
    }
    Region region = e.getEnvironment().getRegion();
    Authorizations authorizations = null;
    try {
      authorizations = get.getAuthorizations();
    } catch (DeserializationException de) {
      throw new IOException(de);
    }
    if (authorizations == null) {
      // No Authorizations present for this scan/Get!
      // In case of system tables other than "labels" just scan with out visibility check and
      // filtering. Checking visibility labels for META and NAMESPACE table is not needed.
      TableName table = region.getRegionInfo().getTable();
      if (table.isSystemTable() && !table.equals(LABELS_TABLE_NAME)) {
        return;
      }
    }
    Filter visibilityLabelFilter = VisibilityUtils.createVisibilityLabelFilter(e.getEnvironment()
        .getRegion(), authorizations);
    if (visibilityLabelFilter != null) {
      Filter filter = get.getFilter();
      if (filter != null) {
        get.setFilter(new FilterList(filter, visibilityLabelFilter));
      } else {
        get.setFilter(visibilityLabelFilter);
      }
    }
  }

  private boolean isSystemOrSuperUser() throws IOException {
    return Superusers.isSuperUser(VisibilityUtils.getActiveUser());
  }

  @Override
  public List<Pair<Cell, Cell>> postIncrementBeforeWAL(
      ObserverContext<RegionCoprocessorEnvironment> ctx, Mutation mutation,
      List<Pair<Cell, Cell>> cellPairs) throws IOException {
    List<Pair<Cell, Cell>> resultPairs = new ArrayList<>(cellPairs.size());
    for (Pair<Cell, Cell> pair : cellPairs) {
      resultPairs
          .add(new Pair<>(pair.getFirst(), createNewCellWithTags(mutation, pair.getSecond())));
    }
    return resultPairs;
  }

  @Override
  public List<Pair<Cell, Cell>> postAppendBeforeWAL(
      ObserverContext<RegionCoprocessorEnvironment> ctx, Mutation mutation,
      List<Pair<Cell, Cell>> cellPairs) throws IOException {
    List<Pair<Cell, Cell>> resultPairs = new ArrayList<>(cellPairs.size());
    for (Pair<Cell, Cell> pair : cellPairs) {
      resultPairs
          .add(new Pair<>(pair.getFirst(), createNewCellWithTags(mutation, pair.getSecond())));
    }
    return resultPairs;
  }

  private Cell createNewCellWithTags(Mutation mutation, Cell newCell) throws IOException {
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
    // Prepend new visibility tags to a new list of tags for the cell
    // Don't check user auths for labels with Mutations when the user is super user
    boolean authCheck = authorizationEnabled && checkAuths && !(isSystemOrSuperUser());
    tags.addAll(this.visibilityLabelService.createVisibilityExpTags(cellVisibility.getExpression(),
        true, authCheck));
    // Carry forward all other tags
    Iterator<Tag> tagsItr = PrivateCellUtil.tagsIterator(newCell);
    while (tagsItr.hasNext()) {
      Tag tag = tagsItr.next();
      if (tag.getType() != TagType.VISIBILITY_TAG_TYPE
          && tag.getType() != TagType.VISIBILITY_EXP_SERIALIZATION_FORMAT_TAG_TYPE) {
        tags.add(tag);
      }
    }

    return PrivateCellUtil.createCell(newCell, tags);
  }

  /****************************** VisibilityEndpoint service related methods ******************************/
  @Override
  public synchronized void addLabels(RpcController controller, VisibilityLabelsRequest request,
      RpcCallback<VisibilityLabelsResponse> done) {
    VisibilityLabelsResponse.Builder response = VisibilityLabelsResponse.newBuilder();
    List<VisibilityLabel> visLabels = request.getVisLabelList();
    if (!initialized) {
      setExceptionResults(visLabels.size(),
        new VisibilityControllerNotReadyException("VisibilityController not yet initialized!"),
        response);
    } else {
      List<byte[]> labels = new ArrayList<>(visLabels.size());
      try {
        if (authorizationEnabled) {
          checkCallingUserAuth();
        }
        RegionActionResult successResult = RegionActionResult.newBuilder().build();
        for (VisibilityLabel visLabel : visLabels) {
          byte[] label = visLabel.getLabel().toByteArray();
          labels.add(label);
          response.addResult(successResult); // Just mark as success. Later it will get reset
                                             // based on the result from
                                             // visibilityLabelService.addLabels ()
        }
        if (!labels.isEmpty()) {
          OperationStatus[] opStatus = this.visibilityLabelService.addLabels(labels);
          logResult(true, "addLabels", "Adding labels allowed", null, labels, null);
          int i = 0;
          for (OperationStatus status : opStatus) {
            while (!Objects.equals(response.getResult(i), successResult)) {
              i++;
            }
            if (status.getOperationStatusCode() != SUCCESS) {
              RegionActionResult.Builder failureResultBuilder = RegionActionResult.newBuilder();
              failureResultBuilder.setException(buildException(new DoNotRetryIOException(
                  status.getExceptionMsg())));
              response.setResult(i, failureResultBuilder.build());
            }
            i++;
          }
        }
      } catch (AccessDeniedException e) {
        logResult(false, "addLabels", e.getMessage(), null, labels, null);
        LOG.error("User is not having required permissions to add labels", e);
        setExceptionResults(visLabels.size(), e, response);
      } catch (IOException e) {
        LOG.error(e.toString(), e);
        setExceptionResults(visLabels.size(), e, response);
      }
    }
    done.run(response.build());
  }

  private void setExceptionResults(int size, IOException e,
      VisibilityLabelsResponse.Builder response) {
    RegionActionResult.Builder failureResultBuilder = RegionActionResult.newBuilder();
    failureResultBuilder.setException(buildException(e));
    RegionActionResult failureResult = failureResultBuilder.build();
    for (int i = 0; i < size; i++) {
      response.addResult(i, failureResult);
    }
  }

  @Override
  public synchronized void setAuths(RpcController controller, SetAuthsRequest request,
      RpcCallback<VisibilityLabelsResponse> done) {
    VisibilityLabelsResponse.Builder response = VisibilityLabelsResponse.newBuilder();
    List<ByteString> auths = request.getAuthList();
    if (!initialized) {
      setExceptionResults(auths.size(),
        new VisibilityControllerNotReadyException("VisibilityController not yet initialized!"),
        response);
    } else {
      byte[] user = request.getUser().toByteArray();
      List<byte[]> labelAuths = new ArrayList<>(auths.size());
      try {
        if (authorizationEnabled) {
          checkCallingUserAuth();
        }
        for (ByteString authBS : auths) {
          labelAuths.add(authBS.toByteArray());
        }
        OperationStatus[] opStatus = this.visibilityLabelService.setAuths(user, labelAuths);
        logResult(true, "setAuths", "Setting authorization for labels allowed", user, labelAuths,
          null);
        RegionActionResult successResult = RegionActionResult.newBuilder().build();
        for (OperationStatus status : opStatus) {
          if (status.getOperationStatusCode() == SUCCESS) {
            response.addResult(successResult);
          } else {
            RegionActionResult.Builder failureResultBuilder = RegionActionResult.newBuilder();
            failureResultBuilder.setException(buildException(new DoNotRetryIOException(
                status.getExceptionMsg())));
            response.addResult(failureResultBuilder.build());
          }
        }
      } catch (AccessDeniedException e) {
        logResult(false, "setAuths", e.getMessage(), user, labelAuths, null);
        LOG.error("User is not having required permissions to set authorization", e);
        setExceptionResults(auths.size(), e, response);
      } catch (IOException e) {
        LOG.error(e.toString(), e);
        setExceptionResults(auths.size(), e, response);
      }
    }
    done.run(response.build());
  }

  private void logResult(boolean isAllowed, String request, String reason, byte[] user,
      List<byte[]> labelAuths, String regex) {
    if (AUDITLOG.isTraceEnabled()) {
      // This is more duplicated code!
      List<String> labelAuthsStr = new ArrayList<>();
      if (labelAuths != null) {
        int labelAuthsSize = labelAuths.size();
        labelAuthsStr = new ArrayList<>(labelAuthsSize);
        for (int i = 0; i < labelAuthsSize; i++) {
          labelAuthsStr.add(Bytes.toString(labelAuths.get(i)));
        }
      }

      User requestingUser = null;
      try {
        requestingUser = VisibilityUtils.getActiveUser();
      } catch (IOException e) {
        LOG.warn("Failed to get active system user.");
        LOG.debug("Details on failure to get active system user.", e);
      }
      AUDITLOG.trace("Access " + (isAllowed ? "allowed" : "denied") + " for user " +
          (requestingUser != null ? requestingUser.getShortName() : "UNKNOWN") + "; reason: " +
          reason + "; remote address: " +
          RpcServer.getRemoteAddress().map(InetAddress::toString).orElse("") + "; request: " +
          request + "; user: " + (user != null ? Bytes.toShort(user) : "null") + "; labels: " +
          labelAuthsStr + "; regex: " + regex);
    }
  }

  @Override
  public synchronized void getAuths(RpcController controller, GetAuthsRequest request,
      RpcCallback<GetAuthsResponse> done) {
    GetAuthsResponse.Builder response = GetAuthsResponse.newBuilder();
    if (!initialized) {
      controller.setFailed("VisibilityController not yet initialized");
    } else {
      byte[] user = request.getUser().toByteArray();
      List<String> labels = null;
      try {
        // We do ACL check here as we create scanner directly on region. It will not make calls to
        // AccessController CP methods.
        if (authorizationEnabled && accessControllerAvailable && !isSystemOrSuperUser()) {
          User requestingUser = VisibilityUtils.getActiveUser();
          throw new AccessDeniedException("User '"
              + (requestingUser != null ? requestingUser.getShortName() : "null")
              + "' is not authorized to perform this action.");
        }
        if (AuthUtil.isGroupPrincipal(Bytes.toString(user))) {
          String group = AuthUtil.getGroupName(Bytes.toString(user));
          labels = this.visibilityLabelService.getGroupAuths(new String[]{group}, false);
        }
        else {
          labels = this.visibilityLabelService.getUserAuths(user, false);
        }
        logResult(true, "getAuths", "Get authorizations for user allowed", user, null, null);
      } catch (AccessDeniedException e) {
        logResult(false, "getAuths", e.getMessage(), user, null, null);
        CoprocessorRpcUtils.setControllerException(controller, e);
      } catch (IOException e) {
        CoprocessorRpcUtils.setControllerException(controller, e);
      }
      response.setUser(request.getUser());
      if (labels != null) {
        for (String label : labels) {
          response.addAuth(ByteString.copyFrom(Bytes.toBytes(label)));
        }
      }
    }
    done.run(response.build());
  }

  @Override
  public synchronized void clearAuths(RpcController controller, SetAuthsRequest request,
      RpcCallback<VisibilityLabelsResponse> done) {
    VisibilityLabelsResponse.Builder response = VisibilityLabelsResponse.newBuilder();
    List<ByteString> auths = request.getAuthList();
    if (!initialized) {
      setExceptionResults(auths.size(), new CoprocessorException(
          "VisibilityController not yet initialized"), response);
    } else {
      byte[] requestUser = request.getUser().toByteArray();
      List<byte[]> labelAuths = new ArrayList<>(auths.size());
      try {
        // When AC is ON, do AC based user auth check
        if (authorizationEnabled && accessControllerAvailable && !isSystemOrSuperUser()) {
          User user = VisibilityUtils.getActiveUser();
          throw new AccessDeniedException("User '" + (user != null ? user.getShortName() : "null")
              + " is not authorized to perform this action.");
        }
        if (authorizationEnabled) {
          checkCallingUserAuth(); // When AC is not in place the calling user should have
                                  // SYSTEM_LABEL auth to do this action.
        }
        for (ByteString authBS : auths) {
          labelAuths.add(authBS.toByteArray());
        }

        OperationStatus[] opStatus =
            this.visibilityLabelService.clearAuths(requestUser, labelAuths);
        logResult(true, "clearAuths", "Removing authorization for labels allowed", requestUser,
          labelAuths, null);
        RegionActionResult successResult = RegionActionResult.newBuilder().build();
        for (OperationStatus status : opStatus) {
          if (status.getOperationStatusCode() == SUCCESS) {
            response.addResult(successResult);
          } else {
            RegionActionResult.Builder failureResultBuilder = RegionActionResult.newBuilder();
            failureResultBuilder.setException(buildException(new DoNotRetryIOException(
                status.getExceptionMsg())));
            response.addResult(failureResultBuilder.build());
          }
        }
      } catch (AccessDeniedException e) {
        logResult(false, "clearAuths", e.getMessage(), requestUser, labelAuths, null);
        LOG.error("User is not having required permissions to clear authorization", e);
        setExceptionResults(auths.size(), e, response);
      } catch (IOException e) {
        LOG.error(e.toString(), e);
        setExceptionResults(auths.size(), e, response);
      }
    }
    done.run(response.build());
  }

  @Override
  public synchronized void listLabels(RpcController controller, ListLabelsRequest request,
      RpcCallback<ListLabelsResponse> done) {
    ListLabelsResponse.Builder response = ListLabelsResponse.newBuilder();
    if (!initialized) {
      controller.setFailed("VisibilityController not yet initialized");
    } else {
      List<String> labels = null;
      String regex = request.hasRegex() ? request.getRegex() : null;
      try {
        // We do ACL check here as we create scanner directly on region. It will not make calls to
        // AccessController CP methods.
        if (authorizationEnabled && accessControllerAvailable && !isSystemOrSuperUser()) {
          User requestingUser = VisibilityUtils.getActiveUser();
          throw new AccessDeniedException("User '"
              + (requestingUser != null ? requestingUser.getShortName() : "null")
              + "' is not authorized to perform this action.");
        }
        labels = this.visibilityLabelService.listLabels(regex);
        logResult(true, "listLabels", "Listing labels allowed", null, null, regex);
      } catch (AccessDeniedException e) {
        logResult(false, "listLabels", e.getMessage(), null, null, regex);
        CoprocessorRpcUtils.setControllerException(controller, e);
      } catch (IOException e) {
        CoprocessorRpcUtils.setControllerException(controller, e);
      }
      if (labels != null && !labels.isEmpty()) {
        for (String label : labels) {
          response.addLabel(ByteString.copyFrom(Bytes.toBytes(label)));
        }
      }
    }
    done.run(response.build());
  }

  private void checkCallingUserAuth() throws IOException {
    if (!authorizationEnabled) { // Redundant, but just in case
      return;
    }
    if (!accessControllerAvailable) {
      User user = VisibilityUtils.getActiveUser();
      if (user == null) {
        throw new IOException("Unable to retrieve calling user");
      }
      if (!(this.visibilityLabelService.havingSystemAuth(user))) {
        throw new AccessDeniedException("User '" + user.getShortName()
            + "' is not authorized to perform this action.");
      }
    }
  }

  private static class DeleteVersionVisibilityExpressionFilter extends FilterBase {
    private List<Tag> deleteCellVisTags;
    private Byte deleteCellVisTagsFormat;

    public DeleteVersionVisibilityExpressionFilter(List<Tag> deleteCellVisTags,
        Byte deleteCellVisTagsFormat) {
      this.deleteCellVisTags = deleteCellVisTags;
      this.deleteCellVisTagsFormat = deleteCellVisTagsFormat;
    }

    @Override
    public boolean filterRowKey(Cell cell) throws IOException {
      // Impl in FilterBase might do unnecessary copy for Off heap backed Cells.
      return false;
    }

    @Override
    public ReturnCode filterCell(final Cell cell) throws IOException {
      List<Tag> putVisTags = new ArrayList<>();
      Byte putCellVisTagsFormat = VisibilityUtils.extractVisibilityTags(cell, putVisTags);
      if (putVisTags.isEmpty() && deleteCellVisTags.isEmpty()) {
        // Early out if there are no tags in the cell
        return ReturnCode.INCLUDE;
      }
      boolean matchFound = VisibilityLabelServiceManager
          .getInstance().getVisibilityLabelService()
          .matchVisibility(putVisTags, putCellVisTagsFormat, deleteCellVisTags,
              deleteCellVisTagsFormat);
      return matchFound ? ReturnCode.INCLUDE : ReturnCode.SKIP;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof DeleteVersionVisibilityExpressionFilter)) {
        return false;
      }
      if (this == obj){
        return true;
      }
      DeleteVersionVisibilityExpressionFilter f = (DeleteVersionVisibilityExpressionFilter)obj;
      return this.deleteCellVisTags.equals(f.deleteCellVisTags) &&
          this.deleteCellVisTagsFormat.equals(f.deleteCellVisTagsFormat);
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.deleteCellVisTags, this.deleteCellVisTagsFormat);
    }
  }

  /**
   * @param t
   * @return NameValuePair of the exception name to stringified version os exception.
   */
  // Copied from ResponseConverter and made private. Only used in here.
  private static NameBytesPair buildException(final Throwable t) {
    NameBytesPair.Builder parameterBuilder = NameBytesPair.newBuilder();
    parameterBuilder.setName(t.getClass().getName());
    parameterBuilder.setValue(
      ByteString.copyFromUtf8(StringUtils.stringifyException(t)));
    return parameterBuilder.build();
  }
}
