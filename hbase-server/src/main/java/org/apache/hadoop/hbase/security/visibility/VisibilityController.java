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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TagRewriteCell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.coprocessor.BaseMasterAndRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionActionResult;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.GetAuthsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.GetAuthsResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.SetAuthsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabel;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsService;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.DeleteTracker;
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
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;

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
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class VisibilityController extends BaseMasterAndRegionObserver implements
    VisibilityLabelsService.Interface, CoprocessorService {

  private static final Log LOG = LogFactory.getLog(VisibilityController.class);
  // flags if we are running on a region of the 'labels' table
  private boolean labelsRegion = false;
  // Flag denoting whether AcessController is available or not.
  private boolean acOn = false;
  private Configuration conf;
  private volatile boolean initialized = false;
  private boolean checkAuths = false;
  /** Mapping of scanner instances to the user who created them */
  private Map<InternalScanner,String> scannerOwners =
      new MapMaker().weakKeys().makeMap();

  List<String> superUsers;
  private VisibilityLabelService visibilityLabelService;

  // Add to this list if there are any reserved tag types
  private static ArrayList<Byte> RESERVED_VIS_TAG_TYPES = new ArrayList<Byte>();
  static {
    RESERVED_VIS_TAG_TYPES.add(TagType.VISIBILITY_TAG_TYPE);
    RESERVED_VIS_TAG_TYPES.add(TagType.VISIBILITY_EXP_SERIALIZATION_FORMAT_TAG_TYPE);
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    this.conf = env.getConfiguration();
    if (HFile.getFormatVersion(conf) < HFile.MIN_FORMAT_VERSION_WITH_TAGS) {
      throw new RuntimeException("A minimum HFile version of " + HFile.MIN_FORMAT_VERSION_WITH_TAGS
        + " is required to persist visibility labels. Consider setting " + HFile.FORMAT_VERSION_KEY
        + " accordingly.");
    }
    if (env instanceof RegionServerCoprocessorEnvironment) {
      throw new RuntimeException(
          "Visibility controller should not be configured as " +
          "'hbase.coprocessor.regionserver.classes'.");
    }

    if (env instanceof RegionCoprocessorEnvironment) {
      // VisibilityLabelService to be instantiated only with Region Observer.
      visibilityLabelService = VisibilityLabelServiceManager.getInstance()
          .getVisibilityLabelService(this.conf);
    }
    this.superUsers = getSystemAndSuperUsers();
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {

  }

  /********************************* Master related hooks **********************************/

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
    // Need to create the new system table for labels here
    MasterServices master = ctx.getEnvironment().getMasterServices();
    if (!MetaTableAccessor.tableExists(master.getConnection(), LABELS_TABLE_NAME)) {
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
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HTableDescriptor htd) throws IOException {
    if (LABELS_TABLE_NAME.equals(tableName)) {
      throw new ConstraintException("Cannot alter " + LABELS_TABLE_NAME);
    }
  }

  @Override
  public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      HColumnDescriptor column) throws IOException {
    if (LABELS_TABLE_NAME.equals(tableName)) {
      throw new ConstraintException("Cannot alter " + LABELS_TABLE_NAME);
    }
  }

  @Override
  public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor descriptor) throws IOException {
    if (LABELS_TABLE_NAME.equals(tableName)) {
      throw new ConstraintException("Cannot alter " + LABELS_TABLE_NAME);
    }
  }

  @Override
  public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, byte[] c) throws IOException {
    if (LABELS_TABLE_NAME.equals(tableName)) {
      throw new ConstraintException("Cannot alter " + LABELS_TABLE_NAME);
    }
  }

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
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
      this.acOn = CoprocessorHost.getLoadedCoprocessors().contains(AccessController.class.getName());
      // Defer the init of VisibilityLabelService on labels region until it is in recovering state.
      if (!e.getEnvironment().getRegion().isRecovering()) {
        initVisibilityLabelService(e.getEnvironment());
      }
    } else {
      checkAuths = e.getEnvironment().getConfiguration()
          .getBoolean(VisibilityConstants.CHECK_AUTHS_FOR_MUTATION, false);
      initVisibilityLabelService(e.getEnvironment());
    }
  }

  @Override
  public void postLogReplay(ObserverContext<RegionCoprocessorEnvironment> e) {
    if (this.labelsRegion) {
      initVisibilityLabelService(e.getEnvironment());
      LOG.debug("post labels region log replay");
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
      boolean sanityFailure = false;
      for (CellScanner cellScanner = m.cellScanner(); cellScanner.advance();) {
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
            // Don't check user auths for labels with Mutations when the user is super user
            boolean authCheck = this.checkAuths && !(isSystemOrSuperUser());
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
            List<Cell> updatedCells = new ArrayList<Cell>();
            for (CellScanner cellScanner = m.cellScanner(); cellScanner.advance();) {
              Cell cell = cellScanner.current();
              List<Tag> tags = Tag.asList(cell.getTagsArray(), cell.getTagsOffset(),
                  cell.getTagsLength());
              tags.addAll(visibilityTags);
              Cell updatedCell = new TagRewriteCell(cell, Tag.fromList(tags));
              updatedCells.add(updatedCell);
            }
            m.getFamilyCellMap().clear();
            // Clear and add new Cells to the Mutation.
            for (Cell cell : updatedCells) {
              if (m instanceof Put) {
                Put p = (Put) m;
                p.add(cell);
              } else if (m instanceof Delete) {
                Delete d = (Delete) m;
                d.addDeleteMarker(cell);
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
    CellVisibility cellVisibility = null;
    try {
      cellVisibility = delete.getCellVisibility();
    } catch (DeserializationException de) {
      throw new IOException("Invalid cell visibility specified " + delete, de);
    }
    // The check for checkForReservedVisibilityTagPresence happens in preBatchMutate happens.
    // It happens for every mutation and that would be enough.
    List<Tag> visibilityTags = new ArrayList<Tag>();
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
    List<Cell> result = ctx.getEnvironment().getRegion().get(get, false);

    if (result.size() < get.getMaxVersions()) {
      // Nothing to delete
      CellUtil.updateLatestStamp(cell, Long.MIN_VALUE);
      return;
    }
    if (result.size() > get.getMaxVersions()) {
      throw new RuntimeException("Unexpected size: " + result.size()
          + ". Results more than the max versions obtained.");
    }
    Cell getCell = result.get(get.getMaxVersions() - 1);
    CellUtil.setTimestamp(cell, getCell.getTimestamp());

    // We are bypassing here because in the HRegion.updateDeleteLatestVersionTimeStamp we would
    // update with the current timestamp after again doing a get. As the hook as already determined
    // the needed timestamp we need to bypass here.
    // TODO : See if HRegion.updateDeleteLatestVersionTimeStamp() could be
    // called only if the hook is not called.
    ctx.bypass();
  }

  // Checks whether cell contains any tag with type as VISIBILITY_TAG_TYPE.
  // This tag type is reserved and should not be explicitly set by user.
  private boolean checkForReservedVisibilityTagPresence(Cell cell) throws IOException {
    // Bypass this check when the operation is done by a system/super user.
    // This is done because, while Replication, the Cells coming to the peer cluster with reserved
    // typed tags and this is fine and should get added to the peer cluster table
    if (isSystemOrSuperUser()) {
      return true;
    }
    if (cell.getTagsLength() > 0) {
      Iterator<Tag> tagsItr = CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(),
          cell.getTagsLength());
      while (tagsItr.hasNext()) {
        if (RESERVED_VIS_TAG_TYPES.contains(tagsItr.next().getType())) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan,
      RegionScanner s) throws IOException {
    if (!initialized) {
      throw new VisibilityControllerNotReadyException("VisibilityController not yet initialized!");
    }
    HRegion region = e.getEnvironment().getRegion();
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
        return s;
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
    return s;
  }

  @Override
  public DeleteTracker postInstantiateDeleteTracker(
      ObserverContext<RegionCoprocessorEnvironment> ctx, DeleteTracker delTracker)
      throws IOException {
    HRegion region = ctx.getEnvironment().getRegion();
    TableName table = region.getRegionInfo().getTable();
    if (table.isSystemTable()) {
      return delTracker;
    }
    // We are creating a new type of delete tracker here which is able to track
    // the timestamps and also the
    // visibility tags per cell. The covering cells are determined not only
    // based on the delete type and ts
    // but also on the visibility expression matching.
    return new VisibilityScanDeleteTracker();
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
    if (!initialized) {
      throw new VisibilityControllerNotReadyException("VisibilityController not yet initialized!");
    }
    HRegion region = e.getEnvironment().getRegion();
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
    User activeUser = VisibilityUtils.getActiveUser();
    return this.superUsers.contains(activeUser.getShortName());
  }

  @Override
  public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> e, Append append)
      throws IOException {
    for (CellScanner cellScanner = append.cellScanner(); cellScanner.advance();) {
      if (!checkForReservedVisibilityTagPresence(cellScanner.current())) {
        throw new FailedSanityCheckException("Append contains cell with reserved type tag");
      }
    }
    return null;
  }

  @Override
  public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> e, Increment increment)
      throws IOException {
    for (CellScanner cellScanner = increment.cellScanner(); cellScanner.advance();) {
      if (!checkForReservedVisibilityTagPresence(cellScanner.current())) {
        throw new FailedSanityCheckException("Increment contains cell with reserved type tag");
      }
    }
    return null;
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
    // Prepend new visibility tags to a new list of tags for the cell
    // Don't check user auths for labels with Mutations when the user is super user
    boolean authCheck = this.checkAuths && !(isSystemOrSuperUser());
    tags.addAll(this.visibilityLabelService.createVisibilityExpTags(cellVisibility.getExpression(),
        true, authCheck));
    // Save an object allocation where we can
    if (newCell.getTagsLength() > 0) {
      // Carry forward all other tags
      Iterator<Tag> tagsItr = CellUtil.tagsIterator(newCell.getTagsArray(),
          newCell.getTagsOffset(), newCell.getTagsLength());
      while (tagsItr.hasNext()) {
        Tag tag = tagsItr.next();
        if (tag.getType() != TagType.VISIBILITY_TAG_TYPE
            && tag.getType() != TagType.VISIBILITY_EXP_SERIALIZATION_FORMAT_TAG_TYPE) {
          tags.add(tag);
        }
      }
    }

    Cell rewriteCell = new TagRewriteCell(newCell, Tag.fromList(tags));
    return rewriteCell;
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
    List<VisibilityLabel> visLabels = request.getVisLabelList();
    if (!initialized) {
      setExceptionResults(visLabels.size(),
        new VisibilityControllerNotReadyException("VisibilityController not yet initialized!"),
        response);
    } else {
      try {
        checkCallingUserAuth();
        List<byte[]> labels = new ArrayList<byte[]>(visLabels.size());
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
          int i = 0;
          for (OperationStatus status : opStatus) {
            while (response.getResult(i) != successResult)
              i++;
            if (status.getOperationStatusCode() != SUCCESS) {
              RegionActionResult.Builder failureResultBuilder = RegionActionResult.newBuilder();
              failureResultBuilder.setException(ResponseConverter
                  .buildException(new DoNotRetryIOException(status.getExceptionMsg())));
              response.setResult(i, failureResultBuilder.build());
            }
            i++;
          }
        }
      } catch (IOException e) {
        LOG.error(e);
        setExceptionResults(visLabels.size(), e, response);
      }
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
      try {
        checkCallingUserAuth();
        List<byte[]> labelAuths = new ArrayList<byte[]>(auths.size());
        for (ByteString authBS : auths) {
          labelAuths.add(authBS.toByteArray());
        }
        OperationStatus[] opStatus = this.visibilityLabelService.setAuths(request.getUser()
            .toByteArray(), labelAuths);
        RegionActionResult successResult = RegionActionResult.newBuilder().build();
        for (OperationStatus status : opStatus) {
          if (status.getOperationStatusCode() == SUCCESS) {
            response.addResult(successResult);
          } else {
            RegionActionResult.Builder failureResultBuilder = RegionActionResult.newBuilder();
            failureResultBuilder.setException(ResponseConverter
                .buildException(new DoNotRetryIOException(status.getExceptionMsg())));
            response.addResult(failureResultBuilder.build());
          }
        }
      } catch (IOException e) {
        LOG.error(e);
        setExceptionResults(auths.size(), e, response);
      }
    }
    done.run(response.build());
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
        if (this.acOn && !isSystemOrSuperUser()) {
          User requestingUser = VisibilityUtils.getActiveUser();
          throw new AccessDeniedException("User '"
              + (requestingUser != null ? requestingUser.getShortName() : "null")
              + "' is not authorized to perform this action.");
        }
        labels = this.visibilityLabelService.getAuths(user, false);
      } catch (IOException e) {
        ResponseConverter.setControllerException(controller, e);
      }
      response.setUser(request.getUser());
      if (labels != null) {
        for (String label : labels) {
          response.addAuth(ByteStringer.wrap(Bytes.toBytes(label)));
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
      try {
        // When AC is ON, do AC based user auth check
        if (this.acOn && !isSystemOrSuperUser()) {
          User user = VisibilityUtils.getActiveUser();
          throw new AccessDeniedException("User '" + (user != null ? user.getShortName() : "null")
              + " is not authorized to perform this action.");
        }
        checkCallingUserAuth(); // When AC is not in place the calling user should have SYSTEM_LABEL
                                // auth to do this action.
        List<byte[]> labelAuths = new ArrayList<byte[]>(auths.size());
        for (ByteString authBS : auths) {
          labelAuths.add(authBS.toByteArray());
        }
        OperationStatus[] opStatus = this.visibilityLabelService.clearAuths(request.getUser()
            .toByteArray(), labelAuths);
        RegionActionResult successResult = RegionActionResult.newBuilder().build();
        for (OperationStatus status : opStatus) {
          if (status.getOperationStatusCode() == SUCCESS) {
            response.addResult(successResult);
          } else {
            RegionActionResult.Builder failureResultBuilder = RegionActionResult.newBuilder();
            failureResultBuilder.setException(ResponseConverter
                .buildException(new DoNotRetryIOException(status.getExceptionMsg())));
            response.addResult(failureResultBuilder.build());
          }
        }
      } catch (IOException e) {
        LOG.error(e);
        setExceptionResults(auths.size(), e, response);
      }
    }
    done.run(response.build());
  }

  private void checkCallingUserAuth() throws IOException {
    if (!this.acOn) {
      User user = VisibilityUtils.getActiveUser();
      if (user == null) {
        throw new IOException("Unable to retrieve calling user");
      }
      if (!(this.visibilityLabelService.havingSystemAuth(Bytes.toBytes(user.getShortName())))) {
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
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
      List<Tag> putVisTags = new ArrayList<Tag>();
      Byte putCellVisTagsFormat = VisibilityUtils.extractVisibilityTags(cell, putVisTags);
      boolean matchFound = VisibilityLabelServiceManager
          .getInstance().getVisibilityLabelService()
          .matchVisibility(putVisTags, putCellVisTagsFormat, deleteCellVisTags,
              deleteCellVisTagsFormat);
      return matchFound ? ReturnCode.INCLUDE : ReturnCode.SKIP;
    }

    // Override here explicitly as the method in super class FilterBase might do a KeyValue recreate.
    // See HBASE-12068
    @Override
    public Cell transformCell(Cell v) {
      return v;
    }
  }
}
