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
package org.apache.hadoop.hbase.security.visibility;

import static org.apache.hadoop.hbase.TagType.VISIBILITY_TAG_TYPE;
import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_FAMILY;
import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_NAME;
import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABEL_QUALIFIER;
import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.SORTED_ORDINAL_SERIALIZATION_FORMAT;
import static org.apache.hadoop.hbase.security.visibility.VisibilityUtils.SYSTEM_LABEL;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.TagUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.HasRegionServerServices;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class DefaultVisibilityLabelServiceImpl implements VisibilityLabelService {
  private static final Logger LOG =
    LoggerFactory.getLogger(DefaultVisibilityLabelServiceImpl.class);

  // "system" label is having an ordinal value 1.
  private static final int SYSTEM_LABEL_ORDINAL = 1;
  private static final Tag[] LABELS_TABLE_TAGS = new Tag[1];
  private static final byte[] DUMMY_VALUE = new byte[0];

  private AtomicInteger ordinalCounter = new AtomicInteger(-1);
  private Configuration conf;
  private Region labelsRegion;
  private VisibilityLabelsCache labelsCache;
  private List<ScanLabelGenerator> scanLabelGenerators;

  static {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    try {
      StreamUtils.writeRawVInt32(dos, SYSTEM_LABEL_ORDINAL);
    } catch (IOException e) {
      // We write to a byte array. No Exception can happen.
    }
    LABELS_TABLE_TAGS[0] = new ArrayBackedTag(VISIBILITY_TAG_TYPE, baos.toByteArray());
  }

  public DefaultVisibilityLabelServiceImpl() {

  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void init(RegionCoprocessorEnvironment e) throws IOException {
    /*
     * So, presumption that the RegionCE has a ZK Connection is too much. Why would a RCE have a ZK
     * instance? This is cheating presuming we have access to the RS ZKW. TODO: Fix. And what is
     * going on here? This ain't even a Coprocessor? And its being passed a CP Env?
     */
    // This is a CoreCoprocessor. On creation, we should have gotten an environment that
    // implements HasRegionServerServices so we can get at RSS. FIX!!!! Integrate this CP as
    // native service.
    ZKWatcher zk = ((HasRegionServerServices) e).getRegionServerServices().getZooKeeper();
    try {
      labelsCache = VisibilityLabelsCache.createAndGet(zk, this.conf);
    } catch (IOException ioe) {
      LOG.error("Error creating VisibilityLabelsCache", ioe);
      throw ioe;
    }
    this.scanLabelGenerators = VisibilityUtils.getScanLabelGenerators(this.conf);
    if (e.getRegion().getRegionInfo().getTable().equals(LABELS_TABLE_NAME)) {
      this.labelsRegion = e.getRegion();
      Pair<Map<String, Integer>, Map<String, List<Integer>>> labelsAndUserAuths =
        extractLabelsAndAuths(getExistingLabelsWithAuths());
      Map<String, Integer> labels = labelsAndUserAuths.getFirst();
      Map<String, List<Integer>> userAuths = labelsAndUserAuths.getSecond();
      // Add the "system" label if it is not added into the system yet
      addSystemLabel(this.labelsRegion, labels, userAuths);
      int ordinal = SYSTEM_LABEL_ORDINAL; // Ordinal 1 is reserved for "system" label.
      for (Integer i : labels.values()) {
        if (i > ordinal) {
          ordinal = i;
        }
      }
      this.ordinalCounter.set(ordinal + 1);
      if (labels.size() > 0) {
        // If there is no data need not write to zk
        byte[] serialized = VisibilityUtils.getDataToWriteToZooKeeper(labels);
        this.labelsCache.writeToZookeeper(serialized, true);
        this.labelsCache.refreshLabelsCache(serialized);
      }
      if (userAuths.size() > 0) {
        byte[] serialized = VisibilityUtils.getUserAuthsDataToWriteToZooKeeper(userAuths);
        this.labelsCache.writeToZookeeper(serialized, false);
        this.labelsCache.refreshUserAuthsCache(serialized);
      }
    }
  }

  protected List<List<Cell>> getExistingLabelsWithAuths() throws IOException {
    Scan scan = new Scan();
    RegionScanner scanner = labelsRegion.getScanner(scan);
    List<List<Cell>> existingLabels = new ArrayList<>();
    try {
      while (true) {
        List<Cell> cells = new ArrayList<>();
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

  protected Pair<Map<String, Integer>, Map<String, List<Integer>>>
    extractLabelsAndAuths(List<List<Cell>> labelDetails) {
    Map<String, Integer> labels = new HashMap<>();
    Map<String, List<Integer>> userAuths = new HashMap<>();
    for (List<Cell> cells : labelDetails) {
      for (Cell cell : cells) {
        if (CellUtil.matchingQualifier(cell, LABEL_QUALIFIER)) {
          labels.put(
            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()),
            PrivateCellUtil.getRowAsInt(cell));
        } else {
          // These are user cells who has authorization for this label
          String user = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
            cell.getQualifierLength());
          List<Integer> auths = userAuths.get(user);
          if (auths == null) {
            auths = new ArrayList<>();
            userAuths.put(user, auths);
          }
          auths.add(PrivateCellUtil.getRowAsInt(cell));
        }
      }
    }
    return new Pair<>(labels, userAuths);
  }

  protected void addSystemLabel(Region region, Map<String, Integer> labels,
    Map<String, List<Integer>> userAuths) throws IOException {
    if (!labels.containsKey(SYSTEM_LABEL)) {
      byte[] row = Bytes.toBytes(SYSTEM_LABEL_ORDINAL);
      Put p = new Put(row);
      p.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(row)
        .setFamily(LABELS_TABLE_FAMILY).setQualifier(LABEL_QUALIFIER).setTimestamp(p.getTimestamp())
        .setType(Type.Put).setValue(Bytes.toBytes(SYSTEM_LABEL)).build());
      region.put(p);
      labels.put(SYSTEM_LABEL, SYSTEM_LABEL_ORDINAL);
    }
  }

  @Override
  public OperationStatus[] addLabels(List<byte[]> labels) throws IOException {
    assert labelsRegion != null;
    OperationStatus[] finalOpStatus = new OperationStatus[labels.size()];
    List<Mutation> puts = new ArrayList<>(labels.size());
    int i = 0;
    ExtendedCellBuilder builder = ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
    for (byte[] label : labels) {
      String labelStr = Bytes.toString(label);
      if (this.labelsCache.getLabelOrdinal(labelStr) > 0) {
        finalOpStatus[i] = new OperationStatus(OperationStatusCode.FAILURE,
          new LabelAlreadyExistsException("Label '" + labelStr + "' already exists"));
      } else {
        byte[] row = Bytes.toBytes(ordinalCounter.get());
        Put p = new Put(row);
        p.add(builder.clear().setRow(row).setFamily(LABELS_TABLE_FAMILY)
          .setQualifier(LABEL_QUALIFIER).setTimestamp(p.getTimestamp()).setType(Type.Put)
          .setValue(label).setTags(TagUtil.fromList(Arrays.asList(LABELS_TABLE_TAGS))).build());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding the label " + labelStr);
        }
        puts.add(p);
        ordinalCounter.incrementAndGet();
      }
      i++;
    }
    if (mutateLabelsRegion(puts, finalOpStatus)) {
      updateZk(true);
    }
    return finalOpStatus;
  }

  @Override
  public OperationStatus[] setAuths(byte[] user, List<byte[]> authLabels) throws IOException {
    assert labelsRegion != null;
    OperationStatus[] finalOpStatus = new OperationStatus[authLabels.size()];
    List<Mutation> puts = new ArrayList<>(authLabels.size());
    int i = 0;
    ExtendedCellBuilder builder = ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
    for (byte[] auth : authLabels) {
      String authStr = Bytes.toString(auth);
      int labelOrdinal = this.labelsCache.getLabelOrdinal(authStr);
      if (labelOrdinal == 0) {
        // This label is not yet added. 1st this should be added to the system
        finalOpStatus[i] = new OperationStatus(OperationStatusCode.FAILURE,
          new InvalidLabelException("Label '" + authStr + "' doesn't exists"));
      } else {
        byte[] row = Bytes.toBytes(labelOrdinal);
        Put p = new Put(row);
        p.add(builder.clear().setRow(row).setFamily(LABELS_TABLE_FAMILY).setQualifier(user)
          .setTimestamp(p.getTimestamp()).setType(Cell.Type.Put).setValue(DUMMY_VALUE)
          .setTags(TagUtil.fromList(Arrays.asList(LABELS_TABLE_TAGS))).build());
        puts.add(p);
      }
      i++;
    }
    if (mutateLabelsRegion(puts, finalOpStatus)) {
      updateZk(false);
    }
    return finalOpStatus;
  }

  @Override
  public OperationStatus[] clearAuths(byte[] user, List<byte[]> authLabels) throws IOException {
    assert labelsRegion != null;
    OperationStatus[] finalOpStatus = new OperationStatus[authLabels.size()];
    List<String> currentAuths;
    if (AuthUtil.isGroupPrincipal(Bytes.toString(user))) {
      String group = AuthUtil.getGroupName(Bytes.toString(user));
      currentAuths = this.getGroupAuths(new String[] { group }, true);
    } else {
      currentAuths = this.getUserAuths(user, true);
    }
    List<Mutation> deletes = new ArrayList<>(authLabels.size());
    int i = 0;
    for (byte[] authLabel : authLabels) {
      String authLabelStr = Bytes.toString(authLabel);
      if (currentAuths.contains(authLabelStr)) {
        int labelOrdinal = this.labelsCache.getLabelOrdinal(authLabelStr);
        assert labelOrdinal > 0;
        Delete d = new Delete(Bytes.toBytes(labelOrdinal));
        d.addColumns(LABELS_TABLE_FAMILY, user);
        deletes.add(d);
      } else {
        // This label is not set for the user.
        finalOpStatus[i] =
          new OperationStatus(OperationStatusCode.FAILURE, new InvalidLabelException(
            "Label '" + authLabelStr + "' is not set for the user " + Bytes.toString(user)));
      }
      i++;
    }
    if (mutateLabelsRegion(deletes, finalOpStatus)) {
      updateZk(false);
    }
    return finalOpStatus;
  }

  /**
   * Adds the mutations to labels region and set the results to the finalOpStatus. finalOpStatus
   * might have some entries in it where the OpStatus is FAILURE. We will leave those and set in
   * others in the order. nn * @return whether we need a ZK update or not.
   */
  private boolean mutateLabelsRegion(List<Mutation> mutations, OperationStatus[] finalOpStatus)
    throws IOException {
    OperationStatus[] opStatus =
      this.labelsRegion.batchMutate(mutations.toArray(new Mutation[mutations.size()]));
    int i = 0;
    boolean updateZk = false;
    for (OperationStatus status : opStatus) {
      // Update the zk when atleast one of the mutation was added successfully.
      updateZk = updateZk || (status.getOperationStatusCode() == OperationStatusCode.SUCCESS);
      for (; i < finalOpStatus.length; i++) {
        if (finalOpStatus[i] == null) {
          finalOpStatus[i] = status;
          break;
        }
      }
    }
    return updateZk;
  }

  @Override
  public List<String> getUserAuths(byte[] user, boolean systemCall) throws IOException {
    assert (labelsRegion != null || systemCall);
    if (systemCall || labelsRegion == null) {
      return this.labelsCache.getUserAuths(Bytes.toString(user));
    }
    Scan s = new Scan();
    if (user != null && user.length > 0) {
      s.addColumn(LABELS_TABLE_FAMILY, user);
    }
    Filter filter = VisibilityUtils.createVisibilityLabelFilter(this.labelsRegion,
      new Authorizations(SYSTEM_LABEL));
    s.setFilter(filter);
    ArrayList<String> auths = new ArrayList<>();
    RegionScanner scanner = this.labelsRegion.getScanner(s);
    try {
      List<Cell> results = new ArrayList<>(1);
      while (true) {
        scanner.next(results);
        if (results.isEmpty()) break;
        Cell cell = results.get(0);
        int ordinal = PrivateCellUtil.getRowAsInt(cell);
        String label = this.labelsCache.getLabel(ordinal);
        if (label != null) {
          auths.add(label);
        }
        results.clear();
      }
    } finally {
      scanner.close();
    }
    return auths;
  }

  @Override
  public List<String> getGroupAuths(String[] groups, boolean systemCall) throws IOException {
    assert (labelsRegion != null || systemCall);
    if (systemCall || labelsRegion == null) {
      return this.labelsCache.getGroupAuths(groups);
    }
    Scan s = new Scan();
    if (groups != null && groups.length > 0) {
      for (String group : groups) {
        s.addColumn(LABELS_TABLE_FAMILY, Bytes.toBytes(AuthUtil.toGroupEntry(group)));
      }
    }
    Filter filter = VisibilityUtils.createVisibilityLabelFilter(this.labelsRegion,
      new Authorizations(SYSTEM_LABEL));
    s.setFilter(filter);
    Set<String> auths = new HashSet<>();
    RegionScanner scanner = this.labelsRegion.getScanner(s);
    try {
      List<Cell> results = new ArrayList<>(1);
      while (true) {
        scanner.next(results);
        if (results.isEmpty()) break;
        Cell cell = results.get(0);
        int ordinal = PrivateCellUtil.getRowAsInt(cell);
        String label = this.labelsCache.getLabel(ordinal);
        if (label != null) {
          auths.add(label);
        }
        results.clear();
      }
    } finally {
      scanner.close();
    }
    return new ArrayList<>(auths);
  }

  @Override
  public List<String> listLabels(String regex) throws IOException {
    assert (labelsRegion != null);
    Pair<Map<String, Integer>, Map<String, List<Integer>>> labelsAndUserAuths =
      extractLabelsAndAuths(getExistingLabelsWithAuths());
    Map<String, Integer> labels = labelsAndUserAuths.getFirst();
    labels.remove(SYSTEM_LABEL);
    if (regex != null) {
      Pattern pattern = Pattern.compile(regex);
      ArrayList<String> matchedLabels = new ArrayList<>();
      for (String label : labels.keySet()) {
        if (pattern.matcher(label).matches()) {
          matchedLabels.add(label);
        }
      }
      return matchedLabels;
    }
    return new ArrayList<>(labels.keySet());
  }

  @Override
  public List<Tag> createVisibilityExpTags(String visExpression, boolean withSerializationFormat,
    boolean checkAuths) throws IOException {
    Set<Integer> auths = new HashSet<>();
    if (checkAuths) {
      User user = VisibilityUtils.getActiveUser();
      auths.addAll(this.labelsCache.getUserAuthsAsOrdinals(user.getShortName()));
      auths.addAll(this.labelsCache.getGroupAuthsAsOrdinals(user.getGroupNames()));
    }
    return VisibilityUtils.createVisibilityExpTags(visExpression, withSerializationFormat,
      checkAuths, auths, labelsCache);
  }

  protected void updateZk(boolean labelAddition) throws IOException {
    // We will add to zookeeper here.
    // TODO we should add the delta only to zk. Else this will be a very heavy op and when there are
    // so many labels and auth in the system, we will end up adding lots of data to zk. Most
    // possibly we will exceed zk node data limit!
    Pair<Map<String, Integer>, Map<String, List<Integer>>> labelsAndUserAuths =
      extractLabelsAndAuths(getExistingLabelsWithAuths());
    Map<String, Integer> existingLabels = labelsAndUserAuths.getFirst();
    Map<String, List<Integer>> userAuths = labelsAndUserAuths.getSecond();
    if (labelAddition) {
      byte[] serialized = VisibilityUtils.getDataToWriteToZooKeeper(existingLabels);
      this.labelsCache.writeToZookeeper(serialized, true);
    } else {
      byte[] serialized = VisibilityUtils.getUserAuthsDataToWriteToZooKeeper(userAuths);
      this.labelsCache.writeToZookeeper(serialized, false);
    }
  }

  @Override
  public VisibilityExpEvaluator getVisibilityExpEvaluator(Authorizations authorizations)
    throws IOException {
    // If a super user issues a get/scan, he should be able to scan the cells
    // irrespective of the Visibility labels
    if (isReadFromSystemAuthUser()) {
      return new VisibilityExpEvaluator() {
        @Override
        public boolean evaluate(Cell cell) throws IOException {
          return true;
        }
      };
    }
    List<String> authLabels = null;
    for (ScanLabelGenerator scanLabelGenerator : scanLabelGenerators) {
      try {
        // null authorizations to be handled inside SLG impl.
        authLabels = scanLabelGenerator.getLabels(VisibilityUtils.getActiveUser(), authorizations);
        authLabels = (authLabels == null) ? new ArrayList<>() : authLabels;
        authorizations = new Authorizations(authLabels);
      } catch (Throwable t) {
        LOG.error(t.toString(), t);
        throw new IOException(t);
      }
    }
    int labelsCount = this.labelsCache.getLabelsCount();
    final BitSet bs = new BitSet(labelsCount + 1); // ordinal is index 1 based
    if (authLabels != null) {
      for (String authLabel : authLabels) {
        int labelOrdinal = this.labelsCache.getLabelOrdinal(authLabel);
        if (labelOrdinal != 0) {
          bs.set(labelOrdinal);
        }
      }
    }

    return new VisibilityExpEvaluator() {
      @Override
      public boolean evaluate(Cell cell) throws IOException {
        boolean visibilityTagPresent = false;
        Iterator<Tag> tagsItr = PrivateCellUtil.tagsIterator(cell);
        while (tagsItr.hasNext()) {
          boolean includeKV = true;
          Tag tag = tagsItr.next();
          if (tag.getType() == VISIBILITY_TAG_TYPE) {
            visibilityTagPresent = true;
            int offset = tag.getValueOffset();
            int endOffset = offset + tag.getValueLength();
            while (offset < endOffset) {
              Pair<Integer, Integer> result = TagUtil.readVIntValuePart(tag, offset);
              int currLabelOrdinal = result.getFirst();
              if (currLabelOrdinal < 0) {
                // check for the absence of this label in the Scan Auth labels
                // ie. to check BitSet corresponding bit is 0
                int temp = -currLabelOrdinal;
                if (bs.get(temp)) {
                  includeKV = false;
                  break;
                }
              } else {
                if (!bs.get(currLabelOrdinal)) {
                  includeKV = false;
                  break;
                }
              }
              offset += result.getSecond();
            }
            if (includeKV) {
              // We got one visibility expression getting evaluated to true. Good to include this
              // KV in the result then.
              return true;
            }
          }
        }
        return !(visibilityTagPresent);
      }
    };
  }

  protected boolean isReadFromSystemAuthUser() throws IOException {
    User user = VisibilityUtils.getActiveUser();
    return havingSystemAuth(user);
  }

  @Override
  public boolean havingSystemAuth(User user) throws IOException {
    // A super user has 'system' auth.
    if (Superusers.isSuperUser(user)) {
      return true;
    }
    // A user can also be explicitly granted 'system' auth.
    List<String> auths = this.getUserAuths(Bytes.toBytes(user.getShortName()), true);
    if (LOG.isTraceEnabled()) {
      LOG.trace("The auths for user " + user.getShortName() + " are " + auths);
    }
    if (auths.contains(SYSTEM_LABEL)) {
      return true;
    }
    auths = this.getGroupAuths(user.getGroupNames(), true);
    if (LOG.isTraceEnabled()) {
      LOG.trace("The auths for groups of user " + user.getShortName() + " are " + auths);
    }
    return auths.contains(SYSTEM_LABEL);
  }

  @Override
  public boolean matchVisibility(List<Tag> putVisTags, Byte putTagsFormat, List<Tag> deleteVisTags,
    Byte deleteTagsFormat) throws IOException {
    // Early out if there are no tags in both of cell and delete
    if (putVisTags.isEmpty() && deleteVisTags.isEmpty()) {
      return true;
    }
    // Early out if one of the tags is empty
    if (putVisTags.isEmpty() ^ deleteVisTags.isEmpty()) {
      return false;
    }
    if (
      (deleteTagsFormat != null && deleteTagsFormat == SORTED_ORDINAL_SERIALIZATION_FORMAT)
        && (putTagsFormat == null || putTagsFormat == SORTED_ORDINAL_SERIALIZATION_FORMAT)
    ) {
      if (putTagsFormat == null) {
        return matchUnSortedVisibilityTags(putVisTags, deleteVisTags);
      } else {
        return matchOrdinalSortedVisibilityTags(putVisTags, deleteVisTags);
      }
    }
    throw new IOException("Unexpected tag format passed for comparison, deleteTagsFormat : "
      + deleteTagsFormat + ", putTagsFormat : " + putTagsFormat);
  }

  /**
   * @param putVisTags    Visibility tags in Put Mutation
   * @param deleteVisTags Visibility tags in Delete Mutation
   * @return true when all the visibility tags in Put matches with visibility tags in Delete. This
   *         is used when, at least one set of tags are not sorted based on the label ordinal.
   */
  private static boolean matchUnSortedVisibilityTags(List<Tag> putVisTags, List<Tag> deleteVisTags)
    throws IOException {
    return compareTagsOrdinals(sortTagsBasedOnOrdinal(putVisTags),
      sortTagsBasedOnOrdinal(deleteVisTags));
  }

  /**
   * @param putVisTags    Visibility tags in Put Mutation
   * @param deleteVisTags Visibility tags in Delete Mutation
   * @return true when all the visibility tags in Put matches with visibility tags in Delete. This
   *         is used when both the set of tags are sorted based on the label ordinal.
   */
  private static boolean matchOrdinalSortedVisibilityTags(List<Tag> putVisTags,
    List<Tag> deleteVisTags) {
    boolean matchFound = false;
    // If the size does not match. Definitely we are not comparing the equal tags.
    if ((deleteVisTags.size()) == putVisTags.size()) {
      for (Tag tag : deleteVisTags) {
        matchFound = false;
        for (Tag givenTag : putVisTags) {
          if (Tag.matchingValue(tag, givenTag)) {
            matchFound = true;
            break;
          }
        }
        if (!matchFound) break;
      }
    }
    return matchFound;
  }

  private static List<List<Integer>> sortTagsBasedOnOrdinal(List<Tag> tags) throws IOException {
    List<List<Integer>> fullTagsList = new ArrayList<>();
    for (Tag tag : tags) {
      if (tag.getType() == VISIBILITY_TAG_TYPE) {
        getSortedTagOrdinals(fullTagsList, tag);
      }
    }
    return fullTagsList;
  }

  private static void getSortedTagOrdinals(List<List<Integer>> fullTagsList, Tag tag)
    throws IOException {
    List<Integer> tagsOrdinalInSortedOrder = new ArrayList<>();
    int offset = tag.getValueOffset();
    int endOffset = offset + tag.getValueLength();
    while (offset < endOffset) {
      Pair<Integer, Integer> result = TagUtil.readVIntValuePart(tag, offset);
      tagsOrdinalInSortedOrder.add(result.getFirst());
      offset += result.getSecond();
    }
    Collections.sort(tagsOrdinalInSortedOrder);
    fullTagsList.add(tagsOrdinalInSortedOrder);
  }

  /*
   * @return true when all the visibility tags in Put matches with visibility tags in Delete.
   */
  private static boolean compareTagsOrdinals(List<List<Integer>> putVisTags,
    List<List<Integer>> deleteVisTags) {
    boolean matchFound = false;
    if (deleteVisTags.size() == putVisTags.size()) {
      for (List<Integer> deleteTagOrdinals : deleteVisTags) {
        matchFound = false;
        for (List<Integer> tagOrdinals : putVisTags) {
          if (deleteTagOrdinals.equals(tagOrdinals)) {
            matchFound = true;
            break;
          }
        }
        if (!matchFound) break;
      }
    }
    return matchFound;
  }

  @Override
  public byte[] encodeVisibilityForReplication(final List<Tag> tags, final Byte serializationFormat)
    throws IOException {
    if (
      tags.size() > 0 && (serializationFormat == null
        || serializationFormat == SORTED_ORDINAL_SERIALIZATION_FORMAT)
    ) {
      return createModifiedVisExpression(tags);
    }
    return null;
  }

  /**
   * n * - all the visibility tags associated with the current Cell
   * @return - the modified visibility expression as byte[]
   */
  private byte[] createModifiedVisExpression(final List<Tag> tags) throws IOException {
    StringBuilder visibilityString = new StringBuilder();
    for (Tag tag : tags) {
      if (tag.getType() == TagType.VISIBILITY_TAG_TYPE) {
        if (visibilityString.length() != 0) {
          visibilityString.append(VisibilityConstants.CLOSED_PARAN)
            .append(VisibilityConstants.OR_OPERATOR);
        }
        int offset = tag.getValueOffset();
        int endOffset = offset + tag.getValueLength();
        boolean expressionStart = true;
        while (offset < endOffset) {
          Pair<Integer, Integer> result = TagUtil.readVIntValuePart(tag, offset);
          int currLabelOrdinal = result.getFirst();
          if (currLabelOrdinal < 0) {
            int temp = -currLabelOrdinal;
            String label = this.labelsCache.getLabel(temp);
            if (expressionStart) {
              // Quote every label in case of unicode characters if present
              visibilityString.append(VisibilityConstants.OPEN_PARAN)
                .append(VisibilityConstants.NOT_OPERATOR).append(CellVisibility.quote(label));
            } else {
              visibilityString.append(VisibilityConstants.AND_OPERATOR)
                .append(VisibilityConstants.NOT_OPERATOR).append(CellVisibility.quote(label));
            }
          } else {
            String label = this.labelsCache.getLabel(currLabelOrdinal);
            if (expressionStart) {
              visibilityString.append(VisibilityConstants.OPEN_PARAN)
                .append(CellVisibility.quote(label));
            } else {
              visibilityString.append(VisibilityConstants.AND_OPERATOR)
                .append(CellVisibility.quote(label));
            }
          }
          expressionStart = false;
          offset += result.getSecond();
        }
      }
    }
    if (visibilityString.length() != 0) {
      visibilityString.append(VisibilityConstants.CLOSED_PARAN);
      // Return the string formed as byte[]
      return Bytes.toBytes(visibilityString.toString());
    }
    return null;
  }
}
