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
import static org.apache.hadoop.hbase.security.visibility.VisibilityUtils.SYSTEM_LABEL;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.visibility.expression.ExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.LeafExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.NonLeafExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.Operator;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a VisibilityLabelService where labels in Mutation's visibility expression will be
 * persisted as Strings itself rather than ordinals in 'labels' table. Also there is no need to add
 * labels to the system, prior to using them in Mutations/Authorizations.
 */
@InterfaceAudience.Private
public class ExpAsStringVisibilityLabelServiceImpl implements VisibilityLabelService {
  private static final Logger LOG =
    LoggerFactory.getLogger(ExpAsStringVisibilityLabelServiceImpl.class);

  private static final byte[] DUMMY_VALUE = new byte[0];
  private static final byte STRING_SERIALIZATION_FORMAT = 2;
  private static final Tag STRING_SERIALIZATION_FORMAT_TAG =
    new ArrayBackedTag(TagType.VISIBILITY_EXP_SERIALIZATION_FORMAT_TAG_TYPE,
      new byte[] { STRING_SERIALIZATION_FORMAT });
  private final ExpressionParser expressionParser = new ExpressionParser();
  private final ExpressionExpander expressionExpander = new ExpressionExpander();
  private Configuration conf;
  private Region labelsRegion;
  private List<ScanLabelGenerator> scanLabelGenerators;

  @Override
  public OperationStatus[] addLabels(List<byte[]> labels) throws IOException {
    // Not doing specific label add. We will just add labels in Mutation
    // visibility expression as it
    // is along with every cell.
    OperationStatus[] status = new OperationStatus[labels.size()];
    for (int i = 0; i < labels.size(); i++) {
      status[i] = new OperationStatus(OperationStatusCode.SUCCESS);
    }
    return status;
  }

  @Override
  public OperationStatus[] setAuths(byte[] user, List<byte[]> authLabels) throws IOException {
    assert labelsRegion != null;
    OperationStatus[] finalOpStatus = new OperationStatus[authLabels.size()];
    Put p = new Put(user);
    CellBuilder builder = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
    for (byte[] auth : authLabels) {
      p.add(builder.clear().setRow(p.getRow()).setFamily(LABELS_TABLE_FAMILY).setQualifier(auth)
        .setTimestamp(p.getTimestamp()).setType(Cell.Type.Put).setValue(DUMMY_VALUE).build());
    }
    this.labelsRegion.put(p);
    // This is a testing impl and so not doing any caching
    for (int i = 0; i < authLabels.size(); i++) {
      finalOpStatus[i] = new OperationStatus(OperationStatusCode.SUCCESS);
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
    Delete d = new Delete(user);
    int i = 0;
    for (byte[] authLabel : authLabels) {
      String authLabelStr = Bytes.toString(authLabel);
      if (currentAuths.contains(authLabelStr)) {
        d.addColumns(LABELS_TABLE_FAMILY, authLabel);
      } else {
        // This label is not set for the user.
        finalOpStatus[i] =
          new OperationStatus(OperationStatusCode.FAILURE, new InvalidLabelException(
            "Label '" + authLabelStr + "' is not set for the user " + Bytes.toString(user)));
      }
      i++;
    }
    this.labelsRegion.delete(d);
    // This is a testing impl and so not doing any caching
    for (i = 0; i < authLabels.size(); i++) {
      if (finalOpStatus[i] == null) {
        finalOpStatus[i] = new OperationStatus(OperationStatusCode.SUCCESS);
      }
    }
    return finalOpStatus;
  }

  @Override
  public List<String> getUserAuths(byte[] user, boolean systemCall) throws IOException {
    assert (labelsRegion != null || systemCall);
    List<String> auths = new ArrayList<>();
    Get get = new Get(user);
    getAuths(get, auths);
    return auths;
  }

  @Override
  public List<String> getGroupAuths(String[] groups, boolean systemCall) throws IOException {
    assert (labelsRegion != null || systemCall);
    List<String> auths = new ArrayList<>();
    if (groups != null && groups.length > 0) {
      for (String group : groups) {
        Get get = new Get(Bytes.toBytes(AuthUtil.toGroupEntry(group)));
        getAuths(get, auths);
      }
    }
    return auths;
  }

  private void getAuths(Get get, List<String> auths) throws IOException {
    List<Cell> cells = new ArrayList<>();
    RegionScanner scanner = null;
    try {
      if (labelsRegion == null) {
        Table table = null;
        Connection connection = null;
        try {
          connection = ConnectionFactory.createConnection(conf);
          table = connection.getTable(VisibilityConstants.LABELS_TABLE_NAME);
          Result result = table.get(get);
          cells = result.listCells();
        } finally {
          if (table != null) {
            table.close();
          }
          if (connection != null) {
            connection.close();
          }
        }
      } else {
        // NOTE: Please don't use HRegion.get() instead,
        // because it will copy cells to heap. See HBASE-26036
        scanner = this.labelsRegion.getScanner(new Scan(get));
        scanner.next(cells);
      }
      for (Cell cell : cells) {
        String auth = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
          cell.getQualifierLength());
        auths.add(auth);
      }
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
  }

  @Override
  public List<String> listLabels(String regex) throws IOException {
    // return an empty list for this implementation.
    return new ArrayList<>();
  }

  @Override
  public List<Tag> createVisibilityExpTags(String visExpression, boolean withSerializationFormat,
    boolean checkAuths) throws IOException {
    ExpressionNode node = null;
    try {
      node = this.expressionParser.parse(visExpression);
    } catch (ParseException e) {
      throw new IOException(e);
    }
    node = this.expressionExpander.expand(node);
    List<Tag> tags = new ArrayList<>();
    if (withSerializationFormat) {
      tags.add(STRING_SERIALIZATION_FORMAT_TAG);
    }
    if (
      node instanceof NonLeafExpressionNode
        && ((NonLeafExpressionNode) node).getOperator() == Operator.OR
    ) {
      for (ExpressionNode child : ((NonLeafExpressionNode) node).getChildExps()) {
        tags.add(createTag(child));
      }
    } else {
      tags.add(createTag(node));
    }
    return tags;
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
    final List<String> authLabelsFinal = authLabels;
    return new VisibilityExpEvaluator() {
      @Override
      public boolean evaluate(Cell cell) throws IOException {
        boolean visibilityTagPresent = false;
        // Save an object allocation where we can
        if (cell.getTagsLength() > 0) {
          Iterator<Tag> tagsItr = PrivateCellUtil.tagsIterator(cell);
          while (tagsItr.hasNext()) {
            boolean includeKV = true;
            Tag tag = tagsItr.next();
            if (tag.getType() == VISIBILITY_TAG_TYPE) {
              visibilityTagPresent = true;
              int offset = tag.getValueOffset();
              int endOffset = offset + tag.getValueLength();
              while (offset < endOffset) {
                short len = getTagValuePartAsShort(tag, offset);
                offset += 2;
                if (len < 0) {
                  // This is a NOT label.
                  len = (short) (-1 * len);
                  String label = getTagValuePartAsString(tag, offset, len);
                  if (authLabelsFinal.contains(label)) {
                    includeKV = false;
                    break;
                  }
                } else {
                  String label = getTagValuePartAsString(tag, offset, len);
                  if (!authLabelsFinal.contains(label)) {
                    includeKV = false;
                    break;
                  }
                }
                offset += len;
              }
              if (includeKV) {
                // We got one visibility expression getting evaluated to true.
                // Good to include this
                // KV in the result then.
                return true;
              }
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

  private Tag createTag(ExpressionNode node) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    List<String> labels = new ArrayList<>();
    List<String> notLabels = new ArrayList<>();
    extractLabels(node, labels, notLabels);
    Collections.sort(labels);
    Collections.sort(notLabels);
    // We will write the NOT labels 1st followed by normal labels
    // Each of the label we will write with label length (as short 1st) followed
    // by the label bytes.
    // For a NOT node we will write the label length as -ve.
    for (String label : notLabels) {
      byte[] bLabel = Bytes.toBytes(label);
      short length = (short) bLabel.length;
      length = (short) (-1 * length);
      dos.writeShort(length);
      dos.write(bLabel);
    }
    for (String label : labels) {
      byte[] bLabel = Bytes.toBytes(label);
      dos.writeShort(bLabel.length);
      dos.write(bLabel);
    }
    return new ArrayBackedTag(VISIBILITY_TAG_TYPE, baos.toByteArray());
  }

  private void extractLabels(ExpressionNode node, List<String> labels, List<String> notLabels) {
    if (node.isSingleNode()) {
      if (node instanceof NonLeafExpressionNode) {
        // This is a NOT node.
        LeafExpressionNode lNode =
          (LeafExpressionNode) ((NonLeafExpressionNode) node).getChildExps().get(0);
        notLabels.add(lNode.getIdentifier());
      } else {
        labels.add(((LeafExpressionNode) node).getIdentifier());
      }
    } else {
      // A non leaf expression of labels with & operator.
      NonLeafExpressionNode nlNode = (NonLeafExpressionNode) node;
      assert nlNode.getOperator() == Operator.AND;
      List<ExpressionNode> childExps = nlNode.getChildExps();
      for (ExpressionNode child : childExps) {
        extractLabels(child, labels, notLabels);
      }
    }
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void init(RegionCoprocessorEnvironment e) throws IOException {
    this.scanLabelGenerators = VisibilityUtils.getScanLabelGenerators(this.conf);
    if (e.getRegion().getRegionInfo().getTable().equals(LABELS_TABLE_NAME)) {
      this.labelsRegion = e.getRegion();
    }
  }

  @Override
  public boolean havingSystemAuth(User user) throws IOException {
    if (Superusers.isSuperUser(user)) {
      return true;
    }
    Set<String> auths = new HashSet<>();
    auths.addAll(this.getUserAuths(Bytes.toBytes(user.getShortName()), true));
    auths.addAll(this.getGroupAuths(user.getGroupNames(), true));
    return auths.contains(SYSTEM_LABEL);
  }

  @Override
  public boolean matchVisibility(List<Tag> putTags, Byte putTagsFormat, List<Tag> deleteTags,
    Byte deleteTagsFormat) throws IOException {
    assert putTagsFormat == STRING_SERIALIZATION_FORMAT;
    assert deleteTagsFormat == STRING_SERIALIZATION_FORMAT;
    return checkForMatchingVisibilityTagsWithSortedOrder(putTags, deleteTags);
  }

  private static boolean checkForMatchingVisibilityTagsWithSortedOrder(List<Tag> putVisTags,
    List<Tag> deleteVisTags) {
    // Early out if there are no tags in both of cell and delete
    if (putVisTags.isEmpty() && deleteVisTags.isEmpty()) {
      return true;
    }
    boolean matchFound = false;
    // If the size does not match. Definitely we are not comparing the equal
    // tags.
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

  @Override
  public byte[] encodeVisibilityForReplication(final List<Tag> tags, final Byte serializationFormat)
    throws IOException {
    if (
      tags.size() > 0
        && (serializationFormat == null || serializationFormat == STRING_SERIALIZATION_FORMAT)
    ) {
      return createModifiedVisExpression(tags);
    }
    return null;
  }

  /**
   * @param tags - all the tags associated with the current Cell
   * @return - the modified visibility expression as byte[]
   */
  private byte[] createModifiedVisExpression(final List<Tag> tags) throws IOException {
    StringBuilder visibilityString = new StringBuilder();
    for (Tag tag : tags) {
      if (tag.getType() == TagType.VISIBILITY_TAG_TYPE) {
        if (visibilityString.length() != 0) {
          visibilityString
            .append(VisibilityConstants.CLOSED_PARAN + VisibilityConstants.OR_OPERATOR);
        }
        int offset = tag.getValueOffset();
        int endOffset = offset + tag.getValueLength();
        boolean expressionStart = true;
        while (offset < endOffset) {
          short len = getTagValuePartAsShort(tag, offset);
          offset += 2;
          if (len < 0) {
            len = (short) (-1 * len);
            String label = getTagValuePartAsString(tag, offset, len);
            if (expressionStart) {
              visibilityString.append(VisibilityConstants.OPEN_PARAN
                + VisibilityConstants.NOT_OPERATOR + CellVisibility.quote(label));
            } else {
              visibilityString.append(VisibilityConstants.AND_OPERATOR
                + VisibilityConstants.NOT_OPERATOR + CellVisibility.quote(label));
            }
          } else {
            String label = getTagValuePartAsString(tag, offset, len);
            if (expressionStart) {
              visibilityString.append(VisibilityConstants.OPEN_PARAN + CellVisibility.quote(label));
            } else {
              visibilityString
                .append(VisibilityConstants.AND_OPERATOR + CellVisibility.quote(label));
            }
          }
          expressionStart = false;
          offset += len;
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

  private static short getTagValuePartAsShort(Tag t, int offset) {
    if (t.hasArray()) {
      return Bytes.toShort(t.getValueArray(), offset);
    }
    return ByteBufferUtils.toShort(t.getValueByteBuffer(), offset);
  }

  private static String getTagValuePartAsString(Tag t, int offset, int length) {
    if (t.hasArray()) {
      return Bytes.toString(t.getValueArray(), offset, length);
    }
    byte[] b = new byte[length];
    ByteBufferUtils.copyFromBufferToArray(b, t.getValueByteBuffer(), offset, 0, length);
    return Bytes.toString(b);
  }
}
