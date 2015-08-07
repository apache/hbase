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

import static org.apache.hadoop.hbase.TagType.VISIBILITY_TAG_TYPE;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.MultiUserAuthorizations;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.UserAuthorizations;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabel;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsRequest;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.visibility.expression.ExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.LeafExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.NonLeafExpressionNode;
import org.apache.hadoop.hbase.security.visibility.expression.Operator;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.SimpleMutableByteRange;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Utility method to support visibility
 */
@InterfaceAudience.Private
public class VisibilityUtils {

  private static final Log LOG = LogFactory.getLog(VisibilityUtils.class);

  public static final String VISIBILITY_LABEL_GENERATOR_CLASS =
      "hbase.regionserver.scan.visibility.label.generator.class";
  public static final String SYSTEM_LABEL = "system";
  public static final Tag SORTED_ORDINAL_SERIALIZATION_FORMAT_TAG = new Tag(
      TagType.VISIBILITY_EXP_SERIALIZATION_FORMAT_TAG_TYPE,
      VisibilityConstants.SORTED_ORDINAL_SERIALIZATION_FORMAT_TAG_VAL);
  private static final String COMMA = ",";

  private static final ExpressionParser EXP_PARSER = new ExpressionParser();
  private static final ExpressionExpander EXP_EXPANDER = new ExpressionExpander();

  /**
   * Creates the labels data to be written to zookeeper.
   * @param existingLabels
   * @return Bytes form of labels and their ordinal details to be written to zookeeper.
   */
  public static byte[] getDataToWriteToZooKeeper(Map<String, Integer> existingLabels) {
    VisibilityLabelsRequest.Builder visReqBuilder = VisibilityLabelsRequest.newBuilder();
    for (Entry<String, Integer> entry : existingLabels.entrySet()) {
      VisibilityLabel.Builder visLabBuilder = VisibilityLabel.newBuilder();
      visLabBuilder.setLabel(ByteStringer.wrap(Bytes.toBytes(entry.getKey())));
      visLabBuilder.setOrdinal(entry.getValue());
      visReqBuilder.addVisLabel(visLabBuilder.build());
    }
    return ProtobufUtil.prependPBMagic(visReqBuilder.build().toByteArray());
  }

  /**
   * Creates the user auth data to be written to zookeeper.
   * @param userAuths
   * @return Bytes form of user auths details to be written to zookeeper.
   */
  public static byte[] getUserAuthsDataToWriteToZooKeeper(Map<String, List<Integer>> userAuths) {
    MultiUserAuthorizations.Builder builder = MultiUserAuthorizations.newBuilder();
    for (Entry<String, List<Integer>> entry : userAuths.entrySet()) {
      UserAuthorizations.Builder userAuthsBuilder = UserAuthorizations.newBuilder();
      userAuthsBuilder.setUser(ByteStringer.wrap(Bytes.toBytes(entry.getKey())));
      for (Integer label : entry.getValue()) {
        userAuthsBuilder.addAuth(label);
      }
      builder.addUserAuths(userAuthsBuilder.build());
    }
    return ProtobufUtil.prependPBMagic(builder.build().toByteArray());
  }

  /**
   * Reads back from the zookeeper. The data read here is of the form written by
   * writeToZooKeeper(Map&lt;byte[], Integer&gt; entries).
   * 
   * @param data
   * @return Labels and their ordinal details
   * @throws DeserializationException
   */
  public static List<VisibilityLabel> readLabelsFromZKData(byte[] data)
      throws DeserializationException {
    if (ProtobufUtil.isPBMagicPrefix(data)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      try {
        VisibilityLabelsRequest.Builder builder = VisibilityLabelsRequest.newBuilder();
        ProtobufUtil.mergeFrom(builder, data, pblen, data.length - pblen);
        return builder.getVisLabelList();
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
    }
    return null;
  }

  /**
   * Reads back User auth data written to zookeeper.
   * @param data
   * @return User auth details
   * @throws DeserializationException
   */
  public static MultiUserAuthorizations readUserAuthsFromZKData(byte[] data) 
      throws DeserializationException {
    if (ProtobufUtil.isPBMagicPrefix(data)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      try {
        MultiUserAuthorizations.Builder builder = MultiUserAuthorizations.newBuilder();
        ProtobufUtil.mergeFrom(builder, data, pblen, data.length - pblen);
        return builder.build();
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
    }
    return null;
  }

  /**
   * @param conf The configuration to use
   * @return Stack of ScanLabelGenerator instances. ScanLabelGenerator classes can be specified in
   *         Configuration as comma separated list using key
   *         "hbase.regionserver.scan.visibility.label.generator.class"
   * @throws IllegalArgumentException
   *           when any of the specified ScanLabelGenerator class can not be loaded.
   */
  public static List<ScanLabelGenerator> getScanLabelGenerators(Configuration conf) {
    // There can be n SLG specified as comma separated in conf
    String slgClassesCommaSeparated = conf.get(VISIBILITY_LABEL_GENERATOR_CLASS);
    // We have only System level SLGs now. The order of execution will be same as the order in the
    // comma separated config value
    List<ScanLabelGenerator> slgs = new ArrayList<ScanLabelGenerator>();
    if (StringUtils.isNotEmpty(slgClassesCommaSeparated)) {
      String[] slgClasses = slgClassesCommaSeparated.split(COMMA);
      for (String slgClass : slgClasses) {
        Class<? extends ScanLabelGenerator> slgKlass;
        try {
          slgKlass = (Class<? extends ScanLabelGenerator>) conf.getClassByName(slgClass.trim());
          slgs.add(ReflectionUtils.newInstance(slgKlass, conf));
        } catch (ClassNotFoundException e) {
          throw new IllegalArgumentException("Unable to find " + slgClass, e);
        }
      }
    }
    // If no SLG is specified in conf, by default we'll add two SLGs
    // 1. FeedUserAuthScanLabelGenerator
    // 2. DefinedSetFilterScanLabelGenerator
    // This stacking will achieve the following default behavior:
    // 1. If there is no Auths in the scan, we will obtain the global defined set for the user
    //    from the labels table.
    // 2. If there is Auths in the scan, we will examine the passed in Auths and filter out the
    //    labels that the user is not entitled to. Then use the resulting label set.
    if (slgs.isEmpty()) {
      slgs.add(ReflectionUtils.newInstance(FeedUserAuthScanLabelGenerator.class, conf));
      slgs.add(ReflectionUtils.newInstance(DefinedSetFilterScanLabelGenerator.class, conf));
    }
    return slgs;
  }

  /**
   * Extract the visibility tags of the given Cell into the given List
   * @param cell - the cell
   * @param tags - the array that will be populated if visibility tags are present
   * @return The visibility tags serialization format
   */
  public static Byte extractVisibilityTags(Cell cell, List<Tag> tags) {
    Byte serializationFormat = null;
    if (cell.getTagsLength() > 0) {
      Iterator<Tag> tagsIterator = CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(),
          cell.getTagsLength());
      while (tagsIterator.hasNext()) {
        Tag tag = tagsIterator.next();
        if (tag.getType() == TagType.VISIBILITY_EXP_SERIALIZATION_FORMAT_TAG_TYPE) {
          serializationFormat = tag.getBuffer()[tag.getTagOffset()];
        } else if (tag.getType() == VISIBILITY_TAG_TYPE) {
          tags.add(tag);
        }
      }
    }
    return serializationFormat;
  }

  /**
   * Extracts and partitions the visibility tags and nonVisibility Tags
   *
   * @param cell - the cell for which we would extract and partition the
   * visibility and non visibility tags
   * @param visTags
   *          - all the visibilty tags of type TagType.VISIBILITY_TAG_TYPE would
   *          be added to this list
   * @param nonVisTags - all the non visibility tags would be added to this list
   * @return - the serailization format of the tag. Can be null if no tags are found or
   * if there is no visibility tag found
   */
  public static Byte extractAndPartitionTags(Cell cell, List<Tag> visTags,
      List<Tag> nonVisTags) {
    Byte serializationFormat = null;
    if (cell.getTagsLength() > 0) {
      Iterator<Tag> tagsIterator = CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(),
          cell.getTagsLength());
      while (tagsIterator.hasNext()) {
        Tag tag = tagsIterator.next();
        if (tag.getType() == TagType.VISIBILITY_EXP_SERIALIZATION_FORMAT_TAG_TYPE) {
          serializationFormat = tag.getBuffer()[tag.getTagOffset()];
        } else if (tag.getType() == VISIBILITY_TAG_TYPE) {
          visTags.add(tag);
        } else {
          // ignore string encoded visibility expressions, will be added in replication handling
          nonVisTags.add(tag);
        }
      }
    }
    return serializationFormat;
  }

  public static boolean isVisibilityTagsPresent(Cell cell) {
    if (cell.getTagsLength() == 0) {
      return false;
    }
    Iterator<Tag> tagsIterator = CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(),
        cell.getTagsLength());
    while (tagsIterator.hasNext()) {
      Tag tag = tagsIterator.next();
      if (tag.getType() == VISIBILITY_TAG_TYPE) {
        return true;
      }
    }
    return false;
  }

  public static Filter createVisibilityLabelFilter(Region region, Authorizations authorizations)
      throws IOException {
    Map<ByteRange, Integer> cfVsMaxVersions = new HashMap<ByteRange, Integer>();
    for (HColumnDescriptor hcd : region.getTableDesc().getFamilies()) {
      cfVsMaxVersions.put(new SimpleMutableByteRange(hcd.getName()), hcd.getMaxVersions());
    }
    VisibilityLabelService vls = VisibilityLabelServiceManager.getInstance()
        .getVisibilityLabelService();
    Filter visibilityLabelFilter = new VisibilityLabelFilter(
        vls.getVisibilityExpEvaluator(authorizations), cfVsMaxVersions);
    return visibilityLabelFilter;
  }

  /**
   * @return User who called RPC method. For non-RPC handling, falls back to system user
   * @throws IOException When there is IOE in getting the system user (During non-RPC handling).
   */
  public static User getActiveUser() throws IOException {
    User user = RpcServer.getRequestUser();
    if (user == null) {
      user = User.getCurrent();
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Current active user name is " + user.getShortName());
    }
    return user;
  }

  public static List<Tag> createVisibilityExpTags(String visExpression,
      boolean withSerializationFormat, boolean checkAuths, Set<Integer> auths,
      VisibilityLabelOrdinalProvider ordinalProvider) throws IOException {
    ExpressionNode node = null;
    try {
      node = EXP_PARSER.parse(visExpression);
    } catch (ParseException e) {
      throw new IOException(e);
    }
    node = EXP_EXPANDER.expand(node);
    List<Tag> tags = new ArrayList<Tag>();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    List<Integer> labelOrdinals = new ArrayList<Integer>();
    // We will be adding this tag before the visibility tags and the presence of this
    // tag indicates we are supporting deletes with cell visibility
    if (withSerializationFormat) {
      tags.add(VisibilityUtils.SORTED_ORDINAL_SERIALIZATION_FORMAT_TAG);
    }
    if (node.isSingleNode()) {
      getLabelOrdinals(node, labelOrdinals, auths, checkAuths, ordinalProvider);
      writeLabelOrdinalsToStream(labelOrdinals, dos);
      tags.add(new Tag(VISIBILITY_TAG_TYPE, baos.toByteArray()));
      baos.reset();
    } else {
      NonLeafExpressionNode nlNode = (NonLeafExpressionNode) node;
      if (nlNode.getOperator() == Operator.OR) {
        for (ExpressionNode child : nlNode.getChildExps()) {
          getLabelOrdinals(child, labelOrdinals, auths, checkAuths, ordinalProvider);
          writeLabelOrdinalsToStream(labelOrdinals, dos);
          tags.add(new Tag(VISIBILITY_TAG_TYPE, baos.toByteArray()));
          baos.reset();
          labelOrdinals.clear();
        }
      } else {
        getLabelOrdinals(nlNode, labelOrdinals, auths, checkAuths, ordinalProvider);
        writeLabelOrdinalsToStream(labelOrdinals, dos);
        tags.add(new Tag(VISIBILITY_TAG_TYPE, baos.toByteArray()));
        baos.reset();
      }
    }
    return tags;
  }

  private static void getLabelOrdinals(ExpressionNode node, List<Integer> labelOrdinals,
      Set<Integer> auths, boolean checkAuths, VisibilityLabelOrdinalProvider ordinalProvider)
      throws IOException, InvalidLabelException {
    if (node.isSingleNode()) {
      String identifier = null;
      int labelOrdinal = 0;
      if (node instanceof LeafExpressionNode) {
        identifier = ((LeafExpressionNode) node).getIdentifier();
        if (LOG.isTraceEnabled()) {
          LOG.trace("The identifier is " + identifier);
        }
        labelOrdinal = ordinalProvider.getLabelOrdinal(identifier);
        checkAuths(auths, labelOrdinal, identifier, checkAuths);
      } else {
        // This is a NOT node.
        LeafExpressionNode lNode = (LeafExpressionNode) ((NonLeafExpressionNode) node)
            .getChildExps().get(0);
        identifier = lNode.getIdentifier();
        labelOrdinal = ordinalProvider.getLabelOrdinal(identifier);
        checkAuths(auths, labelOrdinal, identifier, checkAuths);
        labelOrdinal = -1 * labelOrdinal; // Store NOT node as -ve ordinal.
      }
      if (labelOrdinal == 0) {
        throw new InvalidLabelException("Invalid visibility label " + identifier);
      }
      labelOrdinals.add(labelOrdinal);
    } else {
      List<ExpressionNode> childExps = ((NonLeafExpressionNode) node).getChildExps();
      for (ExpressionNode child : childExps) {
        getLabelOrdinals(child, labelOrdinals, auths, checkAuths, ordinalProvider);
      }
    }
  }

  /**
   * This will sort the passed labels in ascending oder and then will write one after the other to
   * the passed stream.
   * @param labelOrdinals
   *          Unsorted label ordinals
   * @param dos
   *          Stream where to write the labels.
   * @throws IOException
   *           When IOE during writes to Stream.
   */
  private static void writeLabelOrdinalsToStream(List<Integer> labelOrdinals, DataOutputStream dos)
      throws IOException {
    Collections.sort(labelOrdinals);
    for (Integer labelOrdinal : labelOrdinals) {
      StreamUtils.writeRawVInt32(dos, labelOrdinal);
    }
  }

  private static void checkAuths(Set<Integer> auths, int labelOrdinal, String identifier,
      boolean checkAuths) throws IOException {
    if (checkAuths) {
      if (auths == null || (!auths.contains(labelOrdinal))) {
        throw new AccessDeniedException("Visibility label " + identifier
            + " not authorized for the user " + VisibilityUtils.getActiveUser().getShortName());
      }
    }
  }
}
