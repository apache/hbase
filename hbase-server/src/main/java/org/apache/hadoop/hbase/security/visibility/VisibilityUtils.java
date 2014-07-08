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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.MultiUserAuthorizations;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.UserAuthorizations;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabel;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsRequest;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Utility method to support visibility
 */
@InterfaceAudience.Private
public class VisibilityUtils {

  public static final String VISIBILITY_LABEL_GENERATOR_CLASS =
      "hbase.regionserver.scan.visibility.label.generator.class";
  public static final byte VISIBILITY_TAG_TYPE = TagType.VISIBILITY_TAG_TYPE;
  public static final byte VISIBILITY_EXP_SERIALIZATION_TAG_TYPE =
      TagType.VISIBILITY_EXP_SERIALIZATION_TAG_TYPE;
  public static final String SYSTEM_LABEL = "system";
  public static final Tag VIS_SERIALIZATION_TAG = new Tag(VISIBILITY_EXP_SERIALIZATION_TAG_TYPE,
      VisibilityConstants.SORTED_ORDINAL_SERIALIZATION_FORMAT);
  private static final String COMMA = ",";

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
   * writeToZooKeeper(Map<byte[], Integer> entries).
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
        VisibilityLabelsRequest request = VisibilityLabelsRequest.newBuilder()
            .mergeFrom(data, pblen, data.length - pblen).build();
        return request.getVisLabelList();
      } catch (InvalidProtocolBufferException e) {
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
        MultiUserAuthorizations multiUserAuths = MultiUserAuthorizations.newBuilder()
            .mergeFrom(data, pblen, data.length - pblen).build();
        return multiUserAuths;
      } catch (InvalidProtocolBufferException e) {
        throw new DeserializationException(e);
      }
    }
    return null;
  }

  public static List<ScanLabelGenerator> getScanLabelGenerators(Configuration conf)
      throws IOException {
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
          throw new IOException(e);
        }
      }
    }
    // If the conf is not configured by default we need to have one SLG to be used
    // ie. DefaultScanLabelGenerator
    if (slgs.isEmpty()) {
      slgs.add(ReflectionUtils.newInstance(DefaultScanLabelGenerator.class, conf));
    }
    return slgs;
  }

  /**
   * Get the list of visibility tags in the given cell
   * @param cell - the cell
   * @param tags - the tags array that will be populated if
   * visibility tags are present
   * @return true if the tags are in sorted order.
   */
  public static boolean getVisibilityTags(Cell cell, List<Tag> tags) {
    boolean sortedOrder = false;
    Iterator<Tag> tagsIterator = CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(),
        cell.getTagsLength());
    while (tagsIterator.hasNext()) {
      Tag tag = tagsIterator.next();
      if(tag.getType() == VisibilityUtils.VISIBILITY_EXP_SERIALIZATION_TAG_TYPE) {
        int serializationVersion = Bytes.toShort(tag.getValue());
        if (serializationVersion == VisibilityConstants.VISIBILITY_SERIALIZATION_VERSION) {
          sortedOrder = true;
          continue;
        }
      }
      if (tag.getType() == VisibilityUtils.VISIBILITY_TAG_TYPE) {
        tags.add(tag);
      }
    }
    return sortedOrder;
  }

  /**
   * Checks if the cell has a visibility tag
   * @param cell
   * @return true if found, false if not found
   */
  public static boolean isVisibilityTagsPresent(Cell cell) {
    Iterator<Tag> tagsIterator = CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(),
        cell.getTagsLength());
    while (tagsIterator.hasNext()) {
      Tag tag = tagsIterator.next();
      if (tag.getType() == VisibilityUtils.VISIBILITY_TAG_TYPE) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks for the matching visibility labels in the delete mutation and
   * the cell in consideration
   * @param cell - the cell
   * @param visibilityTagsInDeleteCell - that list of tags in the delete mutation
   * (the specified Cell Visibility)
   * @return true if matching tags are found
   */
  public static boolean checkForMatchingVisibilityTags(Cell cell,
      List<Tag> visibilityTagsInDeleteCell) {
    List<Tag> tags = new ArrayList<Tag>();
    boolean sortedTags = getVisibilityTags(cell, tags);
    if (tags.size() == 0) {
      // Early out if there are no tags in the cell
      return false;
    }
    if (sortedTags) {
      return checkForMatchingVisibilityTagsWithSortedOrder(visibilityTagsInDeleteCell, tags);
    } else {
      try {
        return checkForMatchingVisibilityTagsWithOutSortedOrder(cell, visibilityTagsInDeleteCell);
      } catch (IOException e) {
        // Should not happen
        throw new RuntimeException("Exception while sorting the tags from the cell", e);
      }
    }
  }

  private static boolean checkForMatchingVisibilityTagsWithOutSortedOrder(Cell cell,
      List<Tag> visibilityTagsInDeleteCell) throws IOException {
    List<List<Integer>> sortedDeleteTags = sortTagsBasedOnOrdinal(
        visibilityTagsInDeleteCell);
    List<List<Integer>> sortedTags = sortTagsBasedOnOrdinal(cell);
    return compareTagsOrdinals(sortedDeleteTags, sortedTags);
  }

  private static boolean checkForMatchingVisibilityTagsWithSortedOrder(
      List<Tag> visibilityTagsInDeleteCell, List<Tag> tags) {
    boolean matchFound = false;
    if ((visibilityTagsInDeleteCell.size()) != tags.size()) {
      // If the size does not match. Definitely we are not comparing the
      // equal tags.
      // Return false in that case.
      return matchFound;
    }
    for (Tag tag : visibilityTagsInDeleteCell) {
      matchFound = false;
      for (Tag givenTag : tags) {
        if (Bytes.equals(tag.getBuffer(), tag.getTagOffset(), tag.getTagLength(),
            givenTag.getBuffer(), givenTag.getTagOffset(), givenTag.getTagLength())) {
          matchFound = true;
          break;
        }
      }
    }
    return matchFound;
  }

  private static List<List<Integer>> sortTagsBasedOnOrdinal(Cell cell) throws IOException {
    Iterator<Tag> tagsItr = CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(),
        cell.getTagsLength());
    List<List<Integer>> fullTagsList = new ArrayList<List<Integer>>();
    while (tagsItr.hasNext()) {
      Tag tag = tagsItr.next();
      if (tag.getType() == VisibilityUtils.VISIBILITY_TAG_TYPE) {
        getSortedTagOrdinals(fullTagsList, tag);
      }
    }
    return fullTagsList;
  }

  private static List<List<Integer>> sortTagsBasedOnOrdinal(List<Tag> tags) throws IOException {
    List<List<Integer>> fullTagsList = new ArrayList<List<Integer>>();
    for (Tag tag : tags) {
      if (tag.getType() == VisibilityUtils.VISIBILITY_TAG_TYPE) {
        getSortedTagOrdinals(fullTagsList, tag);
      }
    }
    return fullTagsList;
  }

  private static void getSortedTagOrdinals(List<List<Integer>> fullTagsList, Tag tag)
      throws IOException {
    List<Integer> tagsOrdinalInSortedOrder = new ArrayList<Integer>();
    int offset = tag.getTagOffset();
    int endOffset = offset + tag.getTagLength();
    while (offset < endOffset) {
      Pair<Integer, Integer> result = StreamUtils.readRawVarint32(tag.getBuffer(), offset);
      tagsOrdinalInSortedOrder.add(result.getFirst());
      offset += result.getSecond();
    }
    Collections.sort(tagsOrdinalInSortedOrder);
    fullTagsList.add(tagsOrdinalInSortedOrder);
  }

  private static boolean compareTagsOrdinals(List<List<Integer>> tagsInDeletes,
      List<List<Integer>> tags) {
    boolean matchFound = false;
    if (tagsInDeletes.size() != tags.size()) {
      return matchFound;
    } else {
      for (List<Integer> deleteTagOrdinals : tagsInDeletes) {
        matchFound = false;
        for (List<Integer> tagOrdinals : tags) {
          if (deleteTagOrdinals.equals(tagOrdinals)) {
            matchFound = true;
            break;
          }
        }
      }
      return matchFound;
    }
  }
}
