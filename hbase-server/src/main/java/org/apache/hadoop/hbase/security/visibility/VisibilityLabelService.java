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
package org.apache.hadoop.hbase.security.visibility;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.security.User;

/**
 * The interface which deals with visibility labels and user auths admin service as well as the cell
 * visibility expression storage part and read time evaluation.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface VisibilityLabelService extends Configurable {

  /**
   * System calls this after opening of regions. Gives a chance for the VisibilityLabelService to so
   * any initialization logic.
   * @param e
   *          the region coprocessor env
   */
  void init(RegionCoprocessorEnvironment e) throws IOException;

  /**
   * Adds the set of labels into the system.
   * @param labels
   *          Labels to add to the system.
   * @return OperationStatus for each of the label addition
   */
  OperationStatus[] addLabels(List<byte[]> labels) throws IOException;

  /**
   * Sets given labels globally authorized for the user.
   * @param user
   *          The authorizing user
   * @param authLabels
   *          Labels which are getting authorized for the user
   * @return OperationStatus for each of the label auth addition
   */
  OperationStatus[] setAuths(byte[] user, List<byte[]> authLabels) throws IOException;

  /**
   * Removes given labels from user's globally authorized list of labels.
   * @param user
   *          The user whose authorization to be removed
   * @param authLabels
   *          Labels which are getting removed from authorization set
   * @return OperationStatus for each of the label auth removal
   */
  OperationStatus[] clearAuths(byte[] user, List<byte[]> authLabels) throws IOException;

  /**
   * Retrieve the visibility labels for the user.
   * @param user
   *          Name of the user whose authorization to be retrieved
   * @param systemCall
   *          Whether a system or user originated call.
   * @return Visibility labels authorized for the given user.
   */
  List<String> getUserAuths(byte[] user, boolean systemCall) throws IOException;

  /**
   * Retrieve the visibility labels for the groups.
   * @param groups
   *          Name of the groups whose authorization to be retrieved
   * @param systemCall
   *          Whether a system or user originated call.
   * @return Visibility labels authorized for the given group.
   */
  List<String> getGroupAuths(String[] groups, boolean systemCall) throws IOException;

  /**
   * Retrieve the list of visibility labels defined in the system.
   * @param regex  The regular expression to filter which labels are returned.
   * @return List of visibility labels
   */
  List<String> listLabels(String regex) throws IOException;

  /**
   * Creates tags corresponding to given visibility expression.
   * <br>
   * Note: This will be concurrently called from multiple threads and implementation should
   * take care of thread safety.
   * @param visExpression The Expression for which corresponding Tags to be created.
   * @param withSerializationFormat specifies whether a tag, denoting the serialization version
   *          of the tags, to be added in the list. When this is true make sure to add the
   *          serialization format Tag also. The format tag value should be byte type.
   * @param checkAuths denotes whether to check individual labels in visExpression against user's
   *          global auth label.
   * @return The list of tags corresponds to the visibility expression. These tags will be stored
   *         along with the Cells.
   */
  List<Tag> createVisibilityExpTags(String visExpression, boolean withSerializationFormat,
      boolean checkAuths) throws IOException;

  /**
   * Creates VisibilityExpEvaluator corresponding to given Authorizations. <br>
   * Note: This will be concurrently called from multiple threads and implementation should take
   * care of thread safety.
   * @param authorizations
   *          Authorizations for the read request
   * @return The VisibilityExpEvaluator corresponding to the given set of authorization labels.
   */
  VisibilityExpEvaluator getVisibilityExpEvaluator(Authorizations authorizations)
      throws IOException;

  /**
   * System checks for user auth during admin operations. (ie. Label add, set/clear auth). The
   * operation is allowed only for users having system auth. Also during read, if the requesting
   * user has system auth, he can view all the data irrespective of its labels.
   * @param user
   *          User for whom system auth check to be done.
   * @return true if the given user is having system/super auth
   */
  boolean havingSystemAuth(User user) throws IOException;

  /**
   * System uses this for deciding whether a Cell can be deleted by matching visibility expression
   * in Delete mutation and the cell in consideration. Also system passes the serialization format
   * of visibility tags in Put and Delete.<br>
   * Note: This will be concurrently called from multiple threads and implementation should take
   * care of thread safety.
   * @param putVisTags
   *          The visibility tags present in the Put mutation
   * @param putVisTagFormat
   *          The serialization format for the Put visibility tags. A <code>null</code> value for
   *          this format means the tags are written with unsorted label ordinals
   * @param deleteVisTags
   *          - The visibility tags in the delete mutation (the specified Cell Visibility)
   * @param deleteVisTagFormat
   *          The serialization format for the Delete visibility tags. A <code>null</code> value for
   *          this format means the tags are written with unsorted label ordinals
   * @return true if matching tags are found
   * @see VisibilityConstants#SORTED_ORDINAL_SERIALIZATION_FORMAT
   */
  boolean matchVisibility(List<Tag> putVisTags, Byte putVisTagFormat, List<Tag> deleteVisTags,
      Byte deleteVisTagFormat) throws IOException;

  /**
   * Provides a way to modify the visibility tags of type {@link TagType}
   * .VISIBILITY_TAG_TYPE, that are part of the cell created from the WALEdits
   * that are prepared for replication while calling
   * {@link org.apache.hadoop.hbase.replication.ReplicationEndpoint}
   * .replicate().
   * {@link org.apache.hadoop.hbase.security.visibility.VisibilityReplicationEndpoint}
   * calls this API to provide an opportunity to modify the visibility tags
   * before replicating.
   *
   * @param visTags
   *          the visibility tags associated with the cell
   * @param serializationFormat
   *          the serialization format associated with the tag
   * @return the modified visibility expression in the form of byte[]
   * @throws IOException
   */
  byte[] encodeVisibilityForReplication(final List<Tag> visTags,
      final Byte serializationFormat) throws IOException;

}
