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
package org.apache.hadoop.hbase.security.visibility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.regionserver.ScanDeleteTracker;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Triple;

/**
 * Similar to ScanDeletTracker but tracks the visibility expression also before
 * deciding if a Cell can be considered deleted
 */
@InterfaceAudience.Private
public class VisibilityScanDeleteTracker extends ScanDeleteTracker {

  private static final Log LOG = LogFactory.getLog(VisibilityScanDeleteTracker.class);

  // Its better to track the visibility tags in delete based on each type.  Create individual
  // data structures for tracking each of them.  This would ensure that there is no tracking based
  // on time and also would handle all cases where deletefamily or deletecolumns is specified with
  // Latest_timestamp.  In such cases the ts in the delete marker and the masking
  // put will not be same. So going with individual data structures for different delete
  // type would solve this problem and also ensure that the combination of different type
  // of deletes with diff ts would also work fine
  // Track per TS
  private List<Triple<List<Tag>, Byte, Long>> visibilityTagsDeleteFamily =
      new ArrayList<Triple<List<Tag>, Byte, Long>>();
  // Delete family version with different ts and different visibility expression could come.
  // Need to track it per ts.
  private List<Triple<List<Tag>, Byte, Long>> visibilityTagsDeleteFamilyVersion =
      new ArrayList<Triple<List<Tag>, Byte, Long>>();
  private List<Pair<List<Tag>, Byte>> visibilityTagsDeleteColumns;
  // Tracking as List<List> is to handle same ts cell but different visibility tag. 
  // TODO : Need to handle puts with same ts but different vis tags.
  private List<Pair<List<Tag>, Byte>> visiblityTagsDeleteColumnVersion =
      new ArrayList<Pair<List<Tag>, Byte>>();

  public VisibilityScanDeleteTracker() {
    super();
  }

  @Override
  public void add(Cell delCell) {
    //Cannot call super.add because need to find if the delete needs to be considered
    long timestamp = delCell.getTimestamp();
    int qualifierOffset = delCell.getQualifierOffset();
    int qualifierLength = delCell.getQualifierLength();
    byte type = delCell.getTypeByte();
    if (type == KeyValue.Type.DeleteFamily.getCode()) {
      hasFamilyStamp = true;
      boolean hasVisTag = extractDeleteCellVisTags(delCell, KeyValue.Type.DeleteFamily);
      if (!hasVisTag && timestamp > familyStamp) {
        familyStamp = timestamp;
      }
      return;
    } else if (type == KeyValue.Type.DeleteFamilyVersion.getCode()) {
      familyVersionStamps.add(timestamp);
      extractDeleteCellVisTags(delCell, KeyValue.Type.DeleteFamilyVersion);
      return;
    }
    // new column, or more general delete type
    if (deleteBuffer != null) {
      if (Bytes.compareTo(deleteBuffer, deleteOffset, deleteLength, delCell.getQualifierArray(),
          qualifierOffset, qualifierLength) != 0) {
        // A case where there are deletes for a column qualifier but there are
        // no corresponding puts for them. Rare case.
        visibilityTagsDeleteColumns = null;
        visiblityTagsDeleteColumnVersion = null;
      } else if (type == KeyValue.Type.Delete.getCode() && (deleteTimestamp != timestamp)) {
        // there is a timestamp change which means we could clear the list
        // when ts is same and the vis tags are different we need to collect
        // them all. Interesting part is that in the normal case of puts if
        // there are 2 cells with same ts and diff vis tags only one of them is
        // returned. Handling with a single List<Tag> would mean that only one
        // of the cell would be considered. Doing this as a precaution.
        // Rare cases.
        visiblityTagsDeleteColumnVersion = null;
      }
    }
    deleteBuffer = delCell.getQualifierArray();
    deleteOffset = qualifierOffset;
    deleteLength = qualifierLength;
    deleteType = type;
    deleteTimestamp = timestamp;
    extractDeleteCellVisTags(delCell, KeyValue.Type.codeToType(type));
  }

  private boolean extractDeleteCellVisTags(Cell delCell, Type type) {
    // If tag is present in the delete
    boolean hasVisTag = false;
    if (delCell.getTagsLengthUnsigned() > 0) {
      switch (type) {
        case DeleteFamily:
          List<Tag> delTags = new ArrayList<Tag>();
          if (visibilityTagsDeleteFamily != null) {
            Byte deleteCellVisTagsFormat = VisibilityUtils.extractVisibilityTags(delCell, delTags);
            if (!delTags.isEmpty()) {
              visibilityTagsDeleteFamily.add(new Triple<List<Tag>, Byte, Long>(
                  delTags, deleteCellVisTagsFormat, delCell.getTimestamp()));
              hasVisTag = true;
            }
          }
          break;
        case DeleteFamilyVersion:
          delTags = new ArrayList<Tag>();
          Byte deleteCellVisTagsFormat = VisibilityUtils.extractVisibilityTags(delCell, delTags);
          if (!delTags.isEmpty()) {
            visibilityTagsDeleteFamilyVersion.add(new Triple<List<Tag>, Byte, Long>(delTags,
                deleteCellVisTagsFormat, delCell.getTimestamp()));
            hasVisTag = true;
          }
          break;
        case DeleteColumn:
          if (visibilityTagsDeleteColumns == null) {
            visibilityTagsDeleteColumns = new ArrayList<Pair<List<Tag>, Byte>>();
          }
          delTags = new ArrayList<Tag>();
          deleteCellVisTagsFormat = VisibilityUtils.extractVisibilityTags(delCell, delTags);
          if (!delTags.isEmpty()) {
            visibilityTagsDeleteColumns.add(new Pair<List<Tag>, Byte>(delTags,
                deleteCellVisTagsFormat));
            hasVisTag = true;
          }
          break;
        case Delete:
          if (visiblityTagsDeleteColumnVersion == null) {
            visiblityTagsDeleteColumnVersion = new ArrayList<Pair<List<Tag>, Byte>>();
          }
          delTags = new ArrayList<Tag>();
          deleteCellVisTagsFormat = VisibilityUtils.extractVisibilityTags(delCell, delTags);
          if (!delTags.isEmpty()) {
            visiblityTagsDeleteColumnVersion.add(new Pair<List<Tag>, Byte>(delTags,
                deleteCellVisTagsFormat));
            hasVisTag = true;
          }
          break;
        default:
          throw new IllegalArgumentException("Invalid delete type");
      }
    } else {
      switch (type) {
        case DeleteFamily:
          visibilityTagsDeleteFamily = null;
          break;
        case DeleteFamilyVersion:
          visibilityTagsDeleteFamilyVersion = null;
          break;
        case DeleteColumn:
          visibilityTagsDeleteColumns = null;
          break;
        case Delete:
          visiblityTagsDeleteColumnVersion = null;
          break;
        default:
          throw new IllegalArgumentException("Invalid delete type");
      }
    }
    return hasVisTag;
  }

  @Override
  public DeleteResult isDeleted(Cell cell) {
    long timestamp = cell.getTimestamp();
    int qualifierOffset = cell.getQualifierOffset();
    int qualifierLength = cell.getQualifierLength();
    try {
      if (hasFamilyStamp) {
        if (visibilityTagsDeleteFamily != null) {
          for (int i = 0; i < visibilityTagsDeleteFamily.size(); i++) {
            // visibilityTagsDeleteFamily is ArrayList
            Triple<List<Tag>, Byte, Long> triple = visibilityTagsDeleteFamily.get(i);
            if (timestamp <= triple.getThird()) {
              List<Tag> putVisTags = new ArrayList<Tag>();
              Byte putCellVisTagsFormat = VisibilityUtils.extractVisibilityTags(cell, putVisTags);
              boolean matchFound = VisibilityLabelServiceManager
                  .getInstance().getVisibilityLabelService()
                  .matchVisibility(putVisTags, putCellVisTagsFormat, triple.getFirst(),
                      triple.getSecond());
              if (matchFound) {
                // A return type of FAMILY_DELETED will cause skip for all remaining cells from this
                // family. We would like to match visibility expression on every put cells after
                // this and only remove those matching with the family delete visibility. So we are
                // returning FAMILY_VERSION_DELETED from here.
                return DeleteResult.FAMILY_VERSION_DELETED;
              }
            }
          }
        } else {
          if (!VisibilityUtils.isVisibilityTagsPresent(cell) && timestamp<=familyStamp) {
            // No tags
            return DeleteResult.FAMILY_VERSION_DELETED;
          }
        }
      }
      if (familyVersionStamps.contains(Long.valueOf(timestamp))) {
        if (visibilityTagsDeleteFamilyVersion != null) {
          for (int i = 0; i < visibilityTagsDeleteFamilyVersion.size(); i++) {
            // visibilityTagsDeleteFamilyVersion is ArrayList
            Triple<List<Tag>, Byte, Long> triple = visibilityTagsDeleteFamilyVersion.get(i);
            if (timestamp == triple.getThird()) {
              List<Tag> putVisTags = new ArrayList<Tag>();
              Byte putCellVisTagsFormat = VisibilityUtils.extractVisibilityTags(cell, putVisTags);
              boolean matchFound = VisibilityLabelServiceManager
                  .getInstance()
                  .getVisibilityLabelService()
                  .matchVisibility(putVisTags, putCellVisTagsFormat, triple.getFirst(),
                      triple.getSecond());
              if (matchFound) {
                return DeleteResult.FAMILY_VERSION_DELETED;
              }
            }
          }
        } else {
          if (!VisibilityUtils.isVisibilityTagsPresent(cell)) {
            // No tags
            return DeleteResult.FAMILY_VERSION_DELETED;
          }
        }
      }
      if (deleteBuffer != null) {
        int ret = Bytes.compareTo(deleteBuffer, deleteOffset, deleteLength,
            cell.getQualifierArray(), qualifierOffset, qualifierLength);

        if (ret == 0) {
          if (deleteType == KeyValue.Type.DeleteColumn.getCode()) {
            if (visibilityTagsDeleteColumns != null) {
              for (Pair<List<Tag>, Byte> tags : visibilityTagsDeleteColumns) {
                List<Tag> putVisTags = new ArrayList<Tag>();
                Byte putCellVisTagsFormat = VisibilityUtils.extractVisibilityTags(cell, putVisTags);
                boolean matchFound = VisibilityLabelServiceManager
                    .getInstance()
                    .getVisibilityLabelService()
                    .matchVisibility(putVisTags, putCellVisTagsFormat, tags.getFirst(),
                        tags.getSecond());
                if (matchFound) {
                  return DeleteResult.VERSION_DELETED;
                }
              }
            } else {
              if (!VisibilityUtils.isVisibilityTagsPresent(cell)) {
                // No tags
                return DeleteResult.VERSION_DELETED;
              }
            }
          }
          // Delete (aka DeleteVersion)
          // If the timestamp is the same, keep this one
          if (timestamp == deleteTimestamp) {
            if (visiblityTagsDeleteColumnVersion != null) {
              for (Pair<List<Tag>, Byte> tags : visiblityTagsDeleteColumnVersion) {
                List<Tag> putVisTags = new ArrayList<Tag>();
                Byte putCellVisTagsFormat = VisibilityUtils.extractVisibilityTags(cell, putVisTags);
                boolean matchFound = VisibilityLabelServiceManager
                    .getInstance()
                    .getVisibilityLabelService()
                    .matchVisibility(putVisTags, putCellVisTagsFormat, tags.getFirst(),
                        tags.getSecond());
                if (matchFound) {
                  return DeleteResult.VERSION_DELETED;
                }
              }
            } else {
              if (!VisibilityUtils.isVisibilityTagsPresent(cell)) {
                // No tags
                return DeleteResult.VERSION_DELETED;
              }
            }
          }
        } else if (ret < 0) {
          // Next column case.
          deleteBuffer = null;
          visibilityTagsDeleteColumns = null;
          visiblityTagsDeleteColumnVersion = null;
        } else {
          throw new IllegalStateException("isDeleted failed: deleteBuffer="
              + Bytes.toStringBinary(deleteBuffer, deleteOffset, deleteLength) + ", qualifier="
              + Bytes.toStringBinary(cell.getQualifierArray(), qualifierOffset, qualifierLength)
              + ", timestamp=" + timestamp + ", comparison result: " + ret);
        }
      }
    } catch (IOException e) {
      LOG.error("Error in isDeleted() check! Will treat cell as not deleted", e);
    }
    return DeleteResult.NOT_DELETED;
  }

  @Override
  public void reset() {
    super.reset();
    visibilityTagsDeleteColumns = null;
    visibilityTagsDeleteFamily = new ArrayList<Triple<List<Tag>, Byte, Long>>();
    visibilityTagsDeleteFamilyVersion = new ArrayList<Triple<List<Tag>, Byte, Long>>();
    visiblityTagsDeleteColumnVersion = null;
  }
}
