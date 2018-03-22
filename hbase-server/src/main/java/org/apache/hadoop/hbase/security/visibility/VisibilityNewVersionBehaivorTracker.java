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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.regionserver.querymatcher.NewVersionBehaviorTracker;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Similar to MvccSensitiveTracker but tracks the visibility expression also before
 * deciding if a Cell can be considered deleted
 */
@InterfaceAudience.Private
public class VisibilityNewVersionBehaivorTracker extends NewVersionBehaviorTracker {
  private static final Logger LOG =
      LoggerFactory.getLogger(VisibilityNewVersionBehaivorTracker.class);

  public VisibilityNewVersionBehaivorTracker(NavigableSet<byte[]> columns,
      CellComparator cellComparator, int minVersion, int maxVersion, int resultMaxVersions,
      long oldestUnexpiredTS) {
    super(columns, cellComparator, minVersion, maxVersion, resultMaxVersions, oldestUnexpiredTS);
  }

  private static class TagInfo {
    List<Tag> tags;
    Byte format;

    private TagInfo(Cell c) {
      tags = new ArrayList<>();
      format = VisibilityUtils.extractVisibilityTags(c, tags);
    }

    private TagInfo() {
      tags = new ArrayList<>();
    }
  }

  private class VisibilityDeleteVersionsNode extends DeleteVersionsNode {
    private TagInfo tagInfo;

    // <timestamp, set<mvcc>>
    // Key is ts of version deletes, value is its mvccs.
    // We may delete more than one time for a version.
    private Map<Long, SortedMap<Long, TagInfo>> deletesMap = new HashMap<>();

    // <mvcc, set<mvcc>>
    // Key is mvcc of version deletes, value is mvcc of visible puts before the delete effect.
    private NavigableMap<Long, SortedSet<Long>> mvccCountingMap = new TreeMap<>();

    protected VisibilityDeleteVersionsNode(long ts, long mvcc, TagInfo tagInfo) {
      this.tagInfo = tagInfo;
      this.ts = ts;
      this.mvcc = mvcc;
      mvccCountingMap.put(Long.MAX_VALUE, new TreeSet<Long>());
    }

    @Override
    protected VisibilityDeleteVersionsNode getDeepCopy() {
      VisibilityDeleteVersionsNode node = new VisibilityDeleteVersionsNode(ts, mvcc, tagInfo);
      for (Map.Entry<Long, SortedMap<Long, TagInfo>> e : deletesMap.entrySet()) {
        node.deletesMap.put(e.getKey(), new TreeMap<>(e.getValue()));
      }
      for (Map.Entry<Long, SortedSet<Long>> e : mvccCountingMap.entrySet()) {
        node.mvccCountingMap.put(e.getKey(), new TreeSet<>(e.getValue()));
      }
      return node;
    }

    @Override
    public void addVersionDelete(Cell cell) {
      SortedMap<Long, TagInfo> set = deletesMap.get(cell.getTimestamp());
      if (set == null) {
        set = new TreeMap<>();
        deletesMap.put(cell.getTimestamp(), set);
      }
      set.put(cell.getSequenceId(), new TagInfo(cell));
      // The init set should be the puts whose mvcc is smaller than this Delete. Because
      // there may be some Puts masked by them. The Puts whose mvcc is larger than this Delete can
      // not be copied to this node because we may delete one version and the oldest put may not be
      // masked.
      SortedSet<Long> nextValue = mvccCountingMap.ceilingEntry(cell.getSequenceId()).getValue();
      SortedSet<Long> thisValue = new TreeSet<>(nextValue.headSet(cell.getSequenceId()));
      mvccCountingMap.put(cell.getSequenceId(), thisValue);
    }

  }

  @Override
  public void add(Cell cell) {
    prepare(cell);
    byte type = cell.getTypeByte();
    switch (KeyValue.Type.codeToType(type)) {
    // By the order of seen. We put null cq at first.
    case DeleteFamily: // Delete all versions of all columns of the specified family
      delFamMap.put(cell.getSequenceId(),
          new VisibilityDeleteVersionsNode(cell.getTimestamp(), cell.getSequenceId(),
              new TagInfo(cell)));
      break;
    case DeleteFamilyVersion: // Delete all columns of the specified family and specified version
      delFamMap.ceilingEntry(cell.getSequenceId()).getValue().addVersionDelete(cell);
      break;

    // These two kinds of markers are mix with Puts.
    case DeleteColumn: // Delete all versions of the specified column
      delColMap.put(cell.getSequenceId(),
          new VisibilityDeleteVersionsNode(cell.getTimestamp(), cell.getSequenceId(),
              new TagInfo(cell)));
      break;
    case Delete: // Delete the specified version of the specified column.
      delColMap.ceilingEntry(cell.getSequenceId()).getValue().addVersionDelete(cell);
      break;
    default:
      throw new AssertionError("Unknown delete marker type for " + cell);
    }
  }

  private boolean tagMatched(Cell put, TagInfo delInfo) throws IOException {
    List<Tag> putVisTags = new ArrayList<>();
    Byte putCellVisTagsFormat = VisibilityUtils.extractVisibilityTags(put, putVisTags);
    return putVisTags.isEmpty() == delInfo.tags.isEmpty() && (
        (putVisTags.isEmpty() && delInfo.tags.isEmpty()) || VisibilityLabelServiceManager
            .getInstance().getVisibilityLabelService()
            .matchVisibility(putVisTags, putCellVisTagsFormat, delInfo.tags, delInfo.format));
  }

  @Override
  public DeleteResult isDeleted(Cell cell) {
    try {
      long duplicateMvcc = prepare(cell);

      for (Map.Entry<Long, DeleteVersionsNode> e : delColMap.tailMap(cell.getSequenceId())
          .entrySet()) {
        VisibilityDeleteVersionsNode node = (VisibilityDeleteVersionsNode) e.getValue();
        long deleteMvcc = Long.MAX_VALUE;
        SortedMap<Long, TagInfo> deleteVersionMvccs = node.deletesMap.get(cell.getTimestamp());
        if (deleteVersionMvccs != null) {
          SortedMap<Long, TagInfo> tail = deleteVersionMvccs.tailMap(cell.getSequenceId());
          for (Map.Entry<Long, TagInfo> entry : tail.entrySet()) {
            if (tagMatched(cell, entry.getValue())) {
              deleteMvcc = tail.firstKey();
              break;
            }
          }
        }
        SortedMap<Long, SortedSet<Long>> subMap = node.mvccCountingMap
            .subMap(cell.getSequenceId(), true, Math.min(duplicateMvcc, deleteMvcc), true);
        for (Map.Entry<Long, SortedSet<Long>> seg : subMap.entrySet()) {
          if (seg.getValue().size() >= maxVersions) {
            return DeleteResult.VERSION_MASKED;
          }
          seg.getValue().add(cell.getSequenceId());
        }
        if (deleteMvcc < Long.MAX_VALUE) {
          return DeleteResult.VERSION_DELETED;
        }

        if (cell.getTimestamp() <= node.ts && tagMatched(cell, node.tagInfo)) {
          return DeleteResult.COLUMN_DELETED;
        }
      }
      if (duplicateMvcc < Long.MAX_VALUE) {
        return DeleteResult.VERSION_MASKED;
      }
    } catch (IOException e) {
      LOG.error("Error in isDeleted() check! Will treat cell as not deleted", e);
    }
    return DeleteResult.NOT_DELETED;
  }

  @Override
  protected void resetInternal() {
    delFamMap.put(Long.MAX_VALUE,
        new VisibilityDeleteVersionsNode(Long.MIN_VALUE, Long.MAX_VALUE, new TagInfo()));
  }
}
