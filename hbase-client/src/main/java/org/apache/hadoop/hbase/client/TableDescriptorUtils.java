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
package org.apache.hadoop.hbase.client;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

@InterfaceAudience.Public
public final class TableDescriptorUtils {
  public final static class TableDescriptorDelta {
    private final Set<byte[]> columnsAdded;
    private final Set<byte[]> columnsDeleted;
    private final Set<byte[]> columnsModified;

    private TableDescriptorDelta(TableDescriptor oldTD, TableDescriptor newTD) {
      Preconditions.checkNotNull(oldTD);
      Preconditions.checkNotNull(newTD);

      Map<byte[], ColumnFamilyDescriptor> oldCFs = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      Set<byte[]> newCFs = new TreeSet<>(Bytes.BYTES_COMPARATOR);

      // CFD -> (name, CFD)
      for (ColumnFamilyDescriptor cfd : oldTD.getColumnFamilies()) {
        oldCFs.put(cfd.getName(), cfd);
      }

      Set<byte[]> added = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      Set<byte[]> modified = new TreeSet<>(Bytes.BYTES_COMPARATOR);

      for (ColumnFamilyDescriptor cfd : newTD.getColumnFamilies()) {
        byte[] cfName = cfd.getName();
        newCFs.add(cfName);

        if (!oldCFs.containsKey(cfName)) {
          // If column family is in newTD but not oldTD, then it was added
          added.add(cfName);
        } else if (!cfd.equals(oldCFs.get(cfName))) {
          // If column family is in both, but not equal, then it was modified
          modified.add(cfName);
        }
      }

      // If column family is in oldTD, but not in newTD, then it got deleted.
      Set<byte[]> deleted = oldCFs.keySet();
      deleted.removeAll(newCFs);

      columnsAdded = Collections.unmodifiableSet(added);
      columnsDeleted = Collections.unmodifiableSet(deleted);
      columnsModified = Collections.unmodifiableSet(modified);
    }

    public Set<byte[]> getColumnsAdded() {
      return columnsAdded;
    }

    public Set<byte[]> getColumnsDeleted() {
      return columnsDeleted;
    }

    public Set<byte[]> getColumnsModified() {
      return columnsModified;
    }
  }

  private TableDescriptorUtils() { }

  /**
   * Compares two {@link TableDescriptor} and indicate which columns were added, deleted,
   * or modified from oldTD to newTD
   * @return a TableDescriptorDelta that contains the added/deleted/modified column names
   */
  public static TableDescriptorDelta computeDelta(TableDescriptor oldTD, TableDescriptor newTD) {
    return new TableDescriptorDelta(oldTD, newTD);
  }
}
