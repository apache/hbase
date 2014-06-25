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
 *
 */
package org.apache.hadoop.hbase.jni;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;

public class ResultProxy {

  protected ArrayList<KeyValue> kvList;
  private byte[] namespace;
  private byte[] table;

  public static final Comparator<KeyValue> KV_COMPARATOR =
      new Comparator<KeyValue>() {
    @Override
    public int compare(KeyValue o1, KeyValue o2) {
      int d;
      if ((d = Bytes.memcmp(o1.key(), o2.key())) != 0) {
        return d;
      } else if ((d = Bytes.memcmp(o1.family(), o2.family())) != 0) {
        return d;
      } else if ((d = Bytes.memcmp(o1.qualifier(), o2.qualifier())) != 0) {
        return d;
      } else if ((d = Long.signum(o2.timestamp() - o1.timestamp())) != 0) {
        return d;
      } else {
        d = Bytes.memcmp(o1.value(), o2.value());
      }
      return d;
    }
  };

  public ResultProxy(byte[] table, byte[] namespace, ArrayList<KeyValue> kvList) {
    this.table = table;
    this.namespace = namespace;
    this.kvList = kvList;
  }

  byte[] getRowKey() {
    return kvList == null || kvList.size() == 0
        ? null : kvList.get(0).key();
  }

  byte[] getFamily(int cellIndex) {
    if (kvList == null || kvList.size() <= cellIndex) {
      throw new IndexOutOfBoundsException();
    }
    return kvList.get(cellIndex).family();
  }

  byte[] getQualifier(int cellIndex) {
    if (kvList == null || kvList.size() <= cellIndex) {
      throw new IndexOutOfBoundsException();
    }
    return kvList.get(cellIndex).qualifier();
  }

  byte[] getValue(int cellIndex) {
    if (kvList == null || kvList.size() <= cellIndex) {
      throw new IndexOutOfBoundsException();
    }
    return kvList.get(cellIndex).value();
  }

  long getTS(int cellIndex) {
    if (kvList == null || kvList.size() <= cellIndex) {
      throw new IndexOutOfBoundsException();
    }
    return kvList.get(cellIndex).timestamp();
  }

  int getCellCount() {
    return kvList != null ? kvList.size() : 0;
  }

  byte[] getNamespace() {
    return namespace;
  }

  int getNamespaceLength() {
    return namespace == null ? 0 : namespace.length;
  }

  byte[] getTable() {
    return table;
  }

  int getTableLength() {
    return table == null ? 0 : table.length;
  }

  ArrayList<KeyValue> getKvList() {
    return kvList;
  }

  int indexOf(final byte [] family, final byte [] qualifier) {
    KeyValue searchTerm = new KeyValue(
        getRowKey(), family, qualifier, HBaseClient.EMPTY_ARRAY);
    int pos = Collections.binarySearch(kvList, searchTerm, KV_COMPARATOR);
    // never will exact match
    if (pos < 0) {
      pos = (pos+1) * -1;
      // pos is now insertion point
    }
    if (pos == kvList.size()) {
      return -1; // doesn't exist
    }

    KeyValue kv = kvList.get(pos);
    return (Bytes.equals(family, kv.family())
              && Bytes.equals(qualifier, kv.qualifier()))
        ? pos : -1;
  }
}
