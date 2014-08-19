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

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;

public class GetProxy extends RowProxy {
  static final ParseFilter parseFilter = new ParseFilter();

  final private Map<byte [], NavigableSet<byte []>> familyMap =
      new TreeMap<byte [], NavigableSet<byte []>>(Bytes.BYTES_COMPARATOR);
  private Filter filter;
  private int maxVersions = 1;

  public GetProxy(final byte[] row) {
    this.row_ = row;
  }

  public GetProxy addColumn(final byte [] family,
      final byte [] qualifier) {
    NavigableSet<byte[]> set = familyMap.get(family);
    if(set == null) {
      set = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
      familyMap.put(family, set);
    }
    if (qualifier != null) {
      set.add(qualifier);
    }
    return this;
  }

  public GetProxy setFilter(final String filter) {
    try {
      this.filter = parseFilter.parseFilterString(filter);
    } catch (CharacterCodingException e) {
      // TODO Handle error
      e.printStackTrace();
    }
    return this;
  }

  public GetProxy setMaxVersions(final int maxVersions) {
    this.maxVersions = maxVersions;
    return this;
  }

  Filter getFilter() {
    return filter;
  }

  Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
    return familyMap;
  }

  int getMaxVersions() {
    return maxVersions;
  }

  public void send(final HBaseClient client,
      final GetCallbackHandler<Object,ArrayList<KeyValue>> cbh) {
    final GetRequest getReq  = new GetRequest(getTable(), getRow());
    Map<byte[], NavigableSet<byte[]>> familyMap = getFamilyMap();
    if (familyMap.size() != 0) {
      final int numFamilies = familyMap.size();
      final byte[][] families = new byte[numFamilies][];
      final byte[][][] qualifiers = new byte[numFamilies][][];
      int idx = 0;
      for (byte[] family : familyMap.keySet()) {
        families[idx] = family;
        NavigableSet<byte[]> qualifierSet = familyMap.get(family);
        if (qualifierSet.size() == 0) {
          qualifiers[idx] = null;
        } else {
          qualifiers[idx] = new byte[qualifierSet.size()][];
          int i = 0;
          for (byte[] qualifier : qualifierSet) {
            qualifiers[idx][i] = qualifier;
            ++i;
          }
        }
        ++idx;
      }
      getReq.families(families);
      getReq.qualifiers(qualifiers);
    }
    getReq.maxVersions(getMaxVersions());
    client.get(getReq).addBoth(cbh);
  }
}
