/**
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase;

import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionLoad;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.Coprocessor;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This class is used for exporting current state of load on a RegionServer.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ServerLoad {
  public ServerLoad(HBaseProtos.ServerLoad serverLoad) {
    this.serverLoad = serverLoad;
  }

  /* @return the underlying ServerLoad protobuf object */
  public HBaseProtos.ServerLoad getServerLoadPB() {
    return serverLoad;
  }

  protected HBaseProtos.ServerLoad serverLoad;

  /* @return number of requests per second since last report. */
  public int getRequestsPerSecond() {
    return serverLoad.getRequestsPerSecond();
  }
  public boolean hasRequestsPerSecond() {
    return serverLoad.hasRequestsPerSecond();
  }

  /* @return total Number of requests from the start of the region server. */
  public int getTotalNumberOfRequests() {
    return serverLoad.getTotalNumberOfRequests();
  }
  public boolean hasTotalNumberOfRequests() {
    return serverLoad.hasTotalNumberOfRequests();
  }

  /* @return the amount of used heap, in MB. */
  public int getUsedHeapMB() {
    return serverLoad.getUsedHeapMB();
  }
  public boolean hasUsedHeapMB() {
    return serverLoad.hasUsedHeapMB();
  }

  /* @return the maximum allowable size of the heap, in MB. */
  public int getMaxHeapMB() {
    return serverLoad.getMaxHeapMB();
  }
  public boolean hasMaxHeapMB() {
    return serverLoad.hasMaxHeapMB();
  }

  /* Returns list of RegionLoads, which contain information on the load of individual regions. */
  public List<RegionLoad> getRegionLoadsList() {
    return serverLoad.getRegionLoadsList();
  }
  public RegionLoad getRegionLoads(int index) {
    return serverLoad.getRegionLoads(index);
  }
  public int getRegionLoadsCount() {
    return serverLoad.getRegionLoadsCount();
  }

  /**
   * @return the list Regionserver-level coprocessors, e.g., WALObserver implementations.
   * Region-level coprocessors, on the other hand, are stored inside the RegionLoad objects.
   */
  public List<Coprocessor> getCoprocessorsList() {
    return serverLoad.getCoprocessorsList();
  }
  public Coprocessor getCoprocessors(int index) {
    return serverLoad.getCoprocessors(index);
  }
  public int getCoprocessorsCount() {
    return serverLoad.getCoprocessorsCount();
  }

  /**
   * Return the RegionServer-level coprocessors from a ServerLoad pb.
   * @param sl - ServerLoad
   * @return string array of loaded RegionServer-level coprocessors
   */
  public static String[] getRegionServerCoprocessors(ServerLoad sl) {
    if (sl == null) {
      return null;
    }

    List<Coprocessor> list = sl.getCoprocessorsList();
    String [] ret = new String[list.size()];
    int i = 0;
    for (Coprocessor elem : list) {
      ret[i++] = elem.getName();
    }

    return ret;
  }

  /**
   * Return the RegionServer-level and Region-level coprocessors
   * from a ServerLoad pb.
   * @param sl - ServerLoad
   * @return string array of loaded RegionServer-level and
   *         Region-level coprocessors
   */
  public static String[] getAllCoprocessors(ServerLoad sl) {
    if (sl == null) {
      return null;
    }

    // Need a set to remove duplicates, but since generated Coprocessor class
    // is not Comparable, make it a Set<String> instead of Set<Coprocessor>
    TreeSet<String> coprocessSet = new TreeSet<String>();
    for (Coprocessor coprocessor : sl.getCoprocessorsList()) {
      coprocessSet.add(coprocessor.getName());
    }
    for (RegionLoad rl : sl.getRegionLoadsList()) {
      for (Coprocessor coprocessor : rl.getCoprocessorsList()) {
        coprocessSet.add(coprocessor.getName());
      }
    }

    return coprocessSet.toArray(new String[0]);
  }

  public static final ServerLoad EMPTY_SERVERLOAD =
    new ServerLoad(HBaseProtos.ServerLoad.newBuilder().build());
}
