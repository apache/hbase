/**
 * Copyright 2013 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Data object that helps map assignment plans to json format.
 * Example json file:
 *   {
 *     "assignments" : [
 *       {"regionname" : "regionX", "favored" : ["host1:port1", "host2:port2", "host3:port3"]},
 *       {"regionname" : "regionY", "favored" : ["host4:port4", "host5:port5", "host6:port6"]}
 *     ]
 *   }
 */
public class AssignmentPlanData {
  protected static final Log LOG = LogFactory.getLog(AssignmentPlanData.class.getName());

  private List<Assignment> assignments;

  @JsonCreator
  public AssignmentPlanData(@JsonProperty("assignments") List<Assignment> assignments) {
    this.assignments = assignments;
  }

  public List<Assignment> getAssignments() {
      return assignments;
  }

  /**
   * Contains essential parameters for a region placement.
   */
  public static class Assignment {
    private String regionname;

    private List<String> favored;

    @JsonCreator
    public Assignment(@JsonProperty("regionname") String regionname,
                      @JsonProperty("favored") List<String> favored) {
      this.regionname = regionname;
      this.favored = favored;
    }

    public String getRegionname() {
      return regionname;
    }

    public List<String> getFavored() {
      return favored;
    }
  }

  /**
   * Convert assignment plan into data object which is used in json serialization
   * @param plan
   * @return assignment plan for each region as list, wrapped in data object
   */
  public static AssignmentPlanData constructFromAssignmentPlan(AssignmentPlan plan) {
    List<Assignment> assignments = new ArrayList<Assignment>();

    Map<HRegionInfo, List<HServerAddress>> lists = plan.getAssignmentMap();
    for (Map.Entry<HRegionInfo, List<HServerAddress>> entry : lists.entrySet()) {
      String regionname = entry.getKey().getRegionNameAsString();
      List<String> addresses = new ArrayList<String>();
      for (HServerAddress address : entry.getValue()) {
        addresses.add(address.getHostNameWithPort());
      }
      Assignment assignment = new Assignment(regionname, addresses);
      assignments.add(assignment);
    }

    return new AssignmentPlanData(assignments);
  }
}
