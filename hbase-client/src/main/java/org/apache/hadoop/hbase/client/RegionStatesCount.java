/*
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

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class RegionStatesCount {

  private int openRegions;
  private int splitRegions;
  private int closedRegions;
  private int regionsInTransition;
  private int totalRegions;

  private RegionStatesCount() {
  }

  public int getClosedRegions() {
    return closedRegions;
  }

  public int getOpenRegions() {
    return openRegions;
  }

  public int getSplitRegions() {
    return splitRegions;
  }

  public int getRegionsInTransition() {
    return regionsInTransition;
  }

  public int getTotalRegions() {
    return totalRegions;
  }

  private void setClosedRegions(int closedRegions) {
    this.closedRegions = closedRegions;
  }

  private void setOpenRegions(int openRegions) {
    this.openRegions = openRegions;
  }

  private void setSplitRegions(int splitRegions) {
    this.splitRegions = splitRegions;
  }

  private void setRegionsInTransition(int regionsInTransition) {
    this.regionsInTransition = regionsInTransition;
  }

  private void setTotalRegions(int totalRegions) {
    this.totalRegions = totalRegions;
  }

  public static class RegionStatesCountBuilder {
    private int openRegions;
    private int splitRegions;
    private int closedRegions;
    private int regionsInTransition;
    private int totalRegions;

    public RegionStatesCountBuilder setOpenRegions(int openRegions) {
      this.openRegions = openRegions;
      return this;
    }

    public RegionStatesCountBuilder setSplitRegions(int splitRegions) {
      this.splitRegions = splitRegions;
      return this;
    }

    public RegionStatesCountBuilder setClosedRegions(int closedRegions) {
      this.closedRegions = closedRegions;
      return this;
    }

    public RegionStatesCountBuilder setRegionsInTransition(int regionsInTransition) {
      this.regionsInTransition = regionsInTransition;
      return this;
    }

    public RegionStatesCountBuilder setTotalRegions(int totalRegions) {
      this.totalRegions = totalRegions;
      return this;
    }

    public RegionStatesCount build() {
      RegionStatesCount regionStatesCount=new RegionStatesCount();
      regionStatesCount.setOpenRegions(openRegions);
      regionStatesCount.setClosedRegions(closedRegions);
      regionStatesCount.setRegionsInTransition(regionsInTransition);
      regionStatesCount.setSplitRegions(splitRegions);
      regionStatesCount.setTotalRegions(totalRegions);
      return regionStatesCount;
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("RegionStatesCount{");
    sb.append("openRegions=").append(openRegions);
    sb.append(", splitRegions=").append(splitRegions);
    sb.append(", closedRegions=").append(closedRegions);
    sb.append(", regionsInTransition=").append(regionsInTransition);
    sb.append(", totalRegions=").append(totalRegions);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RegionStatesCount that = (RegionStatesCount) o;

    if (openRegions != that.openRegions) {
      return false;
    }
    if (splitRegions != that.splitRegions) {
      return false;
    }
    if (closedRegions != that.closedRegions) {
      return false;
    }
    if (regionsInTransition != that.regionsInTransition) {
      return false;
    }
    return totalRegions == that.totalRegions;
  }

  @Override
  public int hashCode() {
    int result = openRegions;
    result = 31 * result + splitRegions;
    result = 31 * result + closedRegions;
    result = 31 * result + regionsInTransition;
    result = 31 * result + totalRegions;
    return result;
  }

}
