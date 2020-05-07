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
 */
package org.apache.hadoop.hbase.master;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Stores the plan for the move of an individual region.
 *
 * Contains info for the region being moved, info for the server the region
 * should be moved from, and info for the server the region should be moved
 * to.
 *
 * The comparable implementation of this class compares only the region
 * information and not the source/dest server info.
 */
@InterfaceAudience.LimitedPrivate("Coprocessors")
@InterfaceStability.Evolving
public class RegionPlan implements Comparable<RegionPlan> {
  private final RegionInfo hri;
  private final ServerName source;
  private ServerName dest;

  public static class RegionPlanComparator implements Comparator<RegionPlan>, Serializable {
    private static final long serialVersionUID = 4213207330485734853L;

    @Override
    public int compare(RegionPlan l, RegionPlan r) {
      return RegionPlan.compareTo(l, r);
    }
  }

  /**
   * Instantiate a plan for a region move, moving the specified region from
   * the specified source server to the specified destination server.
   *
   * Destination server can be instantiated as null and later set
   * with {@link #setDestination(ServerName)}.
   *
   * @param hri region to be moved
   * @param source regionserver region should be moved from
   * @param dest regionserver region should be moved to
   */
  public RegionPlan(final RegionInfo hri, ServerName source, ServerName dest) {
    this.hri = hri;
    this.source = source;
    this.dest = dest;
  }

  /**
   * Set the destination server for the plan for this region.
   */
  public void setDestination(ServerName dest) {
    this.dest = dest;
  }

  /**
   * Get the source server for the plan for this region.
   * @return server info for source
   */
  public ServerName getSource() {
    return source;
  }

  /**
   * Get the destination server for the plan for this region.
   * @return server info for destination
   */
  public ServerName getDestination() {
    return dest;
  }

  /**
   * Get the encoded region name for the region this plan is for.
   * @return Encoded region name
   */
  public String getRegionName() {
    return this.hri.getEncodedName();
  }

  public RegionInfo getRegionInfo() {
    return this.hri;
  }

  /**
   * Compare the region info.
   * @param other region plan you are comparing against
   */
  @Override
  public int compareTo(RegionPlan other) {
    return compareTo(this, other);
  }

  private static int compareTo(RegionPlan left, RegionPlan right) {
    int result = compareServerName(left.source, right.source);
    if (result != 0) {
      return result;
    }
    if (left.hri == null) {
      if (right.hri != null) {
        return -1;
      }
    } else if (right.hri == null) {
      return +1;
    } else {
      result = RegionInfo.COMPARATOR.compare(left.hri, right.hri);
    }
    if (result != 0) {
      return result;
    }
    return compareServerName(left.dest, right.dest);
  }

  private static int compareServerName(ServerName left, ServerName right) {
    if (left == null) {
      return right == null? 0: -1;
    } else if (right == null) {
      return +1;
    }
    return left.compareTo(right);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((dest == null) ? 0 : dest.hashCode());
    result = prime * result + ((hri == null) ? 0 : hri.hashCode());
    result = prime * result + ((source == null) ? 0 : source.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    RegionPlan other = (RegionPlan) obj;
    if (dest == null) {
      if (other.dest != null) {
        return false;
      }
    } else if (!dest.equals(other.dest)) {
      return false;
    }
    if (hri == null) {
      if (other.hri != null) {
        return false;
      }
    } else if (!hri.equals(other.hri)) {
      return false;
    }
    if (source == null) {
      if (other.source != null) {
        return false;
      }
    } else if (!source.equals(other.source)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "hri=" + this.hri.getEncodedName() + ", source=" +
      (this.source == null? "": this.source.toString()) +
      ", destination=" + (this.dest == null? "": this.dest.toString());
  }
}
