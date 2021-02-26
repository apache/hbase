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

package org.apache.hadoop.hbase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Container for holding a list of {@link HRegionLocation}'s that correspond to the
 * same range. The list is indexed by the replicaId. This is an immutable list,
 * however mutation operations are provided which returns a new List via copy-on-write
 * (assuming small number of locations)
 */
@InterfaceAudience.Private
public class RegionLocations implements Iterable<HRegionLocation> {

  private final int numNonNullElements;

  // locations array contains the HRL objects for known region replicas indexes by the replicaId.
  // elements can be null if the region replica is not known at all. A null value indicates
  // that there is a region replica with the index as replicaId, but the location is not known
  // in the cache.
  private final HRegionLocation[] locations; // replicaId -> HRegionLocation.

  /**
   * Constructs the region location list. The locations array should
   * contain all the locations for known replicas for the region, and should be
   * sorted in replicaId ascending order, although it can contain nulls indicating replicaIds
   * that the locations of which are not known.
   * @param locations an array of HRegionLocations for the same region range
   */
  public RegionLocations(HRegionLocation... locations) {
    int numNonNullElements = 0;
    int maxReplicaId = -1;
    int maxReplicaIdIndex = -1;
    int index = 0;
    for (HRegionLocation loc : locations) {
      if (loc != null) {
        if (loc.getRegion().getReplicaId() >= maxReplicaId) {
          maxReplicaId = loc.getRegion().getReplicaId();
          maxReplicaIdIndex = index;
        }
      }
      index++;
    }
    // account for the null elements in the array after maxReplicaIdIndex
    maxReplicaId = maxReplicaId + (locations.length - (maxReplicaIdIndex + 1) );

    if (maxReplicaId + 1 == locations.length) {
      this.locations = locations;
    } else {
      this.locations = new HRegionLocation[maxReplicaId + 1];
      for (HRegionLocation loc : locations) {
        if (loc != null) {
          this.locations[loc.getRegion().getReplicaId()] = loc;
        }
      }
    }
    for (HRegionLocation loc : this.locations) {
      if (loc != null && loc.getServerName() != null){
        numNonNullElements++;
      }
    }
    this.numNonNullElements = numNonNullElements;
  }

  public RegionLocations(Collection<HRegionLocation> locations) {
    this(locations.toArray(new HRegionLocation[locations.size()]));
  }

  /**
   * Returns the size of the list even if some of the elements
   * might be null.
   * @return the size of the list (corresponding to the max replicaId)
   */
  public int size() {
    return locations.length;
  }

  /**
   * Returns the size of not-null locations
   * @return the size of not-null locations
   */
  public int numNonNullElements() {
    return numNonNullElements;
  }

  /**
   * Returns whether there are non-null elements in the list
   * @return whether there are non-null elements in the list
   */
  public boolean isEmpty() {
    return numNonNullElements == 0;
  }

  /**
   * Returns a new RegionLocations with the locations removed (set to null)
   * which have the destination server as given.
   * @param serverName the serverName to remove locations of
   * @return an RegionLocations object with removed locations or the same object
   * if nothing is removed
   */
  public RegionLocations removeByServer(ServerName serverName) {
    HRegionLocation[] newLocations = null;
    for (int i = 0; i < locations.length; i++) {
      // check whether something to remove
      if (locations[i] != null && serverName.equals(locations[i].getServerName())) {
        if (newLocations == null) { //first time
          newLocations = new HRegionLocation[locations.length];
          System.arraycopy(locations, 0, newLocations, 0, i);
        }
        newLocations[i] = null;
      } else if (newLocations != null) {
        newLocations[i] = locations[i];
      }
    }
    return newLocations == null ? this : new RegionLocations(newLocations);
  }

  /**
   * Removes the given location from the list
   * @param location the location to remove
   * @return an RegionLocations object with removed locations or the same object
   * if nothing is removed
   */
  public RegionLocations remove(HRegionLocation location) {
    if (location == null) return this;
    if (location.getRegion() == null) return this;
    int replicaId = location.getRegion().getReplicaId();
    if (replicaId >= locations.length) return this;

    // check whether something to remove. HRL.compareTo() compares ONLY the
    // serverName. We want to compare the HRI's as well.
    if (locations[replicaId] == null
        || RegionInfo.COMPARATOR.compare(location.getRegion(), locations[replicaId].getRegion()) != 0
        || !location.equals(locations[replicaId])) {
      return this;
    }

    HRegionLocation[] newLocations = new HRegionLocation[locations.length];
    System.arraycopy(locations, 0, newLocations, 0, locations.length);
    newLocations[replicaId] = null;

    return new RegionLocations(newLocations);
  }

  /**
   * Removes location of the given replicaId from the list
   * @param replicaId the replicaId of the location to remove
   * @return an RegionLocations object with removed locations or the same object
   * if nothing is removed
   */
  public RegionLocations remove(int replicaId) {
    if (getRegionLocation(replicaId) == null) {
      return this;
    }

    HRegionLocation[] newLocations = new HRegionLocation[locations.length];

    System.arraycopy(locations, 0, newLocations, 0, locations.length);
    if (replicaId < newLocations.length) {
      newLocations[replicaId] = null;
    }

    return new RegionLocations(newLocations);
  }

  /**
   * Set the element to null if its getServerName method returns null. Returns null if all the
   * elements are removed.
   */
  public RegionLocations removeElementsWithNullLocation() {
    HRegionLocation[] newLocations = new HRegionLocation[locations.length];
    boolean hasNonNullElement = false;
    for (int i = 0; i < locations.length; i++) {
      if (locations[i] != null && locations[i].getServerName() != null) {
        hasNonNullElement = true;
        newLocations[i] = locations[i];
      }
    }
    return hasNonNullElement ? new RegionLocations(newLocations) : null;
  }

  /**
   * Merges this RegionLocations list with the given list assuming
   * same range, and keeping the most up to date version of the
   * HRegionLocation entries from either list according to seqNum. If seqNums
   * are equal, the location from the argument (other) is taken.
   * @param other the locations to merge with
   * @return an RegionLocations object with merged locations or the same object
   * if nothing is merged
   */
  public RegionLocations mergeLocations(RegionLocations other) {
    assert other != null;

    HRegionLocation[] newLocations = null;

    // Use the length from other, since it is coming from meta. Otherwise,
    // in case of region replication going down, we might have a leak here.
    int max = other.locations.length;

    RegionInfo regionInfo = null;
    for (int i = 0; i < max; i++) {
      HRegionLocation thisLoc = this.getRegionLocation(i);
      HRegionLocation otherLoc = other.getRegionLocation(i);
      if (regionInfo == null && otherLoc != null && otherLoc.getRegion() != null) {
        // regionInfo is the first non-null HRI from other RegionLocations. We use it to ensure that
        // all replica region infos belong to the same region with same region id.
        regionInfo = otherLoc.getRegion();
      }

      HRegionLocation selectedLoc = selectRegionLocation(thisLoc,
        otherLoc, true, false);

      if (selectedLoc != thisLoc) {
        if (newLocations == null) {
          newLocations = new HRegionLocation[max];
          System.arraycopy(locations, 0, newLocations, 0, i);
        }
      }
      if (newLocations != null) {
        newLocations[i] = selectedLoc;
      }
    }

    // ensure that all replicas share the same start code. Otherwise delete them
    if (newLocations != null && regionInfo != null) {
      for (int i=0; i < newLocations.length; i++) {
        if (newLocations[i] != null) {
          if (!RegionReplicaUtil.isReplicasForSameRegion(regionInfo,
            newLocations[i].getRegion())) {
            newLocations[i] = null;
          }
        }
      }
    }

    return newLocations == null ? this : new RegionLocations(newLocations);
  }

  private HRegionLocation selectRegionLocation(HRegionLocation oldLocation,
      HRegionLocation location, boolean checkForEquals, boolean force) {
    if (location == null) {
      return oldLocation == null ? null : oldLocation;
    }

    if (oldLocation == null) {
      return location;
    }

    if (force
        || isGreaterThan(location.getSeqNum(), oldLocation.getSeqNum(), checkForEquals)) {
      return location;
    }
    return oldLocation;
  }

  /**
   * Updates the location with new only if the new location has a higher
   * seqNum than the old one or force is true.
   * @param location the location to add or update
   * @param checkForEquals whether to update the location if seqNums for the
   * HRegionLocations for the old and new location are the same
   * @param force whether to force update
   * @return an RegionLocations object with updated locations or the same object
   * if nothing is updated
   */
  public RegionLocations updateLocation(HRegionLocation location,
      boolean checkForEquals, boolean force) {
    assert location != null;

    int replicaId = location.getRegion().getReplicaId();

    HRegionLocation oldLoc = getRegionLocation(location.getRegion().getReplicaId());
    HRegionLocation selectedLoc = selectRegionLocation(oldLoc, location,
      checkForEquals, force);

    if (selectedLoc == oldLoc) {
      return this;
    }
    HRegionLocation[] newLocations = new HRegionLocation[Math.max(locations.length, replicaId +1)];
    System.arraycopy(locations, 0, newLocations, 0, locations.length);
    newLocations[replicaId] = location;
    // ensure that all replicas share the same start code. Otherwise delete them
    for (int i=0; i < newLocations.length; i++) {
      if (newLocations[i] != null) {
        if (!RegionReplicaUtil.isReplicasForSameRegion(location.getRegion(),
          newLocations[i].getRegion())) {
          newLocations[i] = null;
        }
      }
    }
    return new RegionLocations(newLocations);
  }

  private boolean isGreaterThan(long a, long b, boolean checkForEquals) {
    return a > b || (checkForEquals && (a == b));
  }

  public HRegionLocation getRegionLocation(int replicaId) {
    if (replicaId >= locations.length) {
      return null;
    }
    return locations[replicaId];
  }

  /**
   * Returns the region location from the list for matching regionName, which can
   * be regionName or encodedRegionName
   * @param regionName regionName or encodedRegionName
   * @return HRegionLocation found or null
   */
  public HRegionLocation getRegionLocationByRegionName(byte[] regionName) {
    for (HRegionLocation loc : locations) {
      if (loc != null) {
        if (Bytes.equals(loc.getRegion().getRegionName(), regionName)
            || Bytes.equals(loc.getRegion().getEncodedNameAsBytes(), regionName)) {
          return loc;
        }
      }
    }
    return null;
  }

  public HRegionLocation[] getRegionLocations() {
    return locations;
  }

  public HRegionLocation getDefaultRegionLocation() {
    return locations[RegionInfo.DEFAULT_REPLICA_ID];
  }

  /**
   * Returns the first not-null region location in the list
   */
  public HRegionLocation getRegionLocation() {
    for (HRegionLocation loc : locations) {
      if (loc != null) {
        return loc;
      }
    }
    return null;
  }

  @Override
  public Iterator<HRegionLocation> iterator() {
    return Arrays.asList(locations).iterator();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("[");
    for (HRegionLocation loc : locations) {
      if (builder.length() > 1) {
        builder.append(", ");
      }
      builder.append(loc == null ? "null" : loc);
    }
    builder.append("]");
    return builder.toString();
  }

}
