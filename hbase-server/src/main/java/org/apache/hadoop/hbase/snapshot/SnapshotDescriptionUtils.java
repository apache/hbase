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
package org.apache.hadoop.hbase.snapshot;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.fs.legacy.snapshot.SnapshotManifestV2;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Utility class to help manage {@link SnapshotDescription SnapshotDesriptions}.
 */
@InterfaceAudience.Private
public final class SnapshotDescriptionUtils {

  private static final Log LOG = LogFactory.getLog(SnapshotDescriptionUtils.class);
  /**
   * Version of the fs layout for a snapshot. Future snapshots may have different file layouts,
   * which we may need to read in differently.
   */
  public static final int SNAPSHOT_LAYOUT_VERSION = SnapshotManifestV2.DESCRIPTOR_VERSION;

  // snapshot operation values
  /** Default value if no start time is specified */
  public static final long NO_SNAPSHOT_START_TIME_SPECIFIED = 0;


  public static final String MASTER_SNAPSHOT_TIMEOUT_MILLIS = "hbase.snapshot.master.timeout.millis";

  /** By default, wait 300 seconds for a snapshot to complete */
  public static final long DEFAULT_MAX_WAIT_TIME = 60000 * 5 ;


  /**
   * By default, check to see if the snapshot is complete (ms)
   * @deprecated Use {@link #DEFAULT_MAX_WAIT_TIME} instead.
   * */
  @Deprecated
  public static final int SNAPSHOT_TIMEOUT_MILLIS_DEFAULT = 60000 * 5;

  /**
   * Conf key for # of ms elapsed before injecting a snapshot timeout error when waiting for
   * completion.
   * @deprecated Use {@link #MASTER_SNAPSHOT_TIMEOUT_MILLIS} instead.
   */
  @Deprecated
  public static final String SNAPSHOT_TIMEOUT_MILLIS_KEY = "hbase.snapshot.master.timeoutMillis";

  private SnapshotDescriptionUtils() {
    // private constructor for utility class
  }

  /**
   * @param conf {@link Configuration} from which to check for the timeout
   * @param type type of snapshot being taken
   * @param defaultMaxWaitTime Default amount of time to wait, if none is in the configuration
   * @return the max amount of time the master should wait for a snapshot to complete
   */
  public static long getMaxMasterTimeout(Configuration conf, SnapshotDescription.Type type,
      long defaultMaxWaitTime) {
    String confKey;
    switch (type) {
    case DISABLED:
    default:
      confKey = MASTER_SNAPSHOT_TIMEOUT_MILLIS;
    }
    return Math.max(conf.getLong(confKey, defaultMaxWaitTime),
        conf.getLong(SNAPSHOT_TIMEOUT_MILLIS_KEY, defaultMaxWaitTime));
  }

  /**
   * Convert the passed snapshot description into a 'full' snapshot description based on default
   * parameters, if none have been supplied. This resolves any 'optional' parameters that aren't
   * supplied to their default values.
   * @param snapshot general snapshot descriptor
   * @param conf Configuration to read configured snapshot defaults if snapshot is not complete
   * @return a valid snapshot description
   * @throws IllegalArgumentException if the {@link SnapshotDescription} is not a complete
   *           {@link SnapshotDescription}.
   */
  public static SnapshotDescription validate(SnapshotDescription snapshot, Configuration conf)
      throws IllegalArgumentException {
    if (!snapshot.hasTable()) {
      throw new IllegalArgumentException(
        "Descriptor doesn't apply to a table, so we can't build it.");
    }

    // set the creation time, if one hasn't been set
    long time = snapshot.getCreationTime();
    if (time == SnapshotDescriptionUtils.NO_SNAPSHOT_START_TIME_SPECIFIED) {
      time = EnvironmentEdgeManager.currentTime();
      LOG.debug("Creation time not specified, setting to:" + time + " (current time:"
          + EnvironmentEdgeManager.currentTime() + ").");
      SnapshotDescription.Builder builder = snapshot.toBuilder();
      builder.setCreationTime(time);
      snapshot = builder.build();
    }
    return snapshot;
  }

  /**
   * Check if the user is this table snapshot's owner
   * @param snapshot the table snapshot description
   * @param user the user
   * @return true if the user is the owner of the snapshot,
   *         false otherwise or the snapshot owner field is not present.
   */
  public static boolean isSnapshotOwner(final SnapshotDescription snapshot, final User user) {
    if (user == null) return false;
    if (!snapshot.hasOwner()) return false;
    return snapshot.getOwner().equals(user.getShortName());
  }
}
