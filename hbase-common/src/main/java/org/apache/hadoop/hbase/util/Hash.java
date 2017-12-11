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

package org.apache.hadoop.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * This class represents a common API for hashing functions.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public abstract class Hash {
  /** Constant to denote invalid hash type. */
  public static final int INVALID_HASH = -1;
  /** Constant to denote {@link JenkinsHash}. */
  public static final int JENKINS_HASH = 0;
  /** Constant to denote {@link MurmurHash}. */
  public static final int MURMUR_HASH  = 1;
  /** Constant to denote {@link MurmurHash3}. */
  public static final int MURMUR_HASH3 = 2;

  /**
   * This utility method converts String representation of hash function name
   * to a symbolic constant. Currently three function types are supported,
   * "jenkins", "murmur" and "murmur3".
   * @param name hash function name
   * @return one of the predefined constants
   */
  public static int parseHashType(String name) {
    if ("jenkins".equalsIgnoreCase(name)) {
      return JENKINS_HASH;
    } else if ("murmur".equalsIgnoreCase(name)) {
      return MURMUR_HASH;
    } else if ("murmur3".equalsIgnoreCase(name)) {
      return MURMUR_HASH3;
    } else {
      return INVALID_HASH;
    }
  }

  /**
   * This utility method converts the name of the configured
   * hash type to a symbolic constant.
   * @param conf configuration
   * @return one of the predefined constants
   */
  public static int getHashType(Configuration conf) {
    String name = conf.get("hbase.hash.type", "murmur");
    return parseHashType(name);
  }

  /**
   * Get a singleton instance of hash function of a given type.
   * @param type predefined hash type
   * @return hash function instance, or null if type is invalid
   */
  public static Hash getInstance(int type) {
    switch (type) {
      case JENKINS_HASH:
        return JenkinsHash.getInstance();
      case MURMUR_HASH:
        return MurmurHash.getInstance();
      case MURMUR_HASH3:
        return MurmurHash3.getInstance();
      default:
        return null;
    }
  }

  /**
   * Get a singleton instance of hash function of a type
   * defined in the configuration.
   * @param conf current configuration
   * @return defined hash type, or null if type is invalid
   */
  public static Hash getInstance(Configuration conf) {
    int type = getHashType(conf);
    return getInstance(type);
  }

  /**
   * Calculate a hash using bytes from HashKey and the provided seed value.
   * @param <T>
   * @param hashKey key to extract the hash
   * @param initval the seed value
   * @return hash value
   */
  public abstract <T> int hash(HashKey<T> hashKey, int initval);
}
