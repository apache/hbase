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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Utility functions for region server storage layer.
 */
public class StoreUtils {
  /**
   * Creates a deterministic hash code for store file collection.
   */
  public static Integer getDeterministicRandomSeed(final List<StoreFile> files) {
    if (files != null && !files.isEmpty()) {
      return files.get(0).getPath().getName().hashCode();
    }
    return null;
  }

  /**
   * Determines whether any files in the collection are references.
   */
  public static boolean hasReferences(final Collection<StoreFile> files) {
    if (files != null && files.size() > 0) {
      for (StoreFile hsf: files) {
        if (hsf.isReference()) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Gets lowest timestamp from candidate StoreFiles
   */
  public static long getLowestTimestamp(final List<StoreFile> candidates)
    throws IOException {
    long minTs = Long.MAX_VALUE;
    for (StoreFile storeFile : candidates) {
      minTs = Math.min(minTs, storeFile.getModificationTimeStamp());
    }
    return minTs;
  }
}
