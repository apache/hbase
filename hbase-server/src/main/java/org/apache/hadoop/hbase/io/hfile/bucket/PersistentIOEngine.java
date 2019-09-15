/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.hfile.bucket;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A class implementing PersistentIOEngine interface supports persistent and file integrity verify
 * for {@link BucketCache}
 */
@InterfaceAudience.Private
public interface PersistentIOEngine extends IOEngine {

  /**
   * Delete bucketcache files
   */
  void deleteCacheDataFile();

  /**
   * Using an encryption algorithm to calculate a checksum, the default encryption algorithm is MD5
   * @return the checksum which is convert to HexString
   * @throws IOException something happened like file not exists
   * @throws NoSuchAlgorithmException no such algorithm
   */
  byte[] calculateChecksum()
    throws IOException, NoSuchAlgorithmException;

  /**
   * Whether the persistent file support verify file integrity, it's for back compatibility
   * @return true if the persistent file does not support verify file integrity
   */
  boolean isOldVersion();

  /**
   * Verify cache files's integrity
   * @param persistentPath the backingMap persistent path
   */
  void verifyFileIntegrity(String persistentPath);
}
