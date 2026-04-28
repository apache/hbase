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
package org.apache.hadoop.hbase.io.crypto;

import java.security.Key;
import java.security.spec.AlgorithmParameterSpec;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Common interface for cipher operations shared by both {@link Encryptor} and {@link Decryptor}.
 */
@InterfaceAudience.Public
public interface CipherOperator {

  /**
   * Set the secret key
   */
  void setKey(Key key);

  /**
   * Get the expected length for the initialization vector
   * @return the expected length for the initialization vector
   */
  int getIvLength();

  /**
   * Get the cipher's internal block size
   * @return the cipher's internal block size
   */
  int getBlockSize();

  /**
   * Get the initialization vector
   */
  byte[] getIv();

  /**
   * Set the initialization vector
   */
  void setIv(byte[] iv);

  /**
   * Set custom algorithm parameters for the cipher. If set, it will bypass the default parameter
   * generation logic during the cipher initialization. This must be called before the cipher is
   * initialized.
   * @param algorithmParameter the algorithm parameter
   */
  void setAlgorithmParameter(AlgorithmParameterSpec algorithmParameter);

  /**
   * Reset state, reinitialize with the key and iv
   */
  void reset();
}
