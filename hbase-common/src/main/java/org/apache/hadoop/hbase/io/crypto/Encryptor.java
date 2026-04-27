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

import java.io.OutputStream;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Encryptors apply a cipher to an OutputStream to produce ciphertext.
 */
@InterfaceAudience.Public
public interface Encryptor extends CipherOperator {

  /**
   * Create a stream for encryption
   */
  OutputStream createEncryptionStream(OutputStream out);

  /**
   * Return the number of IV increments to apply after encrypting data of the given ciphertext size.
   * For CTR mode, this is based on the number of blocks; for GCM mode, each encrypt operation uses
   * a single nonce so the increment is always 1.
   * @param ciphertextSize the size of the ciphertext produced
   * @return the IV increment value
   */
  default int getIvIncrement(int ciphertextSize) {
    return 1 + (ciphertextSize / getBlockSize());
  }
}
