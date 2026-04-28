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
package org.apache.hadoop.hbase.io.crypto.aes;

import java.security.SecureRandom;
import javax.crypto.spec.GCMParameterSpec;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AES256GCMEncryptor extends AESEncryptor {
  public AES256GCMEncryptor(javax.crypto.Cipher cipher, SecureRandom rng) {
    super(cipher, rng);
  }

  @Override
  public int getIvLength() {
    return AES256GCM.NONCE_LENGTH;
  }

  @Override
  public int getIvIncrement(int ciphertextSize) {
    // GCM uses a unique nonce per encryption; increment by 1 regardless of data size
    return 1;
  }

  @Override
  protected void initAlgorithmParameter() {
    algorithmParameter = new GCMParameterSpec(AES256GCM.TAG_LENGTH_BITS, getIv());
  }
}
