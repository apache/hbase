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
package org.apache.hadoop.hbase.security;

import java.io.IOException;
import java.util.Properties;
import org.apache.commons.crypto.cipher.CryptoCipherFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.crypto.aes.CryptoAES;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;

/**
 * Some static utility methods for encryption uses in hbase-client.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class EncryptionUtil {

  /**
   * Private constructor to keep this class from being instantiated.
   */
  private EncryptionUtil() {
  }

  /**
   * Helper to create an instance of CryptoAES.
   * @param conf             The current configuration.
   * @param cryptoCipherMeta The metadata for create CryptoAES.
   * @return The instance of CryptoAES.
   * @throws IOException if create CryptoAES failed
   */
  public static CryptoAES createCryptoAES(RPCProtos.CryptoCipherMeta cryptoCipherMeta,
    Configuration conf) throws IOException {
    Properties properties = new Properties();
    // the property for cipher class
    properties.setProperty(CryptoCipherFactory.CLASSES_KEY,
      conf.get("hbase.rpc.crypto.encryption.aes.cipher.class",
        "org.apache.commons.crypto.cipher.JceCipher"));
    // create SaslAES for client
    return new CryptoAES(cryptoCipherMeta.getTransformation(), properties,
      cryptoCipherMeta.getInKey().toByteArray(), cryptoCipherMeta.getOutKey().toByteArray(),
      cryptoCipherMeta.getInIv().toByteArray(), cryptoCipherMeta.getOutIv().toByteArray());
  }
}
