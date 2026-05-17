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

import java.util.Properties;
import org.apache.commons.crypto.cipher.CryptoCipherFactory;
import org.apache.commons.crypto.stream.CryptoInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.crypto.CipherProvider;
import org.apache.hadoop.hbase.io.crypto.Decryptor;
import org.apache.hadoop.hbase.io.crypto.Encryptor;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CommonsCryptoAES extends AESCipher {

  public static final String CIPHER_MODE_KEY = "hbase.crypto.commons.mode";
  public static final String CIPHER_CLASSES_KEY = "hbase.crypto.commons.cipher.classes";
  public static final String CIPHER_JCE_PROVIDER_KEY = "hbase.crypto.commons.cipher.jce.provider";
  public static final String CRYPTOSTREAM_BUFFERSIZE_KEY =
    "hbase.crypto.commons.cryptoStream.bufferSize";

  private final String cipherMode;
  private final Properties props;

  public CommonsCryptoAES(CipherProvider provider) {
    super(provider);
    cipherMode = provider.getConf().get(CIPHER_MODE_KEY, "AES/CTR/NoPadding");
    props = readCryptoProps(provider.getConf());
  }

  private static Properties readCryptoProps(Configuration conf) {
    Properties props = new Properties();

    props.setProperty(CryptoCipherFactory.CLASSES_KEY, conf.get(CIPHER_CLASSES_KEY, ""));
    props.setProperty(CryptoCipherFactory.JCE_PROVIDER_KEY, conf.get(CIPHER_JCE_PROVIDER_KEY, ""));
    props.setProperty(CryptoInputStream.STREAM_BUFFER_SIZE_KEY,
      conf.get(CRYPTOSTREAM_BUFFERSIZE_KEY, ""));

    return props;
  }

  @Override
  public String getName() {
    return "AES";
  }

  @Override
  public int getKeyLength() {
    return KEY_LENGTH;
  }

  @Override
  public int getIvLength() {
    return IV_LENGTH;
  }

  @Override
  public Encryptor getEncryptor() {
    return new CommonsCryptoAESEncryptor(cipherMode, props, getRNG());
  }

  @Override
  public Decryptor getDecryptor() {
    return new CommonsCryptoAESDecryptor(cipherMode, props);
  }
}
