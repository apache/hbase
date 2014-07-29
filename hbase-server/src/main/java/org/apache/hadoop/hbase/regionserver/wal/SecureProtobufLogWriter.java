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

package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.security.Key;
import java.security.SecureRandom;

import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.Encryptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALHeader;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class SecureProtobufLogWriter extends ProtobufLogWriter {

  private static final Log LOG = LogFactory.getLog(SecureProtobufLogWriter.class);
  private static final String DEFAULT_CIPHER = "AES";

  private Encryptor encryptor = null;

  @Override
  protected WALHeader buildWALHeader(Configuration conf, WALHeader.Builder builder)
      throws IOException {
    builder.setWriterClsName(SecureProtobufLogWriter.class.getSimpleName());
    if (conf.getBoolean(HConstants.ENABLE_WAL_ENCRYPTION, false)) {
      // Get an instance of our cipher
      final String cipherName = conf.get(HConstants.CRYPTO_WAL_ALGORITHM_CONF_KEY, DEFAULT_CIPHER);
      Cipher cipher = Encryption.getCipher(conf, cipherName);
      if (cipher == null) {
        throw new RuntimeException("Cipher '" + cipherName + "' is not available");
      }

      // Generate an encryption key for this WAL
      SecureRandom rng = new SecureRandom();
      byte[] keyBytes = new byte[cipher.getKeyLength()];
      rng.nextBytes(keyBytes);
      Key key = new SecretKeySpec(keyBytes, cipher.getName());
      builder.setEncryptionKey(ByteStringer.wrap(EncryptionUtil.wrapKey(conf,
          conf.get(HConstants.CRYPTO_WAL_KEY_NAME_CONF_KEY,
              conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY,
                  User.getCurrent().getShortName())),
          key)));

      // Set up the encryptor
      encryptor = cipher.getEncryptor();
      encryptor.setKey(key);

      if (LOG.isTraceEnabled()) {
        LOG.trace("Initialized secure protobuf WAL: cipher=" + cipher.getName());
      }
    }
    builder.setCellCodecClsName(SecureWALCellCodec.class.getName());
    return super.buildWALHeader(conf, builder);
  }

  @Override
  protected void initAfterHeader(boolean doCompress) throws IOException {
    if (conf.getBoolean(HConstants.ENABLE_WAL_ENCRYPTION, false) && encryptor != null) {
      WALCellCodec codec = SecureWALCellCodec.getCodec(this.conf, encryptor);
      this.cellEncoder = codec.getEncoder(this.output);
      // We do not support compression
      this.compressionContext = null;
    } else {
      super.initAfterHeader(doCompress);
    }
  }

}
