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
package org.apache.hadoop.hbase.keymeta;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.DigestAlgorithms;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Binary encoding for key full identity row keys (custodian + namespace + partial identity). Shared
 * by {@link KeyIdentitySingleArrayBacked} and server-side persistence utilities.
 */
@InterfaceAudience.Private
public final class ManagedKeyIdentityUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ManagedKeyIdentityUtils.class);
  private static final short MAX_UNSIGNED_BYTE = 255;
  /** Cached digest algorithm list; parsed once on first use and not updated for JVM life. */
  private static List<DigestAlgorithms> DIGEST_ALGOS = null;

  private ManagedKeyIdentityUtils() {
  }

  /**
   * Build row key prefix for custodian + namespace. Format: [custLen (1 byte)][keyCust][nsLen (1
   * byte)][keyNamespace bytes].
   */
  public static byte[] constructRowKeyForCustNamespace(byte[] keyCust, byte[] keyNamespace) {
    return constructRowKey(keyCust, keyNamespace, null);
  }

  /**
   * Build full row key for identity row. Format: prefix from
   * {@link #constructRowKeyForCustNamespace} + [partialIdentityLen (1 byte)] [partialIdentity].
   * Partial identity length is encoded in a single byte (max 255).
   * @param keyCustodian    key custodian bytes
   * @param keyNamespace    key namespace bytes
   * @param partialIdentity partial identity bytes (digest of metadata), can be of 0-length for a
   *                        marker row.
   * @return full identity byte array suitable for SystemKeyCache and HFile trailer
   */
  public static byte[] constructRowKeyForIdentity(byte[] keyCustodian, byte[] keyNamespace,
    byte[] partialIdentity) {
    Preconditions.checkNotNull(partialIdentity, "partialIdentity cannot be null");
    Preconditions.checkArgument(
      partialIdentity.length >= 0 && partialIdentity.length <= MAX_UNSIGNED_BYTE,
      "Partial identity length must be 0-255, got %s", partialIdentity.length);
    return constructRowKey(keyCustodian, keyNamespace, partialIdentity);
  }

  private static byte[] constructRowKey(byte[] keyCustodian, byte[] keyNamespace,
    byte[] partialIdentity) {
    validateCustodianAndNamespaceLength(keyCustodian, keyNamespace);
    int nsLen = keyNamespace.length;
    int piLen = partialIdentity == null ? 0 : partialIdentity.length;
    byte[] result = new byte[1 + keyCustodian.length + 1 + nsLen + (piLen > 0 ? 1 : 0) + piLen];
    int off = 0;
    result[off++] = (byte) keyCustodian.length;
    System.arraycopy(keyCustodian, 0, result, off, keyCustodian.length);
    off += keyCustodian.length;
    result[off++] = (byte) nsLen;
    System.arraycopy(keyNamespace, 0, result, off, nsLen);
    off += nsLen;
    if (piLen > 0) {
      result[off++] = (byte) piLen;
      System.arraycopy(partialIdentity, 0, result, off, piLen);
    }
    return result;
  }

  /**
   * Validates that key custodian and key namespace length are between 1 and the maximum allowed.
   */
  private static void validateCustodianAndNamespaceLength(byte[] keyCust, byte[] keyNamespace) {
    Preconditions.checkArgument(keyCust != null, "Key custodian cannot be null");
    Preconditions.checkArgument(keyNamespace != null, "Key namespace cannot be null");
    Preconditions.checkArgument(keyCust.length >= 1 && keyCust.length <= MAX_UNSIGNED_BYTE,
      "Key custodian length must be 1-%s, got %s", MAX_UNSIGNED_BYTE, keyCust.length);
    Preconditions.checkArgument(
      keyNamespace.length >= 1 && keyNamespace.length <= MAX_UNSIGNED_BYTE,
      "Key namespace length must be 1-%s, got %s", MAX_UNSIGNED_BYTE, keyNamespace.length);
  }

  /**
   * Construct the partial identity (digest) for the given metadata string. Uses default algorithm
   * (xxh3) when no configuration is available. The result is prefixed with a single byte encoding
   * the algorithm(s) used (bitwise OR of DigestAlgo bit positions).
   * @param metadata the key metadata string
   * @return partial identity bytes: [algoSelectorByte][digest1][digest2...]
   */
  public static byte[] constructMetadataHash(String metadata) {
    byte[] input = metadata.getBytes(StandardCharsets.UTF_8);
    int totalDigestSize = 0;
    for (DigestAlgorithms a : getDigestAlgos()) {
      totalDigestSize += a.getDigestSizeBytes();
    }
    byte[] result = new byte[1 + totalDigestSize];
    byte selector = 0;
    int outOff = 1;
    for (DigestAlgorithms a : getDigestAlgos()) {
      selector |= a.getBitPosition();
      a.digest(input, 0, input.length, result, outOff);
      outOff += a.getDigestSizeBytes();
    }
    result[0] = selector;
    return result;
  }

  public static List<DigestAlgorithms> getDigestAlgos() {
    if (DIGEST_ALGOS == null) {
      initDigestAlgos(null);
    }
    return DIGEST_ALGOS;
  }

  /**
   * Initializes the list of digest algorithms to use (up to 2), sorted by bitPosition. Dedupes by
   * algorithm and picks first 2; logs a warning if config lists more than 2. Parsed once on first
   * use and cached for the life of the JVM.
   * @param conf the configuration to use
   */
  public static void initDigestAlgos(Configuration conf) {
    if (DIGEST_ALGOS == null) {
      String algoList = conf != null
        ? conf.get(HConstants.CRYPTO_MANAGED_KEY_METADATA_DIGEST_ALGORITHMS_CONF_KEY,
          HConstants.CRYPTO_MANAGED_KEY_METADATA_DIGEST_ALGORITHMS_DEFAULT)
        : HConstants.CRYPTO_MANAGED_KEY_METADATA_DIGEST_ALGORITHMS_DEFAULT;
      String[] names = algoList.split(",");
      Set<DigestAlgorithms> deduped = new LinkedHashSet<>();
      for (String name : names) {
        DigestAlgorithms a = DigestAlgorithms.fromName(name);
        if (a != null) {
          deduped.add(a);
        }
      }
      List<DigestAlgorithms> sorted = new ArrayList<>(deduped);
      sorted.sort(Comparator.comparingInt(a -> a.getBitPosition() & 0xFF));
      if (sorted.size() > 2) {
        LOG.warn(
          "Configured digest algorithms list has more than 2 entries; using first 2 by bitPosition: "
            + sorted.get(0).name() + ", " + sorted.get(1).name());
        sorted = sorted.subList(0, 2);
      }
      if (sorted.isEmpty()) {
        sorted = new ArrayList<>();
        sorted.add(DigestAlgorithms.XXH3);
      }
      DIGEST_ALGOS = Collections.unmodifiableList(sorted);
    }
  }

  /**
   * Creates a {@link ManagedKeyIdentity} from custodian, namespace, and key metadata. The partial
   * identity is computed as the hash of the metadata string via {@link #constructMetadataHash}.
   * @param custodian custodian bytes
   * @param namespace namespace bytes
   * @param metadata  key metadata string
   * @return FullKeyIdentity for the given custodian, namespace, and metadata
   */
  public static ManagedKeyIdentity fullKeyIdentityFromMetadata(Bytes custodian, Bytes namespace,
    String metadata) {
    Preconditions.checkNotNull(custodian, "custodian should not be null");
    Preconditions.checkNotNull(namespace, "namespace should not be null");
    Preconditions.checkNotNull(metadata, "metadata should not be null");
    return new KeyIdentityBytesBacked(custodian, namespace,
      new Bytes(constructMetadataHash(metadata)));
  }

  public static ManagedKeyIdentity buildIdentityFromMetadata(byte[] key_cust, byte[] key_namespace,
    String keyMetadata) {
    return fullKeyIdentityFromMetadata(key_cust == null ? null : new Bytes(key_cust),
      key_namespace == null ? null : new Bytes(key_namespace), keyMetadata);
  }

  public static ManagedKeyIdentity buildIdentityFromMetadata(Bytes custodian, Bytes namespace,
    String keyMetadata) {
    Preconditions.checkNotNull(custodian, "custodian should not be null");
    Preconditions.checkNotNull(namespace, "namespace should not be null");
    Preconditions.checkNotNull(keyMetadata, "metadata should not be null");
    return new KeyIdentityBytesBacked(custodian, namespace,
      new Bytes(constructMetadataHash(keyMetadata)));
  }
}
