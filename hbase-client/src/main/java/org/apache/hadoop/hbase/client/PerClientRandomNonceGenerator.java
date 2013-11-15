package org.apache.hadoop.hbase.client;

import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants;

/**
 * NonceGenerator implementation that uses client ID hash + random int as nonce group,
 * and random numbers as nonces.
 */
@InterfaceAudience.Private
public class PerClientRandomNonceGenerator implements NonceGenerator {
  private final Random rdm = new Random();
  private final long clientId;

  public PerClientRandomNonceGenerator() {
    byte[] clientIdBase = ClientIdGenerator.generateClientId();
    this.clientId = (((long)Arrays.hashCode(clientIdBase)) << 32) + rdm.nextInt();
  }

  public long getNonceGroup() {
    return this.clientId;
  }

  public long newNonce() {
    long result = HConstants.NO_NONCE;
    do {
      result = rdm.nextLong();
    } while (result == HConstants.NO_NONCE);
    return result;
  }
}
