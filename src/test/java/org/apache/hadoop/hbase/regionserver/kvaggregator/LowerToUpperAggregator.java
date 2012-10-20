package org.apache.hadoop.hbase.regionserver.kvaggregator;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode;

/**
 * <p>
 * Dummy test aggregator which takes the String Value of the KV which is
 * expected to be in lowercase and transforms it to uppercase
 * </p>
 */
public class LowerToUpperAggregator implements KeyValueAggregator {

  @Override
  public void reset() {
  }

  @Override
  public KeyValue process(KeyValue kv) {
    byte[] newValue;
    String currentValue = new String(kv.getValue());
    /**
     * transform it to uppercase.
     */
    String newValueString = currentValue.toUpperCase();
    newValue = newValueString.getBytes();
    KeyValue newKv = new KeyValue(kv.getRow(), kv.getFamily(),
        kv.getQualifier(), kv.getTimestamp(), newValue);
    return newKv;
  }

  @Override
  public MatchCode nextAction(MatchCode origCode) {
    return origCode;
  }

  @Override
  public KeyValue finalizeKeyValues() {
    return null;
  }
}
