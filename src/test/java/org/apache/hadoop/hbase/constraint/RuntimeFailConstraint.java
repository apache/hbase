package org.apache.hadoop.hbase.constraint;

import org.apache.hadoop.hbase.client.Put;

/**
 * Always non-gracefully fail on attempt
 */
public class RuntimeFailConstraint extends BaseConstraint {

  @Override
  public void check(Put p) throws ConstraintException {
    throw new RuntimeException(
        "RuntimeFailConstraint always throws a runtime exception");
  }

}
