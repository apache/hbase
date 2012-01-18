package org.apache.hadoop.hbase.mapreduce.loadtest;

/**
 * OperationGenerators provide a sequence of operations according to some rules
 * or patterns defined by the implementation.
 */
public interface OperationGenerator {

  /**
   * Get the next operation to be executed. This method may occasionally return
   * null if there was a transient issue in generating the operation. Returning
   * null is not an error and should simply be ignored. This allows other
   * OperationGenerator instances an opportunity to return operations instead of
   * waiting for any particular instance to return its next value operation.
   *
   * Implementations may be intended to only provide a certain number of
   * operations. Those implementations will thereafter throw ExhaustedExceptions
   * when nextOperation() is invoked on them. Any instance which has thrown an
   * ExhaustedException should no longer be asked for more operations, unless
   * implementation-specific measures are used to reset that instance.
   *
   * @param dataGenerator DataGenerator used to construct the next operation
   * @return the next operation, or null
   * @throws ExhaustedException if the
   */
  public Operation nextOperation(DataGenerator dataGenerator)
      throws ExhaustedException;

  /**
   * Thrown when an instance has run out of operations to return.
   */
  public static class ExhaustedException extends Exception {
    private static final long serialVersionUID = -5647515931122719787L;
  }

}
