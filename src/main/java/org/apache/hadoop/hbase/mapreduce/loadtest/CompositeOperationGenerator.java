package org.apache.hadoop.hbase.mapreduce.loadtest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An operation generator which is a composite of other operation generators.
 * Child generators will be assigned weights and will contribute operations with
 * frequency proportional to their weight.
 */
public class CompositeOperationGenerator implements OperationGenerator {

  // Maintain a list of generators where each child is referenced the same
  // multiple times according to its weight. It is expected that this list will
  // rarely be updated, making a copy-on-write implementation efficient.
  private final CopyOnWriteArrayList<OperationGenerator> generators;

  // The sequence number is incremented for each operation generated, and is
  // used to determine from which child to generate the next operation.
  private final AtomicLong nextSequence;

  /**
   * Create a new instance with no child generators.
   */
  public CompositeOperationGenerator() {
    nextSequence = new AtomicLong(0);
    generators = new CopyOnWriteArrayList<OperationGenerator>();
  }

  /**
   * Add a child generator with a certain weight. The added child generator will
   * be used to generate operations with frequency proportional to its weight
   * relative to the total weight of all child generators of this instance. The
   * value of weights are not required to sum to any particular value. Child
   * weights should be reduced by the lowest common divisor if possible.
   *
   * @param generator the child generator to be added
   * @param weight the relative weight of the child generator
   */
  public void addGenerator(OperationGenerator generator, int weight) {
    if (weight <= 0) {
      throw new IllegalArgumentException("generator weights must be positive");
    }
    List<OperationGenerator> newGenerators =
        new ArrayList<OperationGenerator>();
    for (int i = 0; i < weight; i++) {
      newGenerators.add(generator);
    }
    generators.addAll(newGenerators);
  }

  /**
   * Remove a child generator from this instance. All child instances equal to
   * the passed generator will be removed.
   *
   * @param generator
   */
  public void removeGenerator(OperationGenerator generator) {
    while (generators.remove(generator));
  }

  /**
   * Get the next operation in the combined sequence of operations of this
   * instances child generators. The returned operation may be null if there are
   * no children or if the next operation could not be generated. If a child
   * generator becomes exhausted as a result of calling nextOperation() on it,
   * then it will be removed from this instance's children.
   *
   * @return the next operation to be executed, or null if the next operation
   *         could not be found
   * @throws ExhaustedException if the last of this instance's child generators
   *         has itself become exhausted
   */
  @Override
  public Operation nextOperation(DataGenerator dataGenerator)
      throws ExhaustedException {
    if (generators.isEmpty()) {
      throw new ExhaustedException();
    }

    OperationGenerator next = generators.get(
        (int)(Math.abs(nextSequence.getAndIncrement() % generators.size())));

    try {
      return next.nextOperation(dataGenerator);
    } catch (ExhaustedException e) {
      removeGenerator(next);
      return null;
    } catch (IndexOutOfBoundsException e) {
      return null;
    } catch (ArithmeticException e) {
      return null;
    }
  }
}
