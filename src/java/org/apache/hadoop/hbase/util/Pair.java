package org.apache.hadoop.hbase.util;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.ArrayUtils;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A generic class for immutable pairs.
 * @param <T1>
 * @param <T2>
 */
public final class Pair<T1, T2> implements Serializable
{
  private static final long serialVersionUID = -3986244606585552569L;
  protected T1 first = null;
  protected T2 second = null;
  private int hashcode;


  /**
   * Constructor
   * @param a
   * @param b
   */
  public Pair(T1 a, T2 b)
  {
    this.first = a;
    this.second = b;
    hashcode = new HashCodeBuilder().append(first).append(second).toHashCode();
  }

  /**
   * Return the first element stored in the pair.
   * @return T1
   */
  public T1 getFirst()
  {
    return first;
  }

  /**
   * Return the second element stored in the pair.
   * @return T2
   */
  public T2 getSecond()
  {
    return second;
  }

  /**
   * Creates a new instance of the pair encapsulating the supplied values.
   *
   * @param one  the first value
   * @param two  the second value
   * @param <T1> the type of the first element.
   * @param <T2> the type of the second element.
   * @return the new instance
   */
  public static <T1, T2> Pair<T1, T2> of(T1 one, T2 two)
  {
    return new Pair<T1, T2>(one, two);
  }

  private static boolean equals(Object x, Object y)
  {
    // Null safe compare first
    if (x == null || y == null) {
      return x == y;
    }

    Class clazz = x.getClass();
    // If they are both the same type of array
    if (clazz.isArray() && clazz == y.getClass()) {
      // Do an array compare instead
      return ArrayUtils.isEquals(x, y);
    } else {
      // Standard comparison
      return x.equals(y);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object other)
  {
    return other instanceof Pair && equals(first, ((Pair)other).first) &&
      equals(second, ((Pair)other).second);
  }

  @Override
  public int hashCode()
  {
    return hashcode;
  }

  @Override
  public String toString()
  {
    return "{" + getFirst() + "," + getSecond() + "}"; // TODO user ToStringBuilder
  }
}