package org.apache.hadoop.hbase.mapreduce.loadtest;

import java.lang.reflect.Constructor;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

/**
 * The DataGenerator class and its subclasses define the relationship between
 * rows and columns and columns and values. These classes are used to construct
 * the contents to write to a table as well as for verifying the integrity of
 * the same data when it is read back from a table.
 */
public abstract class DataGenerator {

  /**
   * Get the column qualifiers for a specified row. A row with 0 columns is
   * allowed.
   *
   * @param row
   * @return the column qualifiers
   */
  public abstract byte[][] getColumnQualifiers(byte[] row);

  /**
   * Get the contents of a single column in a row.
   *
   * @param row
   * @param column
   * @return the contents of the column
   */
  public abstract byte[] getContent(byte[] row, byte[] column);

  /**
   * Get the contents of all columns in a row.
   *
   * @param row
   * @param columns
   * @return the contents of all columns
   */
  public abstract byte[][] getContents(byte[] row, byte[][] columns);

  /**
   * Verify that the specified result is consistent, that it has the expected
   * column names and values for the corresponding key.
   *
   * @param result the result from a get or scan operation to be verified
   * @return true if the result is consistent
   */
  public abstract boolean verify(Result result);

  /**
   * Construct a get operation which will get all of the columns of a specified
   * key from a specified column family.
   *
   * @param key the key within the load-tester key space
   * @param columnFamily
   * @return a get operation for the specified key and column family
   */
  public abstract Get constructGet(long key, byte[] columnFamily);

  /**
   * Construct a put for the specified key into the specified column family. The
   * returned put will contain all of the key values to be inserted in a single
   * operation. This may return null if the row should have 0 columns.
   *
   * @param key the key within the load-tester key space
   * @param columnFamily
   * @return a single put containing all of the key values
   */
  public abstract Put constructBulkPut(long key, byte[] columnFamily);

  /**
   * Construct puts for the specified key into the specified column family. Each
   * key value will be in a distinct put. This may return null if the row should
   * have 0 columns.
   *
   * @param key the key within the load-tester key space
   * @param columnFamily
   * @return a list of puts, with each put having a single key value
   */
  public abstract List<Put> constructPuts(long key, byte[] columnFamily);

  /**
   * Construct a row key from a long key. The key will be prefixed by the MD5
   * hash of the long key to randomize the order of keys.
   *
   * @param key the long key which is to be prefixed
   * @return the prefixed key
   */
  public static String md5PrefixedKey(long key) {
    String stringKey = Long.toString(key);
    String md5hash = MD5Hash.getMD5AsHex(Bytes.toBytes(stringKey));

    // flip the key to randomize
    return md5hash + ":" + stringKey;
  }

  /**
   * Create a new instance of the specified DataGenerator subclass, given the
   * specified arguments. The structure of the arguments depends on the
   * implementation of the DataGenerator subclass.
   *
   * @param className the full class name of the DataGenerator to instantiate
   * @param args the arguments required by the DataGenerator implementation
   * @return a new instance of the specified DataGenerator class
   * @throws RuntimeException if for any reason the DataGenerator could not be
   *         instantiated
   */
  public static DataGenerator newInstance(String className, String args) {
    try {
      @SuppressWarnings("unchecked")
      Class<DataGenerator> theClass =
          (Class<DataGenerator>)Class.forName(className);
      Constructor<DataGenerator> constructor =
          theClass.getDeclaredConstructor(String.class);
      return (DataGenerator)constructor.newInstance(args);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
