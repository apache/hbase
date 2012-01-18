package org.apache.hadoop.hbase.mapreduce.loadtest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

/**
 * A DataGenerator that pseudo-randomly chooses the number, qualifiers and
 * contents of columns. PRNGs are seeded in a repeatable way per row key.
 */
public class RandomDataGenerator extends DataGenerator {

  private static final int MIN_COLUMNS = 1;
  private static final int MAX_COLUMNS = 65535;

  private static final double DEFAULT_NUM_COLUMNS_MEAN = 10;
  private static final double DEFAULT_QUALIFIER_SIZE_MEAN = 16;
  private static final double DEFAULT_COLUMN_SIZE_MEAN = 512;

  private final double numColumnsMean;
  private final double qualifierSizeMean;
  private final double columnSizeMean;

  /**
   * Constructs a new RandomDataGenerator, parsing properties from the passed
   * arguments if any, using defaults otherwise.
   *
   * @param args
   */
  public RandomDataGenerator(String args) {
    if (args != null) {
      String[] splits = args.split(":");
      if (splits.length != 3) {
        throw new IllegalArgumentException("wrong number of parameters");
      }
      numColumnsMean = Double.parseDouble(splits[0]);
      qualifierSizeMean = Double.parseDouble(splits[1]);
      columnSizeMean = Double.parseDouble(splits[2]);
    } else {
      numColumnsMean = DEFAULT_NUM_COLUMNS_MEAN;
      qualifierSizeMean = DEFAULT_QUALIFIER_SIZE_MEAN;
      columnSizeMean = DEFAULT_COLUMN_SIZE_MEAN;
    }
  }

  /**
   * Choose a random number of columns between MIN_COLUMNS and MAX_COLUMNS (both
   * inclusive) with a gaussian distribution around numColumnsMean. The length
   * of each column qualifier is chosen randomly with a gaussian distribution.
   * The first two bytes of each column qualifier are the index of that column
   * within the row (in big endian order) and the rest of the qualifier is
   * chosen randomly.
   */
  public byte[][] getColumnQualifiers(byte[] row) {
    Random random = new Random(new String(row).hashCode());
    int numCol = (int) Math.min(Math.max(MIN_COLUMNS,
        Math.round(random.nextGaussian() + numColumnsMean)), MAX_COLUMNS);
    byte[][] qualifiers = new byte[numCol][];

    for (int i = 0; i < numCol; i++) {
      int qualifierLength = (int) Math.max(2,
          Math.round(random.nextGaussian() + qualifierSizeMean));
      qualifiers[i] = new byte[qualifierLength];
      random.nextBytes(qualifiers[i]);
      qualifiers[i][0] = (byte)((i >>> 8) & 0xff);
      qualifiers[i][1] = (byte)(i & 0xff);
    }
    return qualifiers;
  }

  /**
   * Column content length and value are chosen randomly, seeded by the row key.
   */
  public byte[] getContent(byte[] row, byte[] column) {
    Random random = new Random(new String(column).hashCode());
    int contentLength =
        (int) Math.max(1, random.nextGaussian() + columnSizeMean);
    byte[] content = new byte[contentLength];
    random.nextBytes(content);
    return content;
  }

  /**
   * Column content length and value are chosen randomly, seeded by the row key.
   */
  public byte[][] getContents(byte[] row, byte[][] columns) {
    byte[][] contents = new byte[columns.length][];
    Random random = new Random();
    for (int i = 0; i < columns.length; i++) {
      random.setSeed(new String(columns[i]).hashCode());
      int contentLength =
          (int)Math.max(1, random.nextGaussian() + columnSizeMean);
      contents[i] = new byte[contentLength];
      random.nextBytes(contents[i]);
    }
    return contents;
  }

  public boolean verify(Result result) {
    if (result.isEmpty()) {
      return false;
    }
    byte[] row = result.getRow();
    byte[][] columns = getColumnQualifiers(row);
    byte[][] contents = getContents(row, columns);
    boolean[] verifiedColumns = new boolean[columns.length];

    for (KeyValue kv : result.list()) {
      byte[] qualifier = kv.getQualifier();
      int index = qualifier[0] << 8 | qualifier[1];
      if (!Arrays.equals(kv.getQualifier(), columns[index])) {
        // The column qualifier did not match the expected qualifier.
        return false;
      } else {
        if (!Arrays.equals(kv.getValue(), contents[index])) {
          return false;
        }
        verifiedColumns[index] = true;
      }
    }
    // Check that all columns were present in the result.
    for (boolean verifiedColumn : verifiedColumns) {
      if (!verifiedColumn) {
        return false;
      }
    }
    return true;
  }

  public Get constructGet(long key, byte[] columnFamily) {
    Get get = new Get(DataGenerator.md5PrefixedKey(key).getBytes());
    get.addFamily(columnFamily);
    return get;
  }

  public Put constructBulkPut(long key, byte[] columnFamily) {
    byte[] row = DataGenerator.md5PrefixedKey(key).getBytes();
    byte[][] qualifiers = getColumnQualifiers(row);
    if (qualifiers.length == 0) {
      return null;
    }
    byte[][] contents = getContents(row, qualifiers);
    Put put = new Put(row);
    for (int i = 0; i < qualifiers.length; i++) {
      put.add(columnFamily, qualifiers[i], contents[i]);
    }
    return put;
  }

  public List<Put> constructPuts(long key, byte[] columnFamily) {
    byte[] row = DataGenerator.md5PrefixedKey(key).getBytes();
    byte[][] qualifiers = getColumnQualifiers(row);
    if (qualifiers.length == 0) {
      return null;
    }
    byte[][] contents = getContents(row, qualifiers);
    List<Put> puts = new ArrayList<Put>(contents.length);
    for (int i = 0; i < qualifiers.length; i++) {
      Put put = new Put(row);
      put.add(columnFamily, qualifiers[i], contents[i]);
      puts.add(put);
    }
    return puts;
  }

}
