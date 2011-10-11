package org.apache.hadoop.hbase.loadtest;

import java.math.BigInteger;
import java.security.MessageDigest;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.lang.StringUtils;

final class HashingSchemes
{
  public static final String SHA_1 = "SHA-1";
  public static final String SHA1 = "SHA1";
  public static final String MD5 = "MD5";
}


public class RegionSplitter {

  private final static String MAXMD5 = "7FFFFFFF";
  private final static int rowComparisonLength = MAXMD5.length();

  /**
   * Creates splits for the given hashingType.
   * @param hashingType
   * @param numberOfSplits
   * @return Byte array of size (numberOfSplits-1) corresponding to the
   * boundaries between splits.
   * @throws NoSuchAlgorithmException if the algorithm is not supported by
   * this splitter
   */
  public static byte[][] splitKeys(String hashingType, int numberOfSplits) {
    if (hashingType.equals(HashingSchemes.MD5)) {
      return splitKeysMD5(numberOfSplits);
    } else {
      throw new UnsupportedOperationException("This algorithm is not" +
      " currently supported by this class");
    }
  }

  /**
   * Creates splits for MD5 hashing.
   * @param numberOfSplits
   * @return Byte array of size (numberOfSplits-1) corresponding to the
   * boundaries between splits.
   */
  private static byte[][] splitKeysMD5(int numberOfSplits) {
    BigInteger max = new BigInteger(MAXMD5, 16);
    BigInteger[] bigIntegerSplits = split(max, numberOfSplits);
    byte[][] byteSplits = convertToBytes(bigIntegerSplits);
    return byteSplits;
  }

  /**
   * Splits the given BigInteger into numberOfSplits parts
   * @param maxValue
   * @param numberOfSplits
   * @return array of BigInteger which is of size (numberOfSplits-1)
   */
  private static BigInteger[] split(BigInteger maxValue, int numberOfSplits) {
    BigInteger[] splits = new BigInteger[numberOfSplits-1];
    BigInteger sizeOfEachSplit = maxValue.divide(BigInteger.
        valueOf(numberOfSplits));
    for (int i = 1; i < numberOfSplits; i++) {
      splits[i-1] = sizeOfEachSplit.multiply(BigInteger.valueOf(i));
    }
    return splits;
  }

  /**
   * Returns an array of bytes corresponding to an array of BigIntegers
   * @param bigIntegers
   * @return bytes corresponding to the bigIntegers
   */
  private static byte[][] convertToBytes(BigInteger[] bigIntegers) {
    byte[][] returnBytes = new byte[bigIntegers.length][];
    for (int i = 0; i < bigIntegers.length; i++) {
      returnBytes[i] = convertToByte(bigIntegers[i]);
    }
    return returnBytes;
  }

  /**
   * Returns the bytes corresponding to the BigInteger
   * @param bigInteger
   * @return byte corresponding to input BigInteger
   */
  private static byte[] convertToByte(BigInteger bigInteger) {
    String bigIntegerString = bigInteger.toString(16);
    bigIntegerString = StringUtils.leftPad(bigIntegerString,
        rowComparisonLength, '0');
    return Bytes.toBytes(bigIntegerString);
  }

  /////////////////////////////////////
  /**Code for hashing*/
  /////////////////////////////////////

  public static byte[] getHBaseKeyFromRowID(long rowID) {
    return getHBaseKeyFromEmail(rowID+"");
  }

  public static byte[] getHBaseKeyFromEmail(String email) {
    String ret = hashToString(hash(email));
    ret += ":" + email;
    return Bytes.toBytes(ret);
  }

  public static String hashToString(BigInteger data) {
    String ret = data.toString(16);
    return "00000000000000000000000000000000".substring(ret.length()) + ret;
  }

  public static BigInteger hash(String data)
  {
    byte[] result = hash(HashingSchemes.MD5, data.getBytes());
    BigInteger hash = new BigInteger(result);
    return hash.abs();
  }

  public static byte[] hash(String type, byte[] data)
  {
    byte[] result = null;
    try {
      MessageDigest messageDigest = MessageDigest.getInstance(type);
      result = messageDigest.digest(data);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    return result;
  }

}
