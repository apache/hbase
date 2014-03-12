package org.apache.hadoop.hbase.loadtest;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.NoServerForRegionException;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

final class HashingSchemes
{
  public static final String SHA_1 = "SHA-1";
  public static final String SHA1 = "SHA1";
  public static final String MD5 = "MD5";
}


public class RegionSplitter {
  private static final Log LOG = LogFactory.getLog(RegionSplitter.class);

  private final static String MAXMD5 = "7FFFFFFF";
  private final static BigInteger MAXMD5_INT = new BigInteger(MAXMD5, 16);
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
    BigInteger[] bigIntegerSplits = split(MAXMD5_INT, numberOfSplits);
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

  private static BigInteger split2(BigInteger minValue, BigInteger maxValue) {
    return maxValue.add(minValue).divide(BigInteger.valueOf(2));
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

  /**
   * Returns the BigInteger represented by thebyte array
   * @param row
   * @return the corresponding BigInteger
   */
  private static BigInteger convertToBigInteger(byte[] row) {
    if (row.length > 0) {
      return new BigInteger(Bytes.toString(row), 16);
    } else {
      return BigInteger.ZERO;
    }
  }

  /////////////////////////////////////
  /**Code from Prometheus for hashing*/
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

  /**
   * main(): performs a BalancedSplit on an existing table
   * @param args table
   * @throws IOException HBase IO problem
   * @throws InterruptedException user requested exit
   * @throws ParseException problem parsing user input
   */
  public static void main(String []args)
  throws IOException, InterruptedException, ParseException {
    Configuration conf = HBaseConfiguration.create();

    // parse user input
    Options opt = new Options();
    opt.addOption("o", true,
        "Max outstanding splits that have unfinished major compactions");
    opt.addOption("D", true,
        "Override HBase Configuration Settings");
    CommandLine cmd = new GnuParser().parse(opt, args);

    if (cmd.hasOption("D")) {
      for (String confOpt : cmd.getOptionValues("D")) {
        String[] kv = confOpt.split("=", 2);
        if (kv.length == 2) {
          conf.set(kv[0], kv[1]);
          LOG.debug("-D configuration override: " + kv[0] + "=" + kv[1]);
        } else {
          throw new ParseException("-D option format invalid: " + confOpt);
        }
      }
    }

    int minOs = cmd.hasOption("o")? Integer.valueOf(cmd.getOptionValue("o")):2;

    // input: tableName
    // TODO: add hashingType?
    if (1 != cmd.getArgList().size()) {
      System.err.println("Usage: RegionSplitter <TABLE>  " +
          "[-D <conf.param=value>] [-o <# outstanding splits>]");
      return;
    }
    String tableName = cmd.getArgs()[0];
    HTable table = new HTable(conf, tableName);

    // max outstanding split + associated compaction. default == 10% of servers
    final int MAX_OUTSTANDING = Math.max(table.getCurrentNrHRS() / 10, minOs);

    Path hbDir = new Path(table.getConfiguration().get(HConstants.HBASE_DIR));
    Path tableDir = HTableDescriptor.getTableDir(hbDir, table.getTableName());
    Path splitFile = new Path(tableDir, "_balancedSplit");
    FileSystem fs = FileSystem.get(table.getConfiguration());

    // get a list of daughter regions to create
    Set<Pair<BigInteger, BigInteger>> tmpRegionSet = getSplits(conf,
        tableName);
    LinkedList<Pair<byte[],byte[]>> outstanding = Lists.newLinkedList();
    int splitCount = 0;
    final int origCount = tmpRegionSet.size();

    // all splits must compact & we have 1 compact thread, so 2 split
    // requests to the same RS can stall the outstanding split queue.
    // To fix, group the regions into an RS pool and round-robin through it
    LOG.debug("Bucketing regions by regionserver...");
    TreeMap<HServerAddress, LinkedList<Pair<BigInteger, BigInteger>>>
      daughterRegions = Maps.newTreeMap();
    for (Pair<BigInteger, BigInteger> dr : tmpRegionSet) {
      HServerAddress rsLocation = table.getRegionLocation(
          convertToByte(dr.getSecond())).getServerAddress();
      if (!daughterRegions.containsKey(rsLocation)) {
        LinkedList<Pair<BigInteger, BigInteger>> entry = Lists.newLinkedList();
        daughterRegions.put(rsLocation, entry);
      }
      daughterRegions.get(rsLocation).add(dr);
    }
    LOG.debug("Done with bucketing.  Split time!");
    long startTime = System.currentTimeMillis();

    // open the split file and modify it as splits finish
    FSDataInputStream tmpIn = fs.open(splitFile);
    byte[] rawData = new byte[tmpIn.available()];
    tmpIn.readFully(rawData);
    tmpIn.close();
    FSDataOutputStream splitOut = fs.create(splitFile);
    splitOut.write(rawData);

    try {
      // *** split code ***
      while (!daughterRegions.isEmpty()) {
        LOG.debug(daughterRegions.size() + " RS have regions to splt.");

        // Get RegionServer : region count mapping
        final TreeMap<HServerAddress, Integer> rsSizes = Maps.newTreeMap();
        Map<HRegionInfo, HServerAddress> regionsInfo = table.getRegionsInfo();
        for (HServerAddress rs : regionsInfo.values()) {
          if (rsSizes.containsKey(rs)) {
            rsSizes.put(rs, rsSizes.get(rs) + 1);
          } else {
            rsSizes.put(rs, 1);
          }
        }

        // sort the RS by the number of regions they have
        List<HServerAddress> serversLeft = Lists.newArrayList(daughterRegions
            .keySet());
        Preconditions.checkState(rsSizes.keySet().containsAll(serversLeft));
        Collections.sort(serversLeft, new Comparator<HServerAddress>() {
          public int compare(HServerAddress o1, HServerAddress o2) {
            return rsSizes.get(o1).compareTo(rsSizes.get(o2));
          }
        });

        // round-robin through the RS list. Choose the lightest-loaded servers
        // first to keep the master from load-balancing regions as we split.
        for (HServerAddress rsLoc : serversLeft) {
          Pair<BigInteger, BigInteger> dr = null;

          // find a region in the RS list that hasn't been moved
          LOG.debug("Finding a region on " + rsLoc);
          LinkedList<Pair<BigInteger, BigInteger>> regionList
            = daughterRegions.get(rsLoc);
          while (!regionList.isEmpty()) {
            dr = regionList.pop();

            // get current region info
            byte[] split = convertToByte(dr.getSecond());
            HRegionLocation regionLoc = table.getRegionLocation(split);

            // if this region moved locations
            HServerAddress newRs = regionLoc.getServerAddress();
            if (newRs.compareTo(rsLoc) != 0) {
              LOG.debug("Region with " + Bytes.toStringBinary(split)
                  + " moved to " + newRs + ". Relocating...");
              // relocate it, don't use it right now
              if (!daughterRegions.containsKey(newRs)) {
                LinkedList<Pair<BigInteger, BigInteger>> entry = Lists
                    .newLinkedList();
                daughterRegions.put(newRs, entry);
              }
              daughterRegions.get(newRs).add(dr);
              dr = null;
              continue;
            }

            // make sure this region wasn't already split
            byte[] sk = regionLoc.getRegionInfo().getStartKey();
            if (sk.length != 0) {
              if (Bytes.equals(split, sk)) {
                LOG.debug("Region already split on " 
                    + Bytes.toStringBinary(split)
                    + ".  Skipping this region...");
                  dr = null;
                  continue;
              }
              byte[] start = convertToByte(dr.getFirst());
              Preconditions.checkArgument(Bytes.equals(start, sk), Bytes
                  .toStringBinary(start) + " != " + Bytes.toStringBinary(sk));
            }

            // passed all checks! found a good region
            break;
          }
          if (regionList.isEmpty()) {
            daughterRegions.remove(rsLoc);
          }
          if (dr == null) continue;

          // we have a good region, time to split!

          byte[] start = convertToByte(dr.getFirst());
          byte[] split = convertToByte(dr.getSecond());
          // request split
          LOG.debug("Splitting at " + Bytes.toString(split));
          HBaseAdmin admin = new HBaseAdmin(table.getConfiguration());
          admin.split(table.getTableName(), split);

          splitCount++;
          if (splitCount % 10 == 0) {
            long tDiff = (System.currentTimeMillis() - startTime) / splitCount;
            LOG.debug("STATUS UPDATE: " + splitCount + " / " + origCount +
                      ". Avg Time / Split = " +
                      org.apache.hadoop.util.StringUtils.formatTime(tDiff));
          }

          // if we have too many outstanding splits, wait for oldest ones to finish
          outstanding.addLast(Pair.newPair(start, split));
          while (outstanding.size() >= MAX_OUTSTANDING) {
            splitScan(outstanding, table, splitOut);
            if (outstanding.size() >= MAX_OUTSTANDING) Thread.sleep(30 * 1000);
          }
        }
      }
      while (!outstanding.isEmpty()) {
        splitScan(outstanding, table, splitOut);
        if (!outstanding.isEmpty()) Thread.sleep(30 * 1000);
      }
      LOG.debug("All regions have been sucesfully split!");
    } finally {
      long tDiff = System.currentTimeMillis() - startTime;
      LOG.debug("TOTAL TIME = " +
          org.apache.hadoop.util.StringUtils.formatTime(tDiff));
      LOG.debug("Splits = " + splitCount);
      LOG.debug("Avg Time / Split = " +
          org.apache.hadoop.util.StringUtils.formatTime(tDiff/splitCount));

      splitOut.close();
    }
    fs.delete(splitFile, false);
  }

  private static void splitScan(LinkedList<Pair<byte[], byte[]>> regionList,
      HTable table, FSDataOutputStream splitOut) throws IOException,
      InterruptedException {
    LinkedList<Pair<byte[], byte[]>> finished = Lists.newLinkedList();
    LinkedList<Pair<byte[], byte[]>> logicalSplitting = Lists.newLinkedList();
    LinkedList<Pair<byte[], byte[]>> physicalSplitting = Lists.newLinkedList();

    // get table info
    Path hbDir = new Path(table.getConfiguration().get(HConstants.HBASE_DIR));
    Path tableDir = HTableDescriptor.getTableDir(hbDir, table.getTableName());
    Path splitFile = new Path(tableDir, "_balancedSplit");
    FileSystem fs = FileSystem.get(table.getConfiguration());

    // clear the cache to make sure we have current region information
    table.clearRegionCache();

    // for every region that we haven't verified a finished split
    for (Pair<byte[], byte[]> region : regionList) {
      byte[] start = region.getFirst();
      byte[] split = region.getSecond();

      try {
        // check if one of the daughter regions has come online
        HRegionInfo dri = table.getRegionLocation(split).getRegionInfo();
        if (!Bytes.equals(dri.getStartKey(), split) || dri.isOffline()) {
          logicalSplitting.add(region);
          continue;
        }
      } catch (NoServerForRegionException nsfre) {
        // NSFRE will occur if the old META entry has no server assigned
        LOG.info(nsfre);
        logicalSplitting.add(region);
        continue;
      }

      // when a daughter region is opened, a compaction is triggered
      // wait until compaction completes for both daughter regions
      try {
        LinkedList<HRegionInfo> check = Lists.newLinkedList();
        check.add(table.getRegionLocation(start).getRegionInfo());
        check.add(table.getRegionLocation(split).getRegionInfo());
        for (HRegionInfo hri : check.toArray(new HRegionInfo[] {})) {
          boolean refFound = false;
          String startKey = Bytes.toStringBinary(hri.getStartKey());
          // check every Column Family for that region
          for (HColumnDescriptor c : hri.getTableDesc().getFamilies()) {
            Path cfDir = Store.getStoreHomedir(tableDir, hri.getEncodedName(),
                c.getName());
            if (fs.exists(cfDir)) {
              for (FileStatus file : fs.listStatus(cfDir)) {
                refFound |= StoreFile.isReference(file.getPath());
                if (refFound)
                  break;
              }
            }
            if (refFound)
              break;
          }
          // compaction is completed when all reference files are gone
          if (!refFound) {
            check.remove(hri);
          }
        }
        if (check.isEmpty()) {
          finished.add(region);
        } else {
          physicalSplitting.add(region);
        }
      } catch (NoServerForRegionException nsfre) {
        LOG.debug("No Server Exception thrown for: "
            + Bytes.toStringBinary(start));
        physicalSplitting.add(region);
        table.clearRegionCache();
      }
    }

    LOG.debug("Split Scan: " + finished.size() + " finished / "
        + logicalSplitting.size() + " split wait / " + physicalSplitting.size()
        + " reference wait");

    for (Pair<byte[], byte[]> region : finished) {
      BigInteger biStart = convertToBigInteger(region.getFirst());
      BigInteger biSplit = convertToBigInteger(region.getSecond());
      if (biSplit == BigInteger.ZERO) biSplit = MAXMD5_INT;
      LOG.debug("Finished split at " + biSplit.toString(16));
      splitOut.writeChars("- " + biStart.toString(16) + " "
          + biSplit.toString(16) + "\n");
    }
    regionList.removeAll(finished);
  }

  private static Set<Pair<BigInteger, BigInteger>> getSplits(
      Configuration conf, String tblName) throws IOException {
    HTable table = new HTable(conf, tblName);
    Path hbDir = new Path(table.getConfiguration().get(HConstants.HBASE_DIR));
    Path tableDir = HTableDescriptor.getTableDir(hbDir, table.getTableName());
    Path splitFile = new Path(tableDir, "_balancedSplit");
    FileSystem fs = FileSystem.get(table.getConfiguration());

    Set<Pair<BigInteger, BigInteger>> daughterRegions = Sets.newHashSet();

    // does a split file exist?
    if (!fs.exists(splitFile)) {
      // NO = fresh start. calculate splits to make
      LOG.debug("No _balancedSplit file.  Calculating splits...");

      // query meta for all regions in the table
      Set<Pair<BigInteger, BigInteger>> rows = Sets.newHashSet();
      Pair<byte[][],byte[][]> tmp = table.getStartEndKeys();
      byte[][] s = tmp.getFirst(), e = tmp.getSecond();
      Preconditions.checkArgument(s.length == e.length,
        "Start and End rows should be equivalent");

      // convert to the BigInteger format we used for original splits
      for (int i = 0; i < tmp.getFirst().length; ++i) {
        BigInteger start = convertToBigInteger(s[i]);
        BigInteger end = convertToBigInteger(e[i]);
        if (end == BigInteger.ZERO) {
          end = MAXMD5_INT;
        }
        rows.add(Pair.newPair(start, end));
      }
      LOG.debug("Table " + tblName + " has " + rows.size() +
                " regions that will be split.");

      // prepare the split file
      Path tmpFile = new Path(tableDir, "_balancedSplit_prepare");
      FSDataOutputStream tmpOut = fs.create(tmpFile);

      // calculate all the splits == [daughterRegions] = [(start, splitPoint)]
      for (Pair<BigInteger, BigInteger> r : rows) {
        BigInteger start = r.getFirst();
        BigInteger splitPoint = split2(r.getFirst(), r.getSecond());
        daughterRegions.add(Pair.newPair(start, splitPoint));
        LOG.debug("Will Split [" + r.getFirst().toString(16) + ", " +
          r.getSecond().toString(16) + ") at " + splitPoint.toString(16));
        tmpOut.writeChars("+ " + start.toString(16) +
                          " " + splitPoint.toString(16) + "\n");
      }
      tmpOut.close();
      fs.rename(tmpFile, splitFile);
    } else {
      LOG.debug("_balancedSplit file found. Replay log to restore state...");
      DistributedFileSystem dfs = (DistributedFileSystem)fs;
      dfs.recoverLease(splitFile);

      // parse split file and process remaining splits
      FSDataInputStream tmpIn = fs.open(splitFile);
      StringBuilder sb = new StringBuilder(tmpIn.available());
      while (tmpIn.available() > 0) {
        sb.append(tmpIn.readChar());
      }
      tmpIn.close();
      for (String line : sb.toString().split("\n")) {
        String[] cmd = line.split(" ");
        Preconditions.checkArgument(3 == cmd.length);
        BigInteger a = new BigInteger(cmd[1], 16);
        BigInteger b = new BigInteger(cmd[2], 16);
        Pair<BigInteger, BigInteger> r = Pair.newPair(a,b);
        if (cmd[0].equals("+")) {
          LOG.debug("Adding: " + a.toString(16) + "," + b.toString(16));
          daughterRegions.add(r);
        } else {
          LOG.debug("Removing: " + a.toString(16) + "," + b.toString(16));
          Preconditions.checkArgument(cmd[0].equals("-"),
                                      "Unknown option: " + cmd[0]);
          Preconditions.checkState(daughterRegions.contains(r),
                                   "Missing row: " + r);
          daughterRegions.remove(r);
        }
      }
      LOG.debug("Done reading. " + daughterRegions.size() + " regions left.");
    }
    return daughterRegions;
  }
}
