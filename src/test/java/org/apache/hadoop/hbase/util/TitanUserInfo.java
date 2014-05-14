package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.loadtest.RegionSplitter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.experimental.categories.Category;

/**
 * Given a Titan userid, figure out where the user is located on a cluster. This
 * is useful for taking userids, which appserver people use, and translating
 * them to regions & current RS for diagnosis.
 */
@Category(SmallTests.class)
public class TitanUserInfo {
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: TitanUserInfo <id@facebook.com> [TABLE_NAME|stop]");
    }
    
    // turn logging level to error so we don't have verbose output
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN);
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.WARN);
    
    String userid = args[0].trim();
    byte[] row = RegionSplitter.getHBaseKeyFromEmail(userid);
    
    System.out.println("userid = " + userid);
    System.out.println("HBase row = " + Bytes.toStringBinary(row));

    Configuration conf = HBaseConfiguration.create();
    List<String> tableNames = new ArrayList<String>();
    String filter = (args.length >= 2) ? args[1].trim() : "MailBox";

    if (filter.toLowerCase().equals("stop")) {
      return;
    }

    HBaseAdmin hba = new HBaseAdmin(conf);
    for (HTableDescriptor htd : hba.listTables()) {
      String curName = Bytes.toString(htd.getName());
      if (curName.startsWith(filter)) {
        tableNames.add(curName);
      }
    }
    
    for (String curName : tableNames) {
      // get region & regionserver given row + table
      HTable table = new HTable(curName);
      HRegionLocation loc = table.getRegionLocation(row);
      System.out.println(curName + " region = " + loc.getRegionInfo().getRegionNameAsString());
      System.out.println(curName + " cur server = " + loc.getServerAddress());
    }
  }
}
