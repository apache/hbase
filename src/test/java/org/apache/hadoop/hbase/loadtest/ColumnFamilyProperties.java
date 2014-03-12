package org.apache.hadoop.hbase.loadtest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ColumnFamilyProperties {

  private static final Log LOG = LogFactory.getLog(ColumnFamilyProperties.class);

  public static int familyIndex = 1;
  
  public String familyName;
  public int startTimestamp;
  public int endTimestamp;
  public int minColDataSize;
  public int maxColDataSize;
  public int minColsPerKey;
  public int maxColsPerKey;
  public int maxVersions;
  public String filterType;
  public String bloomType;
  public String compressionType;

  public void print() {
    LOG.info("\n\nColumnName: " + familyName);
    LOG.info("Timestamp Range: " + startTimestamp + "..." + endTimestamp);
    LOG.info("Number of Columns/Key:" + minColsPerKey + "...." + maxColsPerKey);
    LOG.info("Data Size/Column:" + minColDataSize + "..." + maxColDataSize);
    LOG.info("Max Versions: " + maxVersions);
    LOG.info("Filter type: " + filterType);
    LOG.info("Bloom type: " + bloomType);
    LOG.info("Compression type: " + compressionType +  "\n\n");
  }

  public static final String defaultColumnProperties = 
    "\nReaderThreads=10" +
    "\nWriterThreads=10" +
    "\nStartKey=1" +
    "\nEndKey=1000000" +
    "\nNumberOfFamilies=3" +
    "\nVerifyPercent=2" +
    "\nVerbose=true" +
    "\nClusterTestTime=120" +
    "\nBulkLoad=true" +
    "\nRegionsPerServer=5" +
    "\n" +
    "\nCF1_Name=ActionLog" +
    "\nCF1_StartTimestamp=10" +
    "\nCF1_EndTimestamp=1000" +
    "\nCF1_MinDataSize=1" +
    "\nCF1_MaxDataSize=10" +
    "\nCF1_MinColsPerKey=1" +
    "\nCF1_MaxColsPerKey=1" +
    "\nCF1_MaxVersions=2147483647" +
    "\nCF1_FilterType=Timestamps" +
    "\nCF1_BloomType=Row" +
    "\nCF1_CompressionType=None" +
    "\n" +
    "\nCF2_Name=Snapshot" +
    "\nCF2_StartTimestamp=10" +
    "\nCF2_EndTimestamp=20" +
    "\nCF2_MinDataSize=500" +
    "\nCF2_MaxDataSize=1000000" +
    "\nCF2_MinColsPerKey=1" +
    "\nCF2_MaxColsPerKey=1" +
    "\nCF2_MaxVersions=1" +
    "\nCF2_FilterType=None" +
    "\nCF2_BloomType=None" +
    "\nCF2_CompressionType=LZO" +
    "\n" +
    "\nCF3_Name=IndexSnapshot" +
    "\nCF3_StartTimestamp=20" +
    "\nCF3_EndTimestamp=100" +
    "\nCF3_MinDataSize=1" +
    "\nCF3_MaxDataSize=10" +
    "\nCF3_MinColsPerKey=1" +
    "\nCF3_MaxColsPerKey=1000" +
    "\nCF3_MaxVersions=1" +
    "\nCF3_FilterType=ColumnPrefix" +
    "\nCF3_BloomType=RowCol" +
    "\nCF3_CompressionType=GZ" +
    "";
}

/** 
If creating an external file, you should use the following as a starting point and
make whatever changes you want. It would be best not to omit any fields.

ReaderThreads=10
WriterThreads=10
StartKey=1
EndKey=1000000
NumberOfFamilies=3
VerifyPercent=2
Verbose=true
ClusterTestTime=120
BulkLoad=true
RegionsPerServer=5

CF1_Name=ActionLog
CF1_StartTimestamp=10
CF1_EndTimestamp=1000
CF1_MinDataSize=1
CF1_MaxDataSize=10
CF1_MinColsPerKey=1
CF1_MaxColsPerKey=1
CF1_MaxVersions=2147483647
CF1_FilterType=Timestamps
CF1_BloomType=Row
CF1_CompressionType=None

CF2_Name=Snapshot
CF2_StartTimestamp=10
CF2_EndTimestamp=20
CF2_MinDataSize=500
CF2_MaxDataSize=1000000
CF2_MinColsPerKey=1
CF2_MaxColsPerKey=1
CF2_MaxVersions=1
CF2_FilterType=None
CF2_BloomType=None
CF2_CompressionType=LZO

CF3_Name=IndexSnapshot
CF3_StartTimestamp=20
CF3_EndTimestamp=100
CF3_MinDataSize=1
CF3_MaxDataSize=10
CF3_MinColsPerKey=1
CF3_MaxColsPerKey=1000
CF3_MaxVersions=1
CF3_FilterType=ColumnPrefix
CF3_BloomType=RowCol
CF3_CompressionType=GZ

*
*
*/