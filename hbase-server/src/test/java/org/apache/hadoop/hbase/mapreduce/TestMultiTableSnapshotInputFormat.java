package org.apache.hadoop.hbase.mapreduce;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.BeforeClass;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class TestMultiTableSnapshotInputFormat extends MultiTableInputFormatTestBase {

  @BeforeClass
  public static void setUpSnapshots() throws Exception {

    // take a snapshot of every table we have.
    for (String tableName : TABLES) {
      SnapshotTestingUtils.createSnapshotAndValidate(
          TEST_UTIL.getHBaseAdmin(), TableName.valueOf(tableName),
          ImmutableList.of(MultiTableInputFormatTestBase.INPUT_FAMILY), null,
          snapshotNameForTable(tableName), FSUtils.getRootDir(TEST_UTIL.getConfiguration()), TEST_UTIL.getTestFileSystem(), true);
    }

  }

  @Override
  protected void initJob(List<Scan> scans, Job job) throws IOException {
    TableMapReduceUtil.initMultiTableSnapshotMapperJob(
        getSnapshotScanMapping(scans), ScanMapper.class,
        ImmutableBytesWritable.class, ImmutableBytesWritable.class, job,
        true, new Path("/tmp")
    );
  }

  private Map<String, Collection<Scan>> getSnapshotScanMapping(final List<Scan> scans) {
    return Multimaps.index(scans, new Function<Scan, String>() {
      @Nullable
      @Override
      public String apply(Scan input) {
        return snapshotNameForTable(Bytes.toStringBinary(input.getAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME)));
      }
    }).asMap();
  }

  public static String snapshotNameForTable(String tableName) {
    return tableName + "_snapshot";
  }
}
