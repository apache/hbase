package org.apache.hadoop.hbase.mapred;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableSnapshotInputFormatImpl;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatImpl;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.List;

public class MultiTableSnapshotInputFormat extends TableSnapshotInputFormat implements InputFormat<ImmutableBytesWritable, Result> {

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    List<TableSnapshotInputFormatImpl.InputSplit> splits =
        MultiTableSnapshotInputFormatImpl.getSplits(job);
    InputSplit[] results = new InputSplit[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      results[i] = new TableSnapshotRegionSplit(splits.get(i));
    }
    return results;
  }

  @Override
  public RecordReader<ImmutableBytesWritable, Result>
  getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return new TableSnapshotRecordReader((TableSnapshotRegionSplit) split, job);
  }

}
