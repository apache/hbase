package org.apache.hadoop.hbase.mapreduce;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;

public class MultiTableSnapshotInputFormat extends
    InputFormat<ImmutableBytesWritable, Result> {

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    List<TableSnapshotInputFormatImpl.InputSplit> splits = MultiTableSnapshotInputFormatImpl.getSplits(jobContext.getConfiguration());
    List<InputSplit> rtn = Lists.newArrayListWithCapacity(splits.size());

    for (TableSnapshotInputFormatImpl.InputSplit split : splits) {
      rtn.add(new TableSnapshotInputFormat.TableSnapshotRegionSplit(split));
    }

    return rtn;
  }

  @Override
  public RecordReader<ImmutableBytesWritable, Result> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new TableSnapshotInputFormat.TableSnapshotRegionRecordReader();
  }
}
