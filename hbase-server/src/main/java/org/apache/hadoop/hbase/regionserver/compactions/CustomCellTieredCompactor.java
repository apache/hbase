package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.yetus.audience.InterfaceAudience;
import java.util.List;

/**
 * An extension of DateTieredCompactor, overriding the decorateCells method to allow for custom
 * values to be used for the different file tiers during compaction.
 */
@InterfaceAudience.Private
public class CustomCellTieredCompactor extends DateTieredCompactor {
  public CustomCellTieredCompactor(Configuration conf, HStore store) {
    super(conf, store);
  }

  @Override
  protected void decorateCells(List<ExtendedCell> cells) {
    //TODO
  }

}
