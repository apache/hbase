package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestFlushTableProcedureWithDoNotSupportFlushTableMaster
    extends TestFlushTableProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFlushTableProcedureWithDoNotSupportFlushTableMaster.class);

  @Override
  protected void addConfiguration(Configuration config) {
    super.addConfiguration(config);
    config.set(HConstants.MASTER_IMPL, DoNotSupportFlushTableMaster.class.getName());
  }

  @Test
  public void testFlushFallback() throws IOException {
    assertTableMemStoreNotEmpty();
    TEST_UTIL.getAdmin().flush(TABLE_NAME);
    assertTableMemStoreEmpty();
  }

  public static final class DoNotSupportFlushTableMaster extends HMaster {

    public DoNotSupportFlushTableMaster(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    public long flushTable(TableName tableName, byte[] columnFamily,
        long nonceGroup, long nonce) throws IOException {
      throw new DoNotRetryIOException("UnsupportedOperation: flushTable");
    }
  }
}
