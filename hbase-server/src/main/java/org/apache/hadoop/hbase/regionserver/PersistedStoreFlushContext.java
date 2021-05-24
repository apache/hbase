package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@InterfaceAudience.Private
public class PersistedStoreFlushContext extends DefaultStoreFlushContext {

  private static final Logger LOG = LoggerFactory.getLogger(PersistedStoreFlushContext.class);

  public PersistedStoreFlushContext(HStore store, Long cacheFlushSeqNum,
    FlushLifeCycleTracker tracker) {
    super.init(store, cacheFlushSeqNum, tracker);
  }


  @Override
  public boolean commit(MonitoredTask status) throws IOException {
    return super.commit(p -> {
      status.setStatus("Flushing " + this.store + ": reopening file created directly in family dir");
      HStoreFile sf = store.createStoreFileAndReader(p);

      StoreFileReader r = sf.getReader();
      this.store.storeSize.addAndGet(r.length());
      this.store.totalUncompressedBytes.addAndGet(r.getTotalUncompressedBytes());

      if (LOG.isInfoEnabled()) {
        LOG.info("Added " + sf + ", entries=" + r.getEntries() +
          ", sequenceid=" + cacheFlushSeqNum +
          ", filesize=" + StringUtils.TraditionalBinaryPrefix.long2String(r.length(), "", 1));
      }
      return sf;
    });
  }

}
