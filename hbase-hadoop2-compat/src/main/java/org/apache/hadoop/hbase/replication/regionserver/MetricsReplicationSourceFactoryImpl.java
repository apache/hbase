package org.apache.hadoop.hbase.replication.regionserver;

public class MetricsReplicationSourceFactoryImpl implements MetricsReplicationSourceFactory {

  private static enum SourceHolder {
    INSTANCE;
    final MetricsReplicationSourceImpl source = new MetricsReplicationSourceImpl();
  }

  @Override public MetricsReplicationSinkSource getSink() {
    return new MetricsReplicationSinkSourceImpl(SourceHolder.INSTANCE.source);
  }

  @Override public MetricsReplicationSourceSource getSource(String id) {
    return new MetricsReplicationSourceSourceImpl(SourceHolder.INSTANCE.source, id);
  }

  @Override public MetricsReplicationSourceSource getGlobalSource() {
    return new MetricsReplicationGlobalSourceSource(SourceHolder.INSTANCE.source);
  }
}
