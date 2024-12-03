package org.apache.hadoop.hbase.backup.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@InterfaceAudience.Private
public class ContinuousBackupReplicationEndpoint extends BaseReplicationEndpoint {
  private static final Logger LOG = LoggerFactory.getLogger(ContinuousBackupReplicationEndpoint.class);
  public static final String CONF_PEER_UUID = "hbase.backup.wal.replication.peerUUID";
  private ContinuousBackupManager continuousBackupManager;
  private UUID peerUUID;

  @Override
  public void init(Context context) throws IOException {
    super.init(context);
    LOG.info("{} Initializing ContinuousBackupReplicationEndpoint.", Utils.logPeerId(ctx.getPeerId()));
    Configuration peerConf = this.ctx.getConfiguration();

    setPeerUUID(peerConf);

    Configuration conf = HBaseConfiguration.create(peerConf);

    try {
      continuousBackupManager = new ContinuousBackupManager(this.ctx.getPeerId(), conf);
      LOG.info("{} ContinuousBackupManager initialized successfully.", Utils.logPeerId(ctx.getPeerId()));
    } catch (BackupConfigurationException e) {
      LOG.error("{} Failed to initialize ContinuousBackupManager due to configuration issues.", Utils.logPeerId(ctx.getPeerId()), e);
      throw new IOException("Failed to initialize ContinuousBackupManager", e);
    }
  }

  @Override
  public UUID getPeerUUID() {
    return peerUUID;
  }

  @Override
  public void start() {
    LOG.info("{} Starting ContinuousBackupReplicationEndpoint...", Utils.logPeerId(ctx.getPeerId()));
    startAsync();
  }

  @Override
  protected void doStart() {
    LOG.info("{} ContinuousBackupReplicationEndpoint started successfully.", Utils.logPeerId(ctx.getPeerId()));
    notifyStarted();
  }

  @Override
  public boolean replicate(ReplicateContext replicateContext) {
    final List<WAL.Entry> entries = replicateContext.getEntries();
    if (entries.isEmpty()) {
      LOG.debug("{} No WAL entries to backup.", Utils.logPeerId(ctx.getPeerId()));
      return true;
    }

    LOG.info("{} Received {} WAL entries for backup.", Utils.logPeerId(ctx.getPeerId()), entries.size());

    Map<TableName, List<WAL.Entry>> tableToEntriesMap = new HashMap<>();
    for (WAL.Entry entry : entries) {
      TableName tableName = entry.getKey().getTableName();
      tableToEntriesMap.computeIfAbsent(tableName, key -> new ArrayList<>()).add(entry);
    }
    LOG.debug("{} WAL entries grouped by table: {}", Utils.logPeerId(ctx.getPeerId()), tableToEntriesMap.keySet());

    try {
      LOG.debug("{} Starting backup for {} tables.", Utils.logPeerId(ctx.getPeerId()), tableToEntriesMap.size());
      continuousBackupManager.backup(tableToEntriesMap);
      LOG.info("{} Backup completed successfully for all tables.", Utils.logPeerId(ctx.getPeerId()));
    } catch (IOException e) {
      LOG.error("{} Backup failed for tables: {}. Error details: {}", Utils.logPeerId(ctx.getPeerId()), tableToEntriesMap.keySet(), e.getMessage(), e);
      return false;
    }

    return true;
  }

  @Override
  public void stop() {
    LOG.info("{} Stopping ContinuousBackupReplicationEndpoint...", Utils.logPeerId(ctx.getPeerId()));
    stopAsync();
  }

  @Override
  protected void doStop() {
    if (continuousBackupManager != null) {
      LOG.info("{} Closing ContinuousBackupManager.", Utils.logPeerId(ctx.getPeerId()));
      continuousBackupManager.close();
    }
    LOG.info("{} ContinuousBackupReplicationEndpoint stopped successfully.", Utils.logPeerId(ctx.getPeerId()));
    notifyStopped();
  }

  private void setPeerUUID(Configuration conf) throws IOException {
    String peerUUIDStr = conf.get(CONF_PEER_UUID);
    if (peerUUIDStr == null || peerUUIDStr.isEmpty()) {
      LOG.error("{} Peer UUID is missing. Please specify it with the {} configuration.", Utils.logPeerId(ctx.getPeerId()), CONF_PEER_UUID);
      throw new IOException("Peer UUID not specified in configuration");
    }
    try {
      peerUUID = UUID.fromString(peerUUIDStr);
      LOG.info("{} Peer UUID set to {}", Utils.logPeerId(ctx.getPeerId()), peerUUID);
    } catch (IllegalArgumentException e) {
      LOG.error("{} Invalid Peer UUID format: {}", Utils.logPeerId(ctx.getPeerId()), peerUUIDStr, e);
      throw new IOException("Invalid Peer UUID format", e);
    }
  }
}
