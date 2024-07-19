package org.apache.hadoop.hbase.client;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;
import org.apache.hbase.thirdparty.com.google.gson.JsonSerializer;


@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RegionHistLogEntry extends LogEntry {

  private static final Gson GSON = GsonUtil.createGson().setPrettyPrinting()
    .registerTypeAdapter(RegionHistLogEntry.class,
      (JsonSerializer<RegionHistLogEntry>) (regionHistLogEntry, type,
        jsonSerializationContext) -> {
        Gson gson = new Gson();
        JsonObject jsonObj = (JsonObject) gson.toJsonTree(regionHistLogEntry);
        return jsonObj;
      }).create();

  private final String hostName;
  private final String regionName;
  private final String tableName;
  private final String eventType;
  private final long eventTimestamp;
  private final long pid;
  private final long ppid;

  public RegionHistLogEntry(String hostName, String regionName, String tableName, String eventType,
    long eventTimestamp, long pid, long ppid) {
    this.hostName = hostName;
    this.regionName = regionName;
    this.tableName = tableName;
    this.eventType = eventType;
    this.eventTimestamp = eventTimestamp;
    this.pid = pid;
    this.ppid = ppid;
  }

  // Getters for all fields
  public String getHostName() {
    return hostName;
  }

  public String getRegionName() {
    return regionName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getEventType() {
    return eventType;
  }

  public long getEventTimestamp() {
    return eventTimestamp;
  }

  public long getPid() {
    return pid;
  }

  public long getPpid() {
    return ppid;
  }


  public static class RegionHistorianLogEntryBuilder {
    private String hostName;
    private String regionName;
    private String tableName;
    private String eventType;
    private long eventTimestamp;
    private long pid;
    private long ppid;
    private Long duration;

    public RegionHistorianLogEntryBuilder setHostName(String hostName) {
      this.hostName = hostName;
      return this;
    }

    public RegionHistorianLogEntryBuilder setRegionName(String regionName) {
      this.regionName = regionName;
      return this;
    }

    public RegionHistorianLogEntryBuilder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public RegionHistorianLogEntryBuilder setEventType(String eventType) {
      this.eventType = eventType;
      return this;
    }

    public RegionHistorianLogEntryBuilder setEventTimestamp(long eventTimestamp) {
      this.eventTimestamp = eventTimestamp;
      return this;
    }

    public RegionHistorianLogEntryBuilder setPid(long pid) {
      this.pid = pid;
      return this;
    }

    public RegionHistorianLogEntryBuilder setPpid(long ppid) {
      this.ppid = ppid;
      return this;
    }

    public RegionHistLogEntry build() {
      return new RegionHistLogEntry(hostName, regionName, tableName, eventType, eventTimestamp, pid, ppid);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RegionHistLogEntry that = (RegionHistLogEntry) o;

    return new EqualsBuilder().append(eventTimestamp, that.eventTimestamp)
      .append(pid, that.pid).append(ppid, that.ppid)
      .append(hostName, that.hostName)
      .append(regionName, that.regionName)
      .append(tableName, that.tableName)
      .append(eventType, that.eventType).isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(hostName)
      .append(regionName)
      .append(tableName)
      .append(eventType)
      .append(eventTimestamp)
      .append(pid)
      .append(ppid)
      .toHashCode();
  }

  @Override
  public String toJsonPrettyPrint() {
    return GSON.toJson(this);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
      .append("hostName", hostName)
      .append("regionName", regionName)
      .append("tableName", tableName)
      .append("eventType", eventType)
      .append("eventTimestamp", eventTimestamp)
      .append("pid", pid)
      .append("ppid", ppid)
      .toString();
  }
}

