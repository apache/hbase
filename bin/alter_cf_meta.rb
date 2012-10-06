# This script can be used to update existing CFs or add new CFs to META for a
# certain table. It takes as arguments the table name, followed by the set of
# changes to the CFs (similar to an 'alter table' syntax). The changes will
# not take affect until hbase is stopped and started, since only META is
# changed and not the in-memory state.
# Example usage: bin/hbase org.jruby.Main alter_cf_meta.rb test1 "{NAME =>
# 'actions', BLOCKCACHE => 'true'}" "{NAME => 'thread', VERSIONS => '1'}"

include Java
include_class('java.lang.Integer') {|package,name| "J#{name}" }
include_class('java.lang.Long') {|package,name| "J#{name}" }
include_class('java.lang.Boolean') {|package,name| "J#{name}" }
include_class('java.lang.Float') {|package,name| "J#{name}" }

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.hbase.io.hfile.Compression
import org.apache.hadoop.hbase.regionserver.StoreFile

NAME = HConstants::NAME
VERSIONS = HConstants::VERSIONS
IN_MEMORY = HConstants::IN_MEMORY
BLOCKCACHE = HColumnDescriptor::BLOCKCACHE
BLOOMFILTER = HColumnDescriptor::BLOOMFILTER
BLOOMFILTER_ERRORRATE = HColumnDescriptor::BLOOMFILTER_ERRORRATE
REPLICATION_SCOPE = HColumnDescriptor::REPLICATION_SCOPE
TTL = HColumnDescriptor::TTL
FLASHBACK_QUERY_LIMIT = HColumnDescriptor::FLASHBACK_QUERY_LIMIT
COMPRESSION = HColumnDescriptor::COMPRESSION
BLOCKSIZE = HColumnDescriptor::BLOCKSIZE

# Return a new HColumnDescriptor made of passed args
def getHCD(arg, htd)
  name = arg[NAME]

  family = htd.getFamily(name.to_java_bytes)
  # create it if it's a new family
  family ||= HColumnDescriptor.new(name.to_java_bytes)

  family.setBlockCacheEnabled(JBoolean.valueOf(arg[BLOCKCACHE])) if arg.include?(BLOCKCACHE)
  family.setBloomFilterType(StoreFile::BloomType.valueOf(arg[BLOOMFILTER])) if arg.include?(BLOOMFILTER)
  family.setBloomFilterErrorRate(JFloat.valueOf(arg[BLOOMFILTER_ERRORRATE])) if arg.include?(BLOOMFILTER_ERRORRATE)
  family.setScope(JInteger.valueOf(arg[REPLICATION_SCOPE])) if arg.include?(REPLICATION_SCOPE)
  family.setInMemory(JBoolean.valueOf(arg[IN_MEMORY])) if arg.include?(IN_MEMORY)
  family.setTimeToLive(JInteger.valueOf(arg[TTL])) if arg.include?(TTL)
  family.setFlashBackQueryLimit(JInteger.valueOf(arg[FLASHBACK_QUERY_LIMIT])) if arg.include?(FLASHBACK_QUERY_LIMIT)
  family.setCompressionType(Compression::Algorithm.valueOf(arg[COMPRESSION])) if arg.include?(COMPRESSION)
  family.setBlocksize(JInteger.valueOf(arg[BLOCKSIZE])) if arg.include?(BLOCKSIZE)
  family.setMaxVersions(JInteger.valueOf(arg[VERSIONS])) if arg.include?(VERSIONS)

  return family
end

# Name of this script
SCRIPT_NAME = "alter_cf_meta"

# Print usage for this script
def usage
  puts 'Usage: %s.rb TABLE "{NAME=>name}"' % SCRIPT_NAME
  exit!
end

# Check arguments
if ARGV.size < 2
  usage
end

cfs = []
tableName = ARGV[0]
for i in 1..(ARGV.length-1)
  arg = ARGV[i]
  cfs[i-1] = eval(arg)
end


LOG = LogFactory.getLog(SCRIPT_NAME)
LOG.info("Updating " + cfs.length.to_s + " CFs to table " + tableName)

c = HBaseConfiguration.new()
c.set("fs.default.name", c.get(HConstants::HBASE_DIR))

LOG.info("Scanning META for table: " + tableName)
metaTable = HTable.new(c, HConstants::META_TABLE_NAME)
tableNameMetaPrefix = tableName + HConstants::META_ROW_DELIMITER.chr
scan = Scan.new(Bytes.toBytes(tableNameMetaPrefix + HConstants::META_ROW_DELIMITER.chr))
scanner = metaTable.getScanner(scan)
first = true

while (result = scanner.next())
  rowid = Bytes.toString(result.getRow())
  rowidStr = java.lang.String.new(rowid)
  if not rowidStr.startsWith(tableNameMetaPrefix)
    # Gone too far, break
    break
  end

  hri = Writables.getHRegionInfo(result.getValue(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER))
  LOG.info("Old HRI: " + hri.toString())

  if first
    htd = hri.getTableDesc()
    cfs.each do |cf|
      hcd = getHCD(cf, htd)
      htd.addFamily(hcd)
    end
    first = false
  end

  newHRI = HRegionInfo.new(htd, hri.getStartKey(), hri.getEndKey(), hri.isSplit(), hri.getRegionId())
  LOG.info("New HRI: " + newHRI.toString())

  p = Put.new(result.getRow())
  p.add(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER, Writables.getBytes(newHRI))
  metaTable.put(p)
end
scanner.close()
LOG.info("Updated " + cfs.length.to_s + " CFs to table " + tableName)
