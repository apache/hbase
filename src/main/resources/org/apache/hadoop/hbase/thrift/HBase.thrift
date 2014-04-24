namespace java.swift org.apache.hadoop.hbase
namespace cpp facebook.hbase.hbcpp
namespace php hbase


enum Type {
  INTEGER, LIST_OF_RESULTS, EXCEPTION
}

enum HFileStat {
  KEYVALUECOUNT
}

struct KeyValue {
  1: binary bytes;
}

struct Put {
  1: binary row;
  2: i64 ts;
  3: map<binary, list<KeyValue>> familyMap;
  4: i64 lockId;
  5: bool writeToWAL;
}

struct TimeRange {
  1: bool allTime;
  2: i64 minStamp;
  3: i64 maxStamp;
}

struct TFilter {
  1: binary filterBytes;
}

struct Get {
  1: binary row;
  2: i64 lockId;
  3: i32 maxVersions;
  4: i32 storeLimit;
  5: i32 storeOffset;
  6: TimeRange tr;
  7: map<binary, set<binary>> familyMap;
  8: i64 effectiveTS;
  9: TFilter tFilter;
}

struct MultiPut {
  1: map<binary, list<Put>> putsSerial;
}

struct Delete {
  1: binary row;
  2: i64 timeStamp;
  3: map<binary, list<KeyValue>> familyMapSerial;
  4: i64 lockId;
  5: bool writeToWAL;
}

struct Scan {
  1: binary startRow;
  2: binary stopRow;
  3: i32 maxVersions;
  4: i32 batch;
  5: i32 caching;
  6: i32 storeLimit;
  7: i32 storeOffset;
  8: bool serverPrefetching;
  9: i32 maxResponseSize;
  10: bool partialRow;
  11: bool cacheBlocks;
  12: TimeRange tr;
  13: map<binary, set<binary>> familyMap;
  14: i64 effectiveTS;
  15: i32 currentPartialResponseSize;
  16: TFilter tFilter;
  17: bool preloadBlocks;
}

struct HColumnDescriptor {
  1: binary name;
  2: map<binary, binary> valuesMap;
}

struct HTableDescriptor {
  1: binary name;
  2: list<HColumnDescriptor> families;
  3: map<binary, binary> values;
}

struct HRegionInfo {
  1: HTableDescriptor tableDesc;
  2: binary startKey;
  3: binary endKey;
  4: bool split;
  5: i64 regionid;
  6: binary splitPoint;
  7: bool offline;
}

struct MultiPutResponse {
  1: map<binary, i32> answers;
}

struct Result {
  1: list<KeyValue> kvs;
}

struct HServerAddress {
  1: string bindAddress;
  2: i32 port;
}

struct RegionLoad {
  1: binary name;
  2: i32 stores;
  3: i32 storefiles;
  4: i32 storefileSizeMB;
  5: i32 memstoreSizeMB;
  6: i32 storefileIndexSizeMB;
  7: i32 rootIndexSizeKB;
  8: i32 totalStaticIndexSizeKB;
  9: i32 totalStaticBloomSizeKB;
}

struct HServerLoad {
  1: i32 numberOfRegions;
  2: i32 numberOfRequests;
  3: i32 usedHeapMB;
  4: i32 maxHeapMB;
  5: list<RegionLoad> regionLoad;
  6: i64 lastLoadRefreshTime;
  7: i64 expireAfter;
}

struct HServerInfo {
  1: HServerAddress serverAddress;
  2: i64 startCode;
  3: string hostname;
  4: map<binary, i64> flushedSequenceIdByRegion;
  5: bool sendSequenceIds;
  6: string cachedHostnamePort;
  7: HServerLoad load;
}

exception ThriftHBaseException {
  1: string message;
  2: string exceptionClass;
  3: binary serializedServerJavaEx;
}

struct MultiAction {
  1: map<binary, list<Get>> gets;
  2: map<binary, list<Put>> puts;
  3: map<binary, list<Delete>> deletes;
}

struct IntegerOrResultOrException {
  1: i32 integer;
  2: ThriftHBaseException ex;
  3: list<Result> results;
  4: Type type;
}

struct TMultiResponse {
  1: map<binary, IntegerOrResultOrException> resultsForGet;
  2: map<binary, IntegerOrResultOrException> resultsForPut;
  3: map<binary, IntegerOrResultOrException> resultsForDelete;
}

struct TRowMutations {
  1: binary row;
  2: list<Put> puts;
  3: list<Delete> deletes;
}

struct AssignmentPlan {
  1: map<HRegionInfo, list<HServerAddress>> assignmentMap;
}

struct RowLock {
  1: binary row;
  2: i64 lockId;
}

struct Bucket {
  1: binary startRow;
  2: binary endRow;
  3: double numRows;
  4: map<HFileStat, double> hfileStats;
}

struct HRegionLocation {
  1: HRegionInfo regionInfo;
  2: HServerAddress serverAddress;
  3: i64 serverStartCode;
}

struct ScannerResult {
  1: bool EOS;
  2: bool EOR;
  3: list<Result> result;
  4: i64 ID;
}

service ThriftHRegionInterface {
  void bulkLoadHFile(1: string hfilePath, 2: binary regionName, 3: binary familyName) throws (1: ThriftHBaseException ex1);
  void bulkLoadHFileSeqNum(1: string hfilePath, 2: binary regionName, 3: binary familyName, 4: bool assignSeqNum) throws (1: ThriftHBaseException ex1);
  binary callEndpoint(1: string epName, 2: string methodName, 3: list<binary> params, 4: binary regionName, 5: binary startRow, 6: binary stopRow) throws (1: ThriftHBaseException ex1);
  bool checkAndDelete(1: binary regionName, 2: binary row, 3: binary family, 4: binary qualifier, 5: binary value, 6: Delete deleteArg) throws (1: ThriftHBaseException ex1);
  bool checkAndPut(1: binary regionName, 2: binary row, 3: binary family, 4: binary qualifier, 5: binary value, 6: Put put) throws (1: ThriftHBaseException ex1);
  void close(1: i64 scannerId) throws (1: ThriftHBaseException ex1);
  void closeRegion(1: HRegionInfo hri, 2: bool reportWhenCompleted) throws (1: ThriftHBaseException ex1);
  void deleteAsync(1: binary regionName, 2: Delete deleteArg) throws (1: ThriftHBaseException ex1);
  bool exists(1: binary regionName, 2: Get get) throws (1: ThriftHBaseException ex1);
  void flushRegion(1: binary regionName) throws (1: ThriftHBaseException ex1);
  void flushRegionIfOlderThanTS(1: binary regionName, 2: i64 ifOlderThanTS) throws (1: ThriftHBaseException ex1);
  Result getAsync(1: binary regionName, 2: Get get) throws (1: ThriftHBaseException ex1);
  Result getClosestRowBefore(1: binary regionName, 2: binary row, 3: binary family) throws (1: ThriftHBaseException ex1);
  Result getClosestRowBeforeAsync(1: binary regionName, 2: binary row, 3: binary family) throws (1: ThriftHBaseException ex1);
  string getConfProperty(1: string arg0) throws (1: ThriftHBaseException ex1);
  i64 getCurrentTimeMillis();
  list<string> getHLogsList(1: bool rollCurrentHLog) throws (1: ThriftHBaseException ex1);
  HServerInfo getHServerInfo() throws (1: ThriftHBaseException ex1);
  list<Bucket> getHistogram(1: binary arg0) throws (1: ThriftHBaseException ex1);
  list<Bucket> getHistogramForStore(1: binary arg0, 2: binary arg1) throws (1: ThriftHBaseException ex1);
  list<list<Bucket>> getHistograms(1: list<binary> arg0) throws (1: ThriftHBaseException ex1);
  i64 getLastFlushTime(1: binary regionName);
  map<binary, i64> getLastFlushTimes();
  HRegionLocation getLocation(1: binary tableName, 2: binary row, 3: bool reload) throws (1: ThriftHBaseException ex1);
  HRegionInfo getRegionInfo(1: binary regionName) throws (1: ThriftHBaseException ex1);
  list<HRegionInfo> getRegionsAssignment() throws (1: ThriftHBaseException ex1);
  list<Result> getRows(1: binary regionName, 2: list<Get> gets) throws (1: ThriftHBaseException ex1);
  i64 getStartCode();
  string getStopReason();
  list<string> getStoreFileList(1: binary regionName, 2: binary columnFamily) throws (1: ThriftHBaseException ex1);
  list<string> getStoreFileListForAllColumnFamilies(1: binary regionName) throws (1: ThriftHBaseException ex1);
  list<string> getStoreFileListForColumnFamilies(1: binary regionName, 2: list<binary> columnFamilies) throws (1: ThriftHBaseException ex1);
  i64 incrementColumnValue(1: binary regionName, 2: binary row, 3: binary family, 4: binary qualifier, 5: i64 amount, 6: bool writeToWAL) throws (1: ThriftHBaseException ex1);
  bool isStopped();
  i64 lockRow(1: binary regionName, 2: binary row) throws (1: ThriftHBaseException ex1);
  RowLock lockRowAsync(1: binary regionName, 2: binary row) throws (1: ThriftHBaseException ex1);
  TMultiResponse multiAction(1: MultiAction multi) throws (1: ThriftHBaseException ex1);
  MultiPutResponse multiPut(1: MultiPut puts) throws (1: ThriftHBaseException ex1);
  void mutateRow(1: binary regionName, 2: TRowMutations arm) throws (1: ThriftHBaseException ex1);
  void mutateRowAsync(1: binary regionName, 2: TRowMutations arm) throws (1: ThriftHBaseException ex1);
  void mutateRows(1: binary regionName, 2: list<TRowMutations> armList) throws (1: ThriftHBaseException ex1);
  Result next(1: i64 scannerId) throws (1: ThriftHBaseException ex1);
  list<Result> nextRows(1: i64 scannerId, 2: i32 numberOfRows) throws (1: ThriftHBaseException ex1);
  i64 openScanner(1: binary regionName, 2: Scan scan) throws (1: ThriftHBaseException ex1);
  void processDelete(1: binary regionName, 2: Delete deleteArg) throws (1: ThriftHBaseException ex1);
  Result processGet(1: binary regionName, 2: Get get) throws (1: ThriftHBaseException ex1);
  i32 processListOfDeletes(1: binary regionName, 2: list<Delete> deletes) throws (1: ThriftHBaseException ex1);
  void processPut(1: binary regionName, 2: Put put) throws (1: ThriftHBaseException ex1);
  i32 putRows(1: binary regionName, 2: list<Put> puts) throws (1: ThriftHBaseException ex1);
  bool scanClose(1: i64 id) throws (1: ThriftHBaseException ex1);
  ScannerResult scanNext(1: i64 id, 2: i32 numberOfRows) throws (1: ThriftHBaseException ex1);
  ScannerResult scanOpen(1: binary regionName, 2: Scan scan, 3: i32 numberOfRows) throws (1: ThriftHBaseException ex1);
  void setHDFSQuorumReadTimeoutMillis(1: i64 timeoutMillis);
  void setNumHDFSQuorumReadThreads(1: i32 maxThreads);
  void stop(1: string why);
  void stopForRestart();
  void unlockRow(1: binary regionName, 2: i64 lockId) throws (1: ThriftHBaseException ex1);
  void unlockRowAsync(1: binary regionName, 2: i64 lockId) throws (1: ThriftHBaseException ex1);
  void updateConfiguration() throws (1: ThriftHBaseException ex1);
  i32 updateFavoredNodes(1: AssignmentPlan plan) throws (1: ThriftHBaseException ex1);
}
