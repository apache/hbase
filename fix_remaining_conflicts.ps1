# 修复剩余的Git合并冲突
$filePath = "hbase-common\src\main\java\org\apache\hadoop\hbase\CellComparatorImpl.java"
$content = Get-Content $filePath -Raw

# 修复所有剩余的冲突模式
$content = $content -replace '<<<<<<< HEAD\s*\n\s*private int compareBBKV\(final ByteBufferKeyValue left, final ByteBufferKeyValue right\) \{\s*\n=======\s*\n\s*private static int compareBBKV\(final ByteBufferKeyValue left, final ByteBufferKeyValue right\) \{\s*\n>>>>>>> rvv-optimization', 'private static int compareBBKV(final ByteBufferKeyValue left, final ByteBufferKeyValue right) {'

$content = $content -replace '<<<<<<< HEAD\s*\n\s*private int compareKVVsBBKV\(final KeyValue left, final ByteBufferKeyValue right\) \{\s*\n=======\s*\n\s*private static int compareKVVsBBKV\(final KeyValue left, final ByteBufferKeyValue right\) \{\s*\n>>>>>>> rvv-optimization', 'private static int compareKVVsBBKV(final KeyValue left, final ByteBufferKeyValue right) {'

# 修复方法调用冲突
$content = $content -replace '<<<<<<< HEAD\s*\n\s*diff = ByteBufferUtils\.compareTo\(left\.getRowByteBuffer\(\), left\.getRowPosition\(\), leftRowLength,\s*\n\s*right\.getRowByteBuffer\(\), right\.getRowPosition\(\), rightRowLength\);\s*\n=======\s*\n\s*diff = ByteBufferUtils\.compareToRvv\(left\.getRowByteBuffer\(\), left\.getRowPosition\(\), leftRowLength,\s*\n\s*right\.getRowByteBuffer\(\), right\.getRowPosition\(\), rightRowLength\);\s*\n>>>>>>> rvv-optimization', 'diff = ByteBufferUtils.compareToRvv(left.getRowByteBuffer(), left.getRowPosition(), leftRowLength, right.getRowByteBuffer(), right.getRowPosition(), rightRowLength);'

# 修复其他常见的冲突模式
$content = $content -replace '<<<<<<< HEAD\s*\n\s*.*?\s*\n=======\s*\n\s*(.*?)\s*\n>>>>>>> rvv-optimization', '$1'

# 保存文件
Set-Content $filePath $content -Encoding UTF8

Write-Host "冲突修复完成！"
