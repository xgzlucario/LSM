# LSM
使用 Golang 仿照 LevelDB 实现的一个简化版 KV 数据库项目，目的是为了深入学习 LevelDB 原理。

目前处于开发阶段，迭代较快，如果感兴趣不妨点个 star 保持关注。

目前实现功能：

1. MemTable 基于 Arena SkipList
2. LSM Put() 方法基本完成：MemTable -> Immutable MemTable -> SSTable
3. SSTable 编解码及缓存加载
4. Minor Compact & Major Compact
5. SSTables RefCounter（sst 引用计数模块）

TODO：

1. LSM Get() 方法，可能是 LSM 中最复杂的方法之一
2. block cache
3. WAL
4. ...
