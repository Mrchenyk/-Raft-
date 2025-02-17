# 基于Raft实现的分布式KV存储



## **Raft的核心概念：**

1. **节点角色：**
   - **Leader（领导者）：** 负责处理客户端请求，并将日志条目复制到其他节点。
   - **Follower（跟随者）：** 被动地接收来自Leader的日志条目，并响应客户端的读取请求。
   - **Candidate（候选者）：** 在Leader失效或集群初始化时，发起选举以成为新的Leader。
2. **任期（Term）：**
   - Raft将时间划分为多个任期，每个任期内只能有一个Leader。任期号递增，用于区分不同的Leader。
3. **日志复制：**
   - Leader将客户端请求作为日志条目添加到自己的日志中，并将其复制到所有Follower。
   - 一旦日志条目在大多数节点上被复制，Leader会将其标记为已提交，随后应用到状态机中。
4. **选举机制：**
   - 当Follower在一段时间内未收到Leader的心跳信号时，会转变为Candidate，发起选举。
   - Candidate向其他节点请求投票，若获得大多数节点的支持，则成为新的Leader。

## ShardKV

基于Raft算法，客户端主要实现**Get、Put、Append**这三个方法，当客户端把指令发送给服务端时，首先要找到Raft集群的Leader节点，Raft集群同步之后把**ApplyMsg**发送进**ApplyChan**，此时服务端就能从通道中读取到数据然后应用到自己的状态机中。

![](asserts\shardkv.png)

分片的分布式 KV 存储系统由两个主要的部分组成。首先是**复制组（Replica Group）**，它指的是处理一个或多个 shard 的 KV 服务，通常是由一个 Raft 集群组成的，所以一个完整的分片分布式 KV 系统中一般存在多个 Replica Group，每个 Group 负责一部分 shard 的读写请求和数据存储。第二个组成部分是 **shard controller**，它主要是存储系统元数据，一般是一些配置信息，例如每个 Group 应该负责哪些 shard，这个配置信息是有可能发生变化的。客户端首先会从 shard controller 获取请求 key 所属的 Group，并且 Group 也会从 shard controller 中获取它应该负责哪些 shard。shard controller 也一般是会保证高可用，因为如果 shard controller 发生了单点故障，那么整个分布式 KV 系统就不可用了，因此 shard controller 也会使用 raft 进行状态同步。
