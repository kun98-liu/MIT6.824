## lab2

### lab2 内容

lab2是基于raft的论文来实现一个raft的consensus机制。

整个lab2包含了4个部分：
领导人选举，心跳机制传输log，持久化，快照。

其中，领导人选举的Requestvote，传输log时的Appendentry，Install Snapshot 是以RPC的方式来实现的。

具体可以参考论文中的figure2来设计RPC的参数。

### lab2 分析
课程官方将lab2分成了4个小lab，这样已经很大的帮助了我们分割模块。只要跟着官方的顺序来一步步实现每个部分，就可以完整的实现整个lab2.

在反复的读了好几遍raft论文后，正式开始动手码代码。

1. 先实现```raft/raft.go```下的```Make() *Raft```函数，来生成Raft实例。由于每个raft实例都要并行的进行检测选举超时和心跳，goroutine是必须的。
2. 注意看源码中给出的```ApplyMsg```和```Make()```函数中的参数```applyCh chan ApplyMsg```, 作为一个分布式应用的consensus底层，Raft要将上层传来的指令进行主从同步后，以管道的方式传回给上层。我使用了goroutine，sync.Cond来实现了一个非阻塞的消息队列，一旦有新的ApplyMsg进入消息队列就会唤醒该协程，然后将消息提交到管道里。

接下来就是各个部分的实现

- 2A，2B
  - 这两个部分中，涉及到了核心的心跳和领导人选举。
  - 在实现的过程中，最有可能出现的bug是：领导人选举失败，Term一直增加，直到测试用例发现系统已经不在正常工作。课程官方给出的student guideline非常有用，里面详细描述了什么时候该重置election timeout。
  - 另一个棘手的问题就是，log backtracking。当peer的log与leader出现不一致的情况下，就需要对peer的log进行rollback这样的操作。自己实现时容易出现rpc过多，或者根本就做不对的情况。guideline里同样给出了最佳解法。
- 2C
  - 比较简单的一个部分，根据example实现persist即可。
  - 确保ABC都完美通过在进入下一部分
  - 做好代码的复用性，抽象性
- 2D
  - 先是一个实现Snapshot，调用一下pesister即可
  - InstallSnapshot RPC的调用应该与AppendEntry RPC放在一起。当```rf.nextIndex[i] <= rf.getFirstIndex()```时，则该次心跳应该发送Snapshot，否则就发送AppendEntry
  - dummylog的设计可以让你很简单的获取当前snapshot的index和term。亦能避免很多的下标越界等问题。
  - Installsnapshot同样要发送消息到applych



### 总结

断断续续一个lab2做了两个月，遇到了很多的奇奇怪怪的bug，特别是选举失败。最后发现是没有严格地按照论文里的说法来实现，参照了guideline以后bug就解决了。

抽象出一个好用的工具类真的能够事半功倍。比如在snapshot引入后，log的index和实际slice的index是不一样的。所以，抽象出一套getfirstindex之类的方法会非常好用。

代码量不大，但实现时的每个设计有其原因。最后全部通过后，再review代码时发现，哦，这就是raft描述的consensus啊，真的每一处都不能随便乱改。