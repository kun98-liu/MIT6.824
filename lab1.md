## lab1
### lab1 内容
实现一个MapReduce程序。
- 程序入口在main文件夹下, main/mrcoordinator.go, main/mrworker.go
- 需要编写的代码在mr文件夹下，mr/coordinator.go, mr/worker.go, mr/rpc.go

### lab1 分析
开始动手写代码之前，必须先把整体框架了解清楚。可以参照mapreduce论文中的架构图。

- 首先，从main包入手。
    - mrcoordinator.go为协调器的main入口，其中会调用mr.MakeCoordinator方法生成我们自定义的协调器，并且会循环调用Done()方法来持续检查是否完成所有Task
    - mrworker.go为worker的的main入口，其中会加载mrapps文件夹下的mr插件（plugins），之后调用我们自定义的Worker的worker方法。且根据lab要求，worker要单起一个进程，那么Worker方法中则应该采用同步的方式执行task，而不是异步或多线程的执行任务
- 进入mr文件夹下
    - coordinator.go
        - MakeCoordinator方法会创建Coordinator然后调用serve方法，创建goroutine处理RPC
        - 我们需要实现协调器的功能主要就是亮点：1、向worker分发任务，2、管理任务状态，3、管理超时任务
    - worker.go
        - 在Worker方法中，我们需要让该worker以循环的方式向协调器索取Task来执行，根据Task类型的不同执行不同的方法。
        - 尽管worker应该以阻塞的方式执行task，但没必要再执行完task后通知协调器任务完成的这一个动作也阻塞。所以，通知协调器task完成可以用go func异步的来通知，让主线程继续要task来做。
    - rpc.go
        - 学习lab中给的Example代码，自定义Args和Reply用于RPC调用中的数据保存。第一次看到这种用法，是真的帅。

### lab1 总结
- 一开始看这个lab一头雾水，不知何处下手。其实关键在于对照着论文里的那个图，并结合lab的说明，分析清楚这个lab的结构。
- 虽然是协调器来分发任务，但实际上这个的主动权应该在worker手里。协调器管的功能应该是管好任务的状态，判断超时，判断任务是否全部做完。
- 除了获取任务，做任务之外，其他功能都可以做成异步的。比如，协调器派送任务后启动新线程判断超时，worker完成任务后启动新线程通知协调器任务完成。

做完这个lab后，感觉没有多复杂，然而其实一共写了有5个小时，总共断断续续用了两天。
当中大部分的时间都是处在一个完全没有头绪的状态，只得反复的看lab的说明，看代码中的example，借鉴别人的实现。
论文写的很清楚，课程讲的也不难，然而想法落地成代码还是一个很困难的事。
还是得多学，多写，多总结。