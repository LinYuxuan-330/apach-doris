flink 源码解析
1、分布式架构底层的RPC通信（远程过程调用）,两个进程相互通信，也有自己调自己。高并发
基于Scala的网络编程库：Akka+Netty（1.18之前），Pekko+Netty(1.18之后)
Apach Pekko是Akka2.6.x的一个分支，Akka协议改为不开源，用于构建高并发、高可用的一个框架



1.1 
Actor 负责通信组件 ActorSystem管理Actor生命周期。
每个Actor都有一个Mailbox，别的Actor发送给它的消息都首先存储在Mailbox中，通过这种方式可以实现异步通信。
每个Actor是单线程的处理方式，不断的从Mailbox拉取消息执行处理，所以对于Actor的消息处理，不适合调用会阻塞的处理方法
Actor可以改变他自身的状态，可以接收消息，也可以发送消息，还可以生成新的Actor
每一个ActorSystem和Actor都在启动的时候会给定一个name，如果要从ActorSystem中，获取一个Actor，则通过以下的方式来进行Actor的获取：pekko.tcp://flink@localhost:6123/user/rpc/resourcemanager_*
如果一个 Actor 要和另外一个 Actor进行通信，则必须先获取对方 Actor 的 ActorRef 对象，然后通过该对象发送消息即可。
通过 tell 发送异步消息，不接收响应，通过 ask 发送异步消息，得到 Future 返回，通过异步回到返回处理结果。
