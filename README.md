# 基于Zookeeper解决常见分布式问题

- ZooKeeper是一个分布式的，开放源码的分布式应用程序协调服务
- ZooKeeper的目标就是封装好复杂易出错的关键服务，将简单易用的接口和性能高效、功能稳定的系统提供给用户
- ZooKeeper包含一个简单的原语集，提供Java和C的接口

## 1. 简介

学习Zookeeper也有一段时间了，我想从实际的分布式应用场景出发，来学习如何使用Zookeeper去解决一些常见的分布式问题，对常见应用场景主要提供Java原生API和Curator两种实现方式

### 1. Zookeeper

Zookeeper作为一个分布式服务框架，主要用来解决分布式数据一致性问题，它提供了简单的分布式原语，并且对多种编程语言提供了API，我们重点使用Zookeeper的Java客户端API实现常用应用场景。

### 2. Curator

为了更好的实现Java操作Zookeeper服务器，后来出现了Curator框架，非常的强大，目前已经是Apache的顶级项目，里面提供了更多丰富的操作，例如session超时重连、主从选举、分布式计数器、分布式锁等等适用于各种复杂的zookeeper场景的API封装。

## 2. 常用场景

### 1. 数据发布/订阅

- 数据发布/订阅 (Publish/Subscribe) 系统，即所谓的配置中心，顾名思义就是发布者将数据发布到ZooKeeper的一个或一系列节点上，供订阅者进行数据订阅，进而达到动态获取数据的目的，实现配置信息的集中式管理和数据的动态更新。
- 发布/订阅系统一般有两种设计模式，分别是推 (Push) 模式和拉 (Pull) 模式。在推模式中，服务端主动将数据更新发送给所有订阅的客户端；而拉模式则是由客户端主动发起请求来获取最新数据，通常客户端都采用定时进行轮询拉取的方式。
- 如果将配置信息存放到 ZooKeeper 上进行集中管理，那么通常情况下，应用在启动的时候都会主动到ZooKeeper服务端上进行--次配置信息的获取，同时，在指定节点上注册一个 Watcher 监听，这样一来，但凡配置信息发生变更，服务端都会实时通知到所有订阅的客户端，从而达到实时获取最新配置信息的目的。
- ZooKeeper 采用的是推拉相结合的方式：客户端向服务端注册自己需要关注的节点，一旦该节点的数据发生变更，那么服务端就会向相应的客户端发送 Watcher 事件通知，客户端接收到这个消息通知之后，需要主动到服务端获取最新的数据。

### 2. 负载均衡

- 负载均衡 (Load Balance) 是一种相当常见的计算机网络技术，用来对多个计算机(计算机集群)、网络连接、CPU、磁盘驱动器或其他资源进行分配负载，以达到优化资源使用、最大化吞吐率、最小化响应时间和避免过载的目的。通常负载均衡可以分为硬件和软件负载均衡两类，我们主要探讨的是 ZooKeeper 在“软”负载均衡中的应用场景。
- 分布式系统具有对等性，为了保证系统的高可用性，通常采用副本的方式来对数据和服务进行部署。而对于消费者而言，则需要在这些对等的服务提供方中选择一个来执行相关的业务逻辑，其中比较典型的就是DNS服务。DNS是域名系统 (DomainNameSystem) 的缩写，是因特网中使用最广泛的核心技术之一，DNS 系统可以看作是一个超大规模的分布式映射表，用于将域名和IP地址进行一一映射，进而方便人们通过域名来访问互联网站点。
- 我们来介绍一种基干 ZooKeeper 实现的动态 DNS 方案(以下简称该方案为
“DDNS”, Dynamic DNS)。和配置管理一样，我们首先需要在 ZooKeeper 上创建一个节点来进行域名配置，例如 /DDNS/app1/server:appl.companyl.com (以下简称“域名节点”)。通常应用都会首先从域名节点中获取一份IP地址和端口的配置，进行自行解析。同时，每个应用还会在域名节点上注册一个数据变更 Watcher 监听，以便及时收到域名变更的通知。

### 3. 命名服务

- Java 语言中的 JNDI 便是一种典型的命名服务。JNDI 是 Java 命名与目录接口 (Java Naming and Directory Interface) 的缩写，是 J2EE 体系中重要的规范之一，标准的 J2EE 容器都提供了对 JNDI 规范的实现。因此，在实际开发中，开发人员常常使用应用服务器自带的 JNDI 实现来完成数据源的配置与管理，使用 JNDI 方式后，开发人员可以完全不需要关心与数据库相关的任何信息，包括数据库类型、JDBC 驱动类型以及数据库账户等。
- ZooKeeper 提供的命名服务功能与 JNDI 技术有相似的地方，都能够帮助应用系统通过一个资源引用的方式来实现对资源的定位与使用。另外，广义上命名服务的资源定位都不是真正意义的实体资源，在分布式环境中，上层应用仅仅需要一个全局唯一的名字，类似于数据库中的唯一主键。下面我们来看看如何使用 ZooKeeper 来实现一套分布式全局唯一ID的分配机制。
- 通过调用 ZooKeeper 节点创建的 API 接口可以创建一个顺序节点，并且在 API 返回值中会返回这个节点的完整名字。利用这个特性，我们就可
以借助 ZooKeeper 来生成全局唯一的 ID 了。
  - 所有客户端都会根据自己的任务类型，在指定类型的任务下面通过调用 create() 接口来创建一个顺序节点，例如创建“job-” 节点。
  - 节点创建完毕后，create() 接口会返回一个完整的节点名，例如“job-000000003”。
  - 客户端拿到这个返回值后，拼接上 type 类型，例如“type2-job-000000003”，这就可以作为一个全局唯一的ID了。

### 4. 分布式协调/通知

- 分布式协调/通知服务是分布式系统中不可缺少的-一个环节，是将不同的分布式组件有机结合起来的关键所在。对于一个在多台机器上部署运行的应用而言，通常需要一个协调者 (Coordinator) 来控制整个系统的运行流程，例如分布式事务的处理、机器间的互相协调等。同时，引入这样一个协调者，便于将分布式协调的职责从应用中分离出来，从而可以大大减少系统之间的耦合性，而且能够显著提高系统的可扩展性。
- ZooKeeper 中特有的 Watcher 注册与异步通知机制，能够很好地实现分布式环境下不同机器，甚至是不同系统之间的协调与通知，从而实现对数据变更的实时处理。基于 ZooKeeper 实现分布式协调与通知功能，通常的做法是不同的客户端都对 ZooKeeper 上同一个数据节点进行 Watcher 注册，监听数据节点的变化(包括数据节点本身及其子节点)，如果数据节点发生变化，那么所有订阅的客户端都能够接收到相应的 Watcher 通知，并做出相应的处理。

### 5. 集群管理

- 所谓集群管理，包括集群监控与集群控制两大块。
- 利用 ZooKeeper 的 Watcher 监听和临时节点这两大特性，就可以实现另一种集群机器存活性监控的系统。例如，监控系统在 /elusterServers 节点上注册一个 Watcher 监听,那么但凡进行动态添加机器的操作，就会在 /clusterSrvers 节点下创建一个临时节点: /clusterServers/[Hostname]。这样一来，监控系统就能够实时检测到机器的变动情况，至于后续处理就是监控系统的业务了。

### 6. Master选举

- 在分布式计算中，Master 选举是很重要的一个功能。分布式最核心的特性就是能够将具有独立计算能力的系统单元部署在不同的机器上，构成一个完整的分布式系统。
- 在分布式系统中，Master往往用来协调集群中其他系统单元，具有对分布式系统状态变更的决定权。
- 利用 ZooKeeper 的强一致性，能够很好地保证在分布式高并发情况下节点的创建一定能够保证全局唯一性，即 ZooKeeper 将会保证客户端无法重复创建一个已经存在的数据节点。也就是说，如果同时有多个客户端请求创建同一个节点，那么最终一定只有一个客户端请求能够创建成功。利用这个特性，就能很容易地在分布式环境中进行 Master 选举了。
- 客户端往 ZooKeeper 上创建一个临时节点，在这个过程中，只有一个客户端能够成功创建这个节点，那么这个客户端所在的机器就成为了 Master。同时，其他没有在 ZooKeeper 上成功创建节点的客户端，都会在节点上注册一个子节点变更的 Watcher，用于监控当前的 Master 机器是否存活，一旦发现当前的 Master 挂了，那么其余的客户端将会重新进行 Master 选举。
- 我们可以看到，如果仅仅只是想实现 Master 选举的话，那么其实只需要有一个能够保证数据唯一性的组件即可，例如关系型数据库的主键模型就是非常不错的选择。但是，如果希望能够快速地进行集群 Master 动态选举，那么基于 ZooKeeper 来实现是一个不错的新思路。

### 7. 分布式锁

- 分布式锁是控制分布式系统之间同步访问共享资源的一种方式。如果不同的系统或是同一个系统的不同主机之间共享了一个或一组资源，那么访问这些资源的时候，往往需要通过一些互斥手段来防止彼此之间的干扰，以保证一致性，在这种情况下，就需要使用分布式锁了。

**排它锁**

- 排他锁的核心是如何保证当前有且仅有一个事务获得锁，并且锁被释放后，所有正在等待获取锁的事务都能够被通知到。
- 在通常的 Java 开发编程中，有两种常见的方式可以用来定义锁，分别是 synchronized 机制和JDK5提供的 ReentrantLock。然而，在 ZooKeeper 中，没有类似于这样的 API 可以直接使用，而是通过 ZooKeeper 上的数据节点来表示一个锁，例如 /exclusive_ lock/lock 节点就可以被定义为一个锁。
- 在需要获取排他锁时，所有的客户端都会试图通过调用 create() 接口， 在
 /exclusive_ lock 节点下创建临时子节点 /exclusive_ lock/lock。ZooKeeper 会保证在所有的客户端中，最终只有一个客户端能够创建成功，那么就可以认为该客户端获取了锁。同时，所有没有获取到锁的客户端就需要到 /exclusive_ lock 节点上注册一个子节点变更的 Watcher 监听，以便实时监听到 lock 节点的变更情况。
- /exclusive_ lock/lock 是一个临时节点，因此在以下两种情况下，都有可能释放锁。
  - 当前获取锁的客户端机器发生宕机，那么 ZooKeeper 上的这个临时节点就会被移除。
  - 正常执行完业务逻辑后，客户端就会主动将自己创建的临时节点删除。
- 无论在什么情况下移除了 lock 节点，ZooKeeper 都会通知所有在 /exclusive_ lock 节点上注册了子节点变更 Watcher 监听的客户端。这些客户端在接收到通知后，再次重新发起分布式锁获取。

**共享锁**

- 共享锁和排他锁最根本的区别在于，加上排他锁后，数据对象只对一个事务可见，而加上共享锁后，数据对所有事务都可见。
- 和排他锁-样，同样是通过 ZooKeeper 上的数据节点来表示一个锁，是一个类似于
 /shared_ lock/[Hostname]-请求类型-序号 的临时顺序节点，例如 /shared_ lock/
192.168.0.1-R-0000000001，那么，这个节点就代表了一个共享锁
- 根据共享锁的定义，不同的事务都可以同时对同一个数据对象进行读取操作，而更新操作必须在当前没有任何事务进行读写操作的情况下进行。基于这个原则，我们来看看如何通过 ZooKeeper 的节点来确定分布式读写顺序，大致可以分为如下4个步骤。
  - 创建完节点后，获取 /shared_lock 节点下的所有子节点，并对该节点注册子节点变更的 Watcher 监听。
  - 确定自己的节点序号在所有子节点中的顺序。
  - 对于读请求：如果没有比自己序号小的子节点，或是所有比自己序号小的子节点都是读请求，那么表明自己已经成功获取到了共享锁，同时开始执行读取逻辑。如果比自己序号小的子节点中有写请求，那么就需要进入等待。对于写请求：如果自己不是序号最小的子节点，那么就需要进入等待。
  - 接收到 Watcher 通知后，重复步骤1。

  **羊群效应**

### 8. 分布式队列

- 分布式队列，简单地讲分为两大类，一种是常规的先入先出队列，另一种则是要等到队列元素集聚之后才统一安排执行的Barrier模型。

**FIFO**

- 使用 ZooKeeper 实现 FIFO 队列，和共享锁的实现非常类似。FIFO 队列就类似于一个全写的共享锁模型，大体的设计思路其实非常简单：所有客户端都会到 /queue_fifo 这个节点下面创建一个临时顺序节点，例如 /queue_fifo/192.168.0.1-0000000001。
- 创建完节点之后，根据如下4个步骤来确定执行顺序。
  - 通过调用 getChildren() 接口来获取 /queue_fifo 节点下的所有子节点，即获取队列中所有的元素。
  - 确定自己的节点序号在所有子节点中的顺序。
  - 如果自己不是序号最小的子节点，那么就需要进入等待，同时向比自己序号小的最后一个节点注册 Watcher 监听。
  - 接收到 Watcher 通知后，重复步骤1。

**Barrier**

- Barrier 原意是指障碍物、屏障，而在分布式系统中，特指系统之间的一个协调条件，规定了一个队列的元素必须都集聚后才能统一进行安排，否则一直等待。这往往出现在那些大规模分布式并行计算的应用场景上：最终的合并计算需要基于很多并行计算的子结果来进行。这些队列其实是在 FIFO 队列的基础上进行了增强
- 大致的设计思想如下：开始时，/queue_barrier 节点是一个已经存在的默认节点，并且将其节点的数据内容赋值为一个数字n来代表 Barrier 值，例如n=10表示只有当 /queue_barrier 节点下的子节点个数达到10后，才会打开 Barrier。之后，所有的客户端都会到 /queue_barrier 节点下创建一个临时节点，例如 /queue_barrier/l92.168.0.1。




