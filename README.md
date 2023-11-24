# ozone_test_client
Sample demonstrating the problem with writing data to apache/ozone storage

### Steps to reproduce

#### clone and build latest apache/ozone (1.4.0-SNAPSHOT)
1. git clone https://github.com/apache/ozone.git
2. run ```mvn clean install -DskipTests```

   
#### start pseudo cluster in docker
```
cd hadoop-ozone/dist/target/ozone-1.4.0-SNAPSHOT/compose/ozone
OZONE_REPLICATION_FACTOR=3 ./run.sh -d
```

#### open ozone_test_client project
1. ```mvn clean install -DskipTests```
2. run main class from IDE
* try to write data to ozone bucket, but got an exception 


#### docker output
* Containers are working 
![image](https://github.com/Montura/ozone_test_client/assets/4503006/42c0d227-45a9-4803-9dc1-561217e7a0e1)
* OM log
![image](https://github.com/Montura/ozone_test_client/assets/4503006/c3ea3edc-21b5-45a2-94f7-b4e2d497acec)


#### java sample output

```
main] WARN org.apache.hadoop.util.NativeCodeLoader - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
[main] INFO org.apache.hadoop.ozone.client.rpc.RpcClient - Creating Volume: assets, with andrey as owner and space quota set to -1 bytes, counts quota set to -1
[main] INFO org.apache.hadoop.ozone.client.rpc.RpcClient - Creating Bucket: assets/videos, with bucket layout FILE_SYSTEM_OPTIMIZED, andrey as owner, Versioning false, Storage Type set to DISK and Encryption set to false, Replication Type set to RATIS, Namespace Quota set to -1, Space Quota set to -1 
[main] WARN org.apache.hadoop.metrics2.impl.MetricsConfig - Cannot locate configuration: tried hadoop-metrics2-xceiverclientmetrics.properties,hadoop-metrics2.properties
[main] INFO org.apache.hadoop.metrics2.impl.MetricsSystemImpl - Scheduled Metric snapshot period at 10 second(s).
[main] INFO org.apache.hadoop.metrics2.impl.MetricsSystemImpl - XceiverClientMetrics metrics system started
[main] INFO org.apache.ratis.metrics.MetricRegistries - Loaded MetricRegistries class org.apache.ratis.metrics.dropwizard3.Dm3MetricRegistriesImpl
[grpc-default-executor-1] ERROR org.apache.ratis.client.impl.OrderedAsync - client-2B6E68F82A52: Failed* RaftClientRequest:client-2B6E68F82A52->84b041dc-81d4-4f57-bfa3-38f2f76e3b12@group-6634C2D8E679, cid=1, seq=1*, Watch(0), null
java.util.concurrent.CompletionException: java.io.IOException: org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException: UNAVAILABLE: io exception
	at java.util.concurrent.CompletableFuture.encodeThrowable(CompletableFuture.java:292)
	at java.util.concurrent.CompletableFuture.completeThrowable(CompletableFuture.java:308)
	at java.util.concurrent.CompletableFuture.uniApply(CompletableFuture.java:607)
	at java.util.concurrent.CompletableFuture$UniApply.tryFire(CompletableFuture.java:591)
	at java.util.concurrent.CompletableFuture.postComplete(CompletableFuture.java:488)
	at java.util.concurrent.CompletableFuture.completeExceptionally(CompletableFuture.java:1990)
	at org.apache.ratis.grpc.client.GrpcClientProtocolClient$AsyncStreamObservers.completeReplyExceptionally(GrpcClientProtocolClient.java:394)
	at org.apache.ratis.grpc.client.GrpcClientProtocolClient$AsyncStreamObservers.access$000(GrpcClientProtocolClient.java:300)
	at org.apache.ratis.grpc.client.GrpcClientProtocolClient$AsyncStreamObservers$1.onError(GrpcClientProtocolClient.java:331)
	at org.apache.ratis.thirdparty.io.grpc.stub.ClientCalls$StreamObserverToCallListenerAdapter.onClose(ClientCalls.java:481)
	at org.apache.ratis.thirdparty.io.grpc.PartialForwardingClientCallListener.onClose(PartialForwardingClientCallListener.java:39)
	at org.apache.ratis.thirdparty.io.grpc.ForwardingClientCallListener.onClose(ForwardingClientCallListener.java:23)
	at org.apache.ratis.grpc.metrics.intercept.client.MetricClientCallListener.onClose(MetricClientCallListener.java:47)
	at org.apache.ratis.thirdparty.io.grpc.internal.ClientCallImpl.closeObserver(ClientCallImpl.java:567)
	at org.apache.ratis.thirdparty.io.grpc.internal.ClientCallImpl.access$300(ClientCallImpl.java:71)
	at org.apache.ratis.thirdparty.io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl$1StreamClosed.runInternal(ClientCallImpl.java:735)
	at org.apache.ratis.thirdparty.io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl$1StreamClosed.runInContext(ClientCallImpl.java:716)
	at org.apache.ratis.thirdparty.io.grpc.internal.ContextRunnable.run(ContextRunnable.java:37)
	at org.apache.ratis.thirdparty.io.grpc.internal.SerializingExecutor.run(SerializingExecutor.java:133)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:750)
Caused by: java.io.IOException: org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException: UNAVAILABLE: io exception
	at org.apache.ratis.grpc.GrpcUtil.unwrapException(GrpcUtil.java:99)
	at org.apache.ratis.grpc.GrpcUtil.unwrapIOException(GrpcUtil.java:159)
	at org.apache.ratis.grpc.client.GrpcClientProtocolClient$AsyncStreamObservers$1.onError(GrpcClientProtocolClient.java:330)
	... 13 more
Caused by: org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException: UNAVAILABLE: io exception
	at org.apache.ratis.thirdparty.io.grpc.Status.asRuntimeException(Status.java:537)
	... 13 more
Caused by: org.apache.ratis.thirdparty.io.netty.channel.ConnectTimeoutException: connection timed out: /192.168.0.7:9858
	at org.apache.ratis.thirdparty.io.netty.channel.epoll.AbstractEpollChannel$AbstractEpollUnsafe$2.run(AbstractEpollChannel.java:613)
	at org.apache.ratis.thirdparty.io.netty.util.concurrent.PromiseTask.runTask(PromiseTask.java:98)
	at org.apache.ratis.thirdparty.io.netty.util.concurrent.ScheduledFutureTask.run(ScheduledFutureTask.java:153)
	at org.apache.ratis.thirdparty.io.netty.util.concurrent.AbstractEventExecutor.runTask(AbstractEventExecutor.java:173)
	at org.apache.ratis.thirdparty.io.netty.util.concurrent.AbstractEventExecutor.safeExecute(AbstractEventExecutor.java:166)
	at org.apache.ratis.thirdparty.io.netty.util.concurrent.SingleThreadEventExecutor.runAllTasks(SingleThreadEventExecutor.java:470)
	at org.apache.ratis.thirdparty.io.netty.channel.epoll.EpollEventLoop.run(EpollEventLoop.java:416)
	at org.apache.ratis.thirdparty.io.netty.util.concurrent.SingleThreadEventExecutor$4.run(SingleThreadEventExecutor.java:997)
	at org.apache.ratis.thirdparty.io.netty.util.internal.ThreadExecutorMap$2.run(ThreadExecutorMap.java:74)
	at org.apache.ratis.thirdparty.io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)
	... 1 more
```

* thread dump
  ![image](https://github.com/Montura/ozone_test_client/assets/4503006/3b866f47-eb5e-4c1b-9f55-523ee30f78d7)
