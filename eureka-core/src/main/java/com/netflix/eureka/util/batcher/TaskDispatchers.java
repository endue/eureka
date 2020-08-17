package com.netflix.eureka.util.batcher;

/**
 * See {@link TaskDispatcher} for an overview.
 *
 * @author Tomasz Bak
 */
public class TaskDispatchers {

    public static <ID, T> TaskDispatcher<ID, T> createNonBatchingTaskDispatcher(String id,
                                                                                int maxBufferSize,
                                                                                int workerCount,
                                                                                long maxBatchingDelay,
                                                                                long congestionRetryDelayMs,
                                                                                long networkFailureRetryMs,
                                                                                TaskProcessor<T> taskProcessor) {
        final AcceptorExecutor<ID, T> acceptorExecutor = new AcceptorExecutor<>(
                id, maxBufferSize, 1, maxBatchingDelay, congestionRetryDelayMs, networkFailureRetryMs
        );
        final TaskExecutors<ID, T> taskExecutor = TaskExecutors.singleItemExecutors(id, workerCount, taskProcessor, acceptorExecutor);
        return new TaskDispatcher<ID, T>() {
            @Override
            public void process(ID id, T task, long expiryTime) {
                acceptorExecutor.process(id, task, expiryTime);
            }

            @Override
            public void shutdown() {
                acceptorExecutor.shutdown();
                taskExecutor.shutdown();
            }
        };
    }

    public static <ID, T> TaskDispatcher<ID, T> createBatchingTaskDispatcher(String id,
                                                                             int maxBufferSize,//10000
                                                                             int workloadSize,// 250
                                                                             int workerCount,// 20
                                                                             long maxBatchingDelay,// 500
                                                                             long congestionRetryDelayMs,// 1000
                                                                             long networkFailureRetryMs,// 100
                                                                             TaskProcessor<T> taskProcessor) {// ReplicationTaskProcessor
        // 步骤一、创建接受任务的执行器，里面创建一个AcceptorRunner线程并启动
        final AcceptorExecutor<ID, T> acceptorExecutor = new AcceptorExecutor<>(
                id, maxBufferSize, workloadSize, maxBatchingDelay, congestionRetryDelayMs, networkFailureRetryMs
        );
        // 步骤二、创建执行任务的执行器
        final TaskExecutors<ID, T> taskExecutor = TaskExecutors.batchExecutors(id, workerCount, taskProcessor, acceptorExecutor);

        return new TaskDispatcher<ID, T>() {
            // 将任务交给第一步创建的acceptorExecutor的属性BlockingQueue<TaskHolder<ID, T>> acceptorQueue中
            // acceptorQueue是LinkedBlockingQueue
            @Override
            public void process(ID id, T task, long expiryTime) {
                acceptorExecutor.process(id, task, expiryTime);
            }

            @Override
            public void shutdown() {
                acceptorExecutor.shutdown();
                taskExecutor.shutdown();
            }
        };
    }
}
