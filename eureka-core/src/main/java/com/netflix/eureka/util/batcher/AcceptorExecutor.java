package com.netflix.eureka.util.batcher;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.eureka.util.batcher.TaskProcessor.ProcessingResult;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.monitor.StatsTimer;
import com.netflix.servo.monitor.Timer;
import com.netflix.servo.stats.StatsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.eureka.Names.METRIC_REPLICATION_PREFIX;

/**
 * An active object with an internal thread accepting tasks from clients, and dispatching them to
 * workers in a pull based manner. Workers explicitly request an item or a batch of items whenever they are
 * available. This guarantees that data to be processed are always up to date, and no stale data processing is done.
 *
 * <h3>Task identification</h3>
 * Each task passed for processing has a corresponding task id. This id is used to remove duplicates (replace
 * older copies with newer ones).
 *
 * <h3>Re-processing</h3>
 * If data processing by a worker failed, and the failure is transient in nature, the worker will put back the
 * task(s) back to the {@link AcceptorExecutor}. This data will be merged with current workload, possibly discarded if
 * a newer version has been already received.
 *
 * @author Tomasz Bak
 */
class AcceptorExecutor<ID, T> {

    private static final Logger logger = LoggerFactory.getLogger(AcceptorExecutor.class);

    private final int maxBufferSize;
    private final int maxBatchingSize;
    private final long maxBatchingDelay;

    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    /**
     * 保存接收到的任务
     */
    private final BlockingQueue<TaskHolder<ID, T>> acceptorQueue = new LinkedBlockingQueue<>();
    /**
     * 保存再次执行的任务
     */
    private final BlockingDeque<TaskHolder<ID, T>> reprocessQueue = new LinkedBlockingDeque<>();
    /**
     * 接收任务的单个线程
     */
    private final Thread acceptorThread;
    /**
     * 保存待执行任务
     */
    private final Map<ID, TaskHolder<ID, T>> pendingTasks = new HashMap<>();
    /**
     * 保存待执行任务ID
     */
    private final Deque<ID> processingOrder = new LinkedList<>();

    private final Semaphore singleItemWorkRequests = new Semaphore(0);
    /**
     * 保存单个任务
     */
    private final BlockingQueue<TaskHolder<ID, T>> singleItemWorkQueue = new LinkedBlockingQueue<>();

    private final Semaphore batchWorkRequests = new Semaphore(0);
    /**
     * 保存一批一批的任务，一批最多250个
     */
    private final BlockingQueue<List<TaskHolder<ID, T>>> batchWorkQueue = new LinkedBlockingQueue<>();

    private final TrafficShaper trafficShaper;

    /*
     * Metrics
     */
    @Monitor(name = METRIC_REPLICATION_PREFIX + "acceptedTasks", description = "Number of accepted tasks", type = DataSourceType.COUNTER)
    volatile long acceptedTasks;//接收到任务+1

    @Monitor(name = METRIC_REPLICATION_PREFIX + "replayedTasks", description = "Number of replayedTasks tasks", type = DataSourceType.COUNTER)
    volatile long replayedTasks;

    @Monitor(name = METRIC_REPLICATION_PREFIX + "expiredTasks", description = "Number of expired tasks", type = DataSourceType.COUNTER)
    volatile long expiredTasks;// 记录过期任务

    @Monitor(name = METRIC_REPLICATION_PREFIX + "overriddenTasks", description = "Number of overridden tasks", type = DataSourceType.COUNTER)
    volatile long overriddenTasks;

    @Monitor(name = METRIC_REPLICATION_PREFIX + "queueOverflows", description = "Number of queue overflows", type = DataSourceType.COUNTER)
    volatile long queueOverflows;

    private final Timer batchSizeMetric;

    AcceptorExecutor(String id,
                     int maxBufferSize,// 1000
                     int maxBatchingSize,// 250
                     long maxBatchingDelay,// 500
                     long congestionRetryDelayMs,// 1000
                     long networkFailureRetryMs) {// 100
        this.maxBufferSize = maxBufferSize;
        this.maxBatchingSize = maxBatchingSize;
        this.maxBatchingDelay = maxBatchingDelay;
        // 计算当网络阻塞或失败时的延迟时间
        this.trafficShaper = new TrafficShaper(congestionRetryDelayMs, networkFailureRetryMs);
        // 创建一个AcceptorRunner线程并启动
        ThreadGroup threadGroup = new ThreadGroup("eurekaTaskExecutors");
        this.acceptorThread = new Thread(threadGroup, new AcceptorRunner(), "TaskAcceptor-" + id);
        this.acceptorThread.setDaemon(true);
        this.acceptorThread.start();

        final double[] percentiles = {50.0, 95.0, 99.0, 99.5};
        final StatsConfig statsConfig = new StatsConfig.Builder()
                .withSampleSize(1000)
                .withPercentiles(percentiles)
                .withPublishStdDev(true)
                .build();
        final MonitorConfig config = MonitorConfig.builder(METRIC_REPLICATION_PREFIX + "batchSize").build();
        this.batchSizeMetric = new StatsTimer(config, statsConfig);
        try {
            Monitors.registerObject(id, this);
        } catch (Throwable e) {
            logger.warn("Cannot register servo monitor for this object", e);
        }
    }

    void process(ID id, T task, long expiryTime) {
        acceptorQueue.add(new TaskHolder<ID, T>(id, task, expiryTime));
        acceptedTasks++;
    }

    void reprocess(List<TaskHolder<ID, T>> holders, ProcessingResult processingResult) {
        reprocessQueue.addAll(holders);
        replayedTasks += holders.size();
        trafficShaper.registerFailure(processingResult);
    }

    void reprocess(TaskHolder<ID, T> taskHolder, ProcessingResult processingResult) {
        reprocessQueue.add(taskHolder);
        replayedTasks++;
        trafficShaper.registerFailure(processingResult);
    }

    BlockingQueue<TaskHolder<ID, T>> requestWorkItem() {
        singleItemWorkRequests.release();
        return singleItemWorkQueue;
    }

    BlockingQueue<List<TaskHolder<ID, T>>> requestWorkItems() {
        batchWorkRequests.release();
        return batchWorkQueue;
    }

    void shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            acceptorThread.interrupt();
        }
    }

    @Monitor(name = METRIC_REPLICATION_PREFIX + "acceptorQueueSize", description = "Number of tasks waiting in the acceptor queue", type = DataSourceType.GAUGE)
    public long getAcceptorQueueSize() {
        return acceptorQueue.size();
    }

    @Monitor(name = METRIC_REPLICATION_PREFIX + "reprocessQueueSize", description = "Number of tasks waiting in the reprocess queue", type = DataSourceType.GAUGE)
    public long getReprocessQueueSize() {
        return reprocessQueue.size();
    }

    @Monitor(name = METRIC_REPLICATION_PREFIX + "queueSize", description = "Task queue size", type = DataSourceType.GAUGE)
    public long getQueueSize() {
        return pendingTasks.size();
    }

    @Monitor(name = METRIC_REPLICATION_PREFIX + "pendingJobRequests", description = "Number of worker threads awaiting job assignment", type = DataSourceType.GAUGE)
    public long getPendingJobRequests() {
        return singleItemWorkRequests.availablePermits() + batchWorkRequests.availablePermits();
    }

    @Monitor(name = METRIC_REPLICATION_PREFIX + "availableJobs", description = "Number of jobs ready to be taken by the workers", type = DataSourceType.GAUGE)
    public long workerTaskQueueSize() {
        return singleItemWorkQueue.size() + batchWorkQueue.size();
    }

    class AcceptorRunner implements Runnable {
        @Override
        public void run() {
            long scheduleTime = 0;
            while (!isShutdown.get()) {
                try {
                    // 处理重试队列reprocessQueue、接收任务队列acceptorQueue中的任务取出放到待执行队列pendingTasks中
                    // 如果超过阈值会存在丢弃老任务，从而保存新任务的逻辑
                    drainInputQueues();
                    // 获取待执行任务的数量
                    int totalItems = processingOrder.size();

                    long now = System.currentTimeMillis();
                    // 计算网络拥堵或异常后，需要再等待多久
                    if (scheduleTime < now) {
                        scheduleTime = now + trafficShaper.transmissionDelay();
                    }
                    if (scheduleTime <= now) {
                        // 将任务从待执行队列pendingTasks中批量取出，保存到batchWorkQueue中
                        assignBatchWork();
                        // 将任务从待执行队列pendingTasks中取出一个，保存到singleItemWorkQueue中
                        assignSingleItemWork();
                    }

                    // If no worker is requesting data or there is a delay injected by the traffic shaper,
                    // sleep for some time to avoid tight loop.
                    if (totalItems == processingOrder.size()) {
                        Thread.sleep(10);
                    }
                } catch (InterruptedException ex) {
                    // Ignore
                } catch (Throwable e) {
                    // Safe-guard, so we never exit this loop in an uncontrolled way.
                    logger.warn("Discovery AcceptorThread error", e);
                }
            }
        }

        private boolean isFull() {
            return pendingTasks.size() >= maxBufferSize;// 1000
        }

        private void drainInputQueues() throws InterruptedException {
            do {
                // 将重试队列reprocessQueue中的任务取出放到待执行队列pendingTasks中
                // 当pendingTasks超过阈值则直接丢弃reprocessQueue中的任务
                drainReprocessQueue();
                // 取出接收任务队列acceptorQueue中的值，然后保存到待执行任务队列pendingTasks中
                // 如果pendingTasks已达到阈值，则丢弃最老的任务
                drainAcceptorQueue();

                if (!isShutdown.get()) {
                    // If all queues are empty, block for a while on the acceptor queue
                    // 队列都为空，则等待10s中
                    // 之后如果不为空则处理
                    if (reprocessQueue.isEmpty() && acceptorQueue.isEmpty() && pendingTasks.isEmpty()) {
                        TaskHolder<ID, T> taskHolder = acceptorQueue.poll(10, TimeUnit.MILLISECONDS);
                        if (taskHolder != null) {
                            appendTaskHolder(taskHolder);
                        }
                    }
                }
            } while (!reprocessQueue.isEmpty() || !acceptorQueue.isEmpty() || pendingTasks.isEmpty());
        }

        private void drainAcceptorQueue() {
            // 判断接收任务的队列是否为空
            while (!acceptorQueue.isEmpty()) {
                // 则取出头结点数据
                appendTaskHolder(acceptorQueue.poll());
            }
        }

        /**
         * 将重试队列reprocessQueue中的任务取出放到待执行队列pendingTasks中
         * 超过阈值则直接丢弃reprocessQueue中的任务
         */
        private void drainReprocessQueue() {
            long now = System.currentTimeMillis();
            // 判断reprocessQueue不为空并且pendingTasks没有超过阈值1000，就取出reprocessQueue中的任务放入pendingTasks中
            // 意思就是pendingTasks没有超过阈值就将重试的任务取出放到pendingTasks中
            while (!reprocessQueue.isEmpty() && !isFull()) {
                // 取出reprocessQueue中的任务
                TaskHolder<ID, T> taskHolder = reprocessQueue.pollLast();
                ID id = taskHolder.getId();
                // 判断任务是否过期，如果是则记录
                if (taskHolder.getExpiryTime() <= now) {
                    expiredTasks++;
                // 判断任务是否已经包含了，如果是则记录
                } else if (pendingTasks.containsKey(id)) {
                    overriddenTasks++;
                } else {
                    // 将任务放入pendingTasks
                    pendingTasks.put(id, taskHolder);
                    // 将任务ID放入processingOrder
                    processingOrder.addFirst(id);
                }
            }
            // 判断是否pendingTasks中的任务超过阈值
            if (isFull()) {
                // 记录队列溢出的值
                queueOverflows += reprocessQueue.size();
                // 清空再次执行的任务
                reprocessQueue.clear();
            }
        }


        private void appendTaskHolder(TaskHolder<ID, T> taskHolder) {// taskHolder为acceptorQueue中的结点
            // 判断pendingTasks是否已超过阈值
            if (isFull()) {
                // 取出processingOrder中老的任务ID然后后从待执行队列中删除
                // 最后溢出记录值+1
                pendingTasks.remove(processingOrder.poll());
                queueOverflows++;
            }
            // 将新任务放到待执行队列pendingTasks中
            TaskHolder<ID, T> previousTask = pendingTasks.put(taskHolder.getId(), taskHolder);
            // 如果key不重复保存任务ID到processingOrder中否则记录溢出值+1
            if (previousTask == null) {
                processingOrder.add(taskHolder.getId());
            } else {
                overriddenTasks++;
            }
        }


        void assignSingleItemWork() {
            if (!processingOrder.isEmpty()) {
                if (singleItemWorkRequests.tryAcquire(1)) {
                    long now = System.currentTimeMillis();
                    // 遍历待执行任务队列pendingTasks
                    // 取出第一个未过期的任务保存到singleItemWorkQueue中
                    // 对于过期的任务之间记录
                    while (!processingOrder.isEmpty()) {
                        ID id = processingOrder.poll();
                        TaskHolder<ID, T> holder = pendingTasks.remove(id);
                        // 保存第一个未过期的任务
                        if (holder.getExpiryTime() > now) {
                            singleItemWorkQueue.add(holder);
                            return;
                        }
                        expiredTasks++;
                    }
                    singleItemWorkRequests.release();
                }
            }
        }

        // 将任务从待执行队列pendingTasks中取出记录到batchWorkQueue中
        void assignBatchWork() {
            // 判断是否有待执行的任务
            if (hasEnoughTasksForNextBatch()) {
                if (batchWorkRequests.tryAcquire(1)) {
                    long now = System.currentTimeMillis();
                    // 获取一次发送任务的数量，最多250个
                    int len = Math.min(maxBatchingSize, processingOrder.size());
                    List<TaskHolder<ID, T>> holders = new ArrayList<>(len);
                    while (holders.size() < len && !processingOrder.isEmpty()) {
                        // 获取任务ID
                        ID id = processingOrder.poll();
                        TaskHolder<ID, T> holder = pendingTasks.remove(id);
                        // 任务未过期，记录到holders
                        // 过期直接丢弃并记录
                        if (holder.getExpiryTime() > now) {
                            holders.add(holder);
                        } else {
                            expiredTasks++;
                        }
                    }
                    // 没有待执行任务
                    if (holders.isEmpty()) {
                        batchWorkRequests.release();
                    } else {
                        batchSizeMetric.record(holders.size(), TimeUnit.MILLISECONDS);
                        // 将一批任务保存到batchWorkQueue中
                        batchWorkQueue.add(holders);
                    }
                }
            }
        }

        // 判断是否有待执行任务
        // 有待执行任务则判断是否超过阈值1000
        // 如果没有超过阈值1000，则判断待执行队列中是否有延迟超过500ms的任务
        private boolean hasEnoughTasksForNextBatch() {
            if (processingOrder.isEmpty()) {
                return false;
            }
            if (pendingTasks.size() >= maxBufferSize) {
                return true;
            }
            // 取出延迟最久的节点
            TaskHolder<ID, T> nextHolder = pendingTasks.get(processingOrder.peek());
            // 获取与当前时间的延迟
            long delay = System.currentTimeMillis() - nextHolder.getSubmitTimestamp();
            // 如果超过最大延迟时间，则直接返回true
            return delay >= maxBatchingDelay;// maxBatchingDelay默认500
        }
    }
}
