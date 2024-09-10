/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.server.master.runner;

import org.apache.dolphinscheduler.common.thread.BaseDaemonThread;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.server.master.runner.dispatcher.TaskDispatchFactory;
import org.apache.dolphinscheduler.server.master.runner.dispatcher.TaskDispatcher;
import org.apache.dolphinscheduler.server.master.runner.execute.DefaultTaskExecuteRunnable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/***
 * 任务的分配
 */
@Slf4j
@Component
public class GlobalTaskDispatchWaitingQueueLooper extends BaseDaemonThread implements AutoCloseable {

    @Autowired
    private GlobalTaskDispatchWaitingQueue globalTaskDispatchWaitingQueue;

    @Autowired
    private TaskDispatchFactory taskDispatchFactory;

    private final AtomicBoolean RUNNING_FLAG = new AtomicBoolean(false);

    private final AtomicInteger DISPATCHED_TIMES = new AtomicInteger();

    private static final Integer MAX_DISPATCHED_FAILED_TIMES = 100;
    //子类只会默认调用父类的无参构造方法
    public GlobalTaskDispatchWaitingQueueLooper() {
        super("GlobalTaskDispatchWaitingQueueLooper");
    }

    @Override
    public synchronized void start() {
        //先启动开关
        if (!RUNNING_FLAG.compareAndSet(false, true)) {
            log.error("The GlobalTaskDispatchWaitingQueueLooper already started, will not start again");
            return;
        }
        //再启动线程
        log.info("GlobalTaskDispatchWaitingQueueLooper starting...");
        super.start();
        log.info("GlobalTaskDispatchWaitingQueueLooper started...");
    }

    @Override
    public void run() {
        DefaultTaskExecuteRunnable defaultTaskExecuteRunnable;
        while (RUNNING_FLAG.get()) {
            try {
                // 获取队列中的task 使用take方法来获取，阻塞的方法
                defaultTaskExecuteRunnable = globalTaskDispatchWaitingQueue.takeNeedToDispatchTaskExecuteRunnable();
            } catch (InterruptedException e) {
                log.warn("Get waiting dispatch task failed, the current thread has been interrupted, will stop loop");
                Thread.currentThread().interrupt();
                break;
            }
            try {
                final TaskDispatcher taskDispatcher = taskDispatchFactory
                        .getTaskDispatcher(defaultTaskExecuteRunnable.getTaskInstance().getTaskType());
                taskDispatcher.dispatchTask(defaultTaskExecuteRunnable);
                //成功了这个值置为0
                DISPATCHED_TIMES.set(0);
            } catch (Exception e) {
                //记录分配任务失败的次数
                defaultTaskExecuteRunnable.getTaskExecutionContext().increaseDispatchFailTimes();
                //重新添加到队列中。添加任务到worker的队列失败了重新放入到队列中
                globalTaskDispatchWaitingQueue.submitNeedToDispatchTaskExecuteRunnable(defaultTaskExecuteRunnable);
               //经常失败就会休眠
                if (DISPATCHED_TIMES.incrementAndGet() > MAX_DISPATCHED_FAILED_TIMES) {
                    ThreadUtils.sleep(10 * 1000L);
                }
                // 打印异常信息
                log.error("Dispatch task failed", e);
            }
        }
        log.info("GlobalTaskDispatchWaitingQueueLooper started...");
    }

    @Override
    public void close() throws Exception {
        if (RUNNING_FLAG.compareAndSet(true, false)) {
            log.info("GlobalTaskDispatchWaitingQueueLooper stopping...");
            log.info("GlobalTaskDispatchWaitingQueueLooper stopped...");
        }
    }
}
