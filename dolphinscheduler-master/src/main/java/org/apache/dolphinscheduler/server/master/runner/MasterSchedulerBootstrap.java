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

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.lifecycle.ServerLifeCycleManager;
import org.apache.dolphinscheduler.common.thread.BaseDaemonThread;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.server.master.cache.ProcessInstanceExecCacheManager;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.event.WorkflowEvent;
import org.apache.dolphinscheduler.server.master.event.WorkflowEventQueue;
import org.apache.dolphinscheduler.server.master.event.WorkflowEventType;
import org.apache.dolphinscheduler.server.master.exception.MasterException;
import org.apache.dolphinscheduler.server.master.exception.WorkflowCreateException;
import org.apache.dolphinscheduler.server.master.metrics.MasterServerMetrics;
import org.apache.dolphinscheduler.server.master.metrics.ProcessInstanceMetrics;
import org.apache.dolphinscheduler.server.master.registry.MasterSlotManager;
import org.apache.dolphinscheduler.service.command.CommandService;

import org.apache.commons.collections4.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/***
 * master 调度线程消费commands
 */

/***
 * q  继承AutoCloseable 接口有什么好处？  通过实现AutoCloseable 接口，可以在try-with-resources 语句中使用
 */
@Service
@Slf4j
public class MasterSchedulerBootstrap extends BaseDaemonThread implements AutoCloseable {
    /**
     * Master scheduler thread, this thread will consume the commands from database and trigger processInstance executed.
     */
    @Autowired
    private CommandService commandService;

    @Autowired
    private MasterConfig masterConfig;

    @Autowired
    private ProcessInstanceExecCacheManager processInstanceExecCacheManager;

    @Autowired
    private WorkflowExecuteRunnableFactory workflowExecuteRunnableFactory;
    //消费完command 生成的待执行的工作流Runable会放在这个队列中
    @Autowired
    private WorkflowEventQueue workflowEventQueue;

    @Autowired
    private WorkflowEventLooper workflowEventLooper;

    @Autowired
    private MasterSlotManager masterSlotManager;

    @Autowired
    private MasterTaskExecutorBootstrap masterTaskExecutorBootstrap;

    protected MasterSchedulerBootstrap() {
        //调用父类的构造器，把该线程设置成守护线程，通过继承抽象类的方式来把线程设置成守护线程
        // 这也是一个值得学习的地方，调用父类的构造器，这样子类就拥有了继承过来的父类的属性了
        super("MasterCommandLoopThread");
    }

    @Override
    public synchronized void start() {
        log.info("MasterSchedulerBootstrap starting..");
        //运行该线程的run 方法
        //  这也是一个值得学习的地方，通过调用父类的start 方法，来启动线程，这样才会真正的运行run 方法，run方法消费数据库中的command，将事件加入到WorkflowEventQueue 队列中
        //1、启动扫描command 封装workerFlowEvent到队列中workflowEventLooper
        super.start();
        //在command扫描线程[run方法中运行]中启动了workflowEventLooper线程用于消费workerFlowEvent。也是一个守护线程用户消费WorkflowEventQueue中事件
        //2、启动消费workflowEventLooper线程用于消费workerFlowEvent 线程，提交源头任务到任务优先级队列中
        workflowEventLooper.start();

        masterTaskExecutorBootstrap.start();
        log.info("MasterSchedulerBootstrap started...");
    }
    // q:  实现了AutoCloseable 接口的close 方法有什么作用？   通过实现AutoCloseable 接口，可以在try-with-resources 语句中使用
    @Override
    public void close() throws Exception {
        log.info("MasterSchedulerBootstrap stopping...");
        try (
                final WorkflowEventLooper workflowEventLooper1 = workflowEventLooper;
                final MasterTaskExecutorBootstrap masterTaskExecutorBootstrap1 = masterTaskExecutorBootstrap) {
            // closed the resource
        }
        log.info("MasterSchedulerBootstrap stopped...");
    }

    /**
     * run of MasterSchedulerService
     */
    /***
     * 1. 从数据库中获取command
     * 2. 判断当前服务是否处于运行状态，如果不是则休眠1s
     * 3. 判断当前服务是否处于过载状态，如果是则休眠1s
     * 4. 如果获取到command，则并行处理command
     * 5. 如果处理成功，则将workflowInstance放入到缓存中
     * 6. 将workflowInstance放入到workflowEventQueue中
     * 7. 如果处理失败，则将command移动到错误表中
     * 8. 如果没有获取到command，则休眠1s
     * 9. 如果线程被中断，则退出循环
     * 10. 如果出现异常，则休眠1s
     *
     */
    @Override
    public void run() {
        while (!ServerLifeCycleManager.isStopped()) {
            try {
                //如果服务不是运行状态需要休眠1s
                if (!ServerLifeCycleManager.isRunning()) {
                    // the current server is not at running status, cannot consume command.
                    log.warn("The current server is not at running status, cannot consumes commands.");
                    Thread.sleep(Constants.SLEEP_TIME_MILLIS);
                }
                // todo: if the workflow event queue is much, we need to handle the back pressure
                boolean isOverload =
                        OSUtils.isOverload(masterConfig.getMaxCpuLoadAvg(), masterConfig.getReservedMemory());
                if (isOverload) {
                    log.warn("The current server is overload, cannot consumes commands.");
                    MasterServerMetrics.incMasterOverload();
                    Thread.sleep(Constants.SLEEP_TIME_MILLIS);
                    continue;
                }
                List<Command> commands = findCommands();
                if (CollectionUtils.isEmpty(commands)) {
                    // indicate that no command ,sleep for 1s
                    Thread.sleep(Constants.SLEEP_TIME_MILLIS);
                    continue;
                }
                // 这也是一个值得学习的地方，通过并行流的方式来处理command，加快处理速度
                //这是调度器的核心逻辑，通过并行流的方式来消费command，生产数据
                commands.parallelStream()
                        .forEach(command -> {
                            try {
                                Optional<WorkflowExecuteRunnable> workflowExecuteRunnableOptional =
                                        workflowExecuteRunnableFactory.createWorkflowExecuteRunnable(command);
                                if (!workflowExecuteRunnableOptional.isPresent()) {
                                    log.warn(
                                            "The command execute success, will not trigger a WorkflowExecuteRunnable, this workflowInstance might be in serial mode");
                                   //lambda表达式本质上是一个函数
                                    return;
                                }
                                WorkflowExecuteRunnable workflowExecuteRunnable = workflowExecuteRunnableOptional.get();
                                ProcessInstance processInstance = workflowExecuteRunnable
                                        .getWorkflowExecuteContext().getWorkflowInstance();
                                if (processInstanceExecCacheManager.contains(processInstance.getId())) {
                                    log.error(
                                            "The workflow instance is already been cached, this case shouldn't be happened");
                                }
                                //将workflowInstance放入到缓存中
                                processInstanceExecCacheManager.cache(processInstance.getId(), workflowExecuteRunnable);
                                //关键的一步，将事件添加到工作流线程队列中
                                workflowEventQueue.addEvent(
                                        new WorkflowEvent(WorkflowEventType.START_WORKFLOW, processInstance.getId()));
                            } catch (WorkflowCreateException workflowCreateException) {
                                log.error("Master handle command {} error ", command.getId(), workflowCreateException);
                                commandService.moveToErrorCommand(command, workflowCreateException.toString());
                            }
                        });
                MasterServerMetrics.incMasterConsumeCommand(commands.size());
            } catch (InterruptedException interruptedException) {
                log.warn("Master schedule bootstrap interrupted, close the loop", interruptedException);
                // 恢复中断标志
                Thread.currentThread().interrupt();
                break;
            //把底层其他框架抛出的异常catch住，改写成自己的异常，囊入自己的领域，排查其问题更容易
            } catch (Exception e) {
                log.error("Master schedule workflow error", e);
                // sleep for 1s here to avoid the database down cause the exception boom
                ThreadUtils.sleep(Constants.SLEEP_TIME_MILLIS);
            }
        }
    }

    //主流程查询数据库是需要catch 住异常的
    private List<Command> findCommands() throws MasterException {
        try {
            //获取当前时间
            long scheduleStartTime = System.currentTimeMillis();
            //获取当前的slot
            int thisMasterSlot = masterSlotManager.getSlot();
            //获取master的数量
            int masterCount = masterSlotManager.getMasterSize();
            /***
             * 如果master的数量小于等于0，则返回空集合
             */
            if (masterCount <= 0) {
                log.warn("Master count: {} is invalid, the current slot: {}", masterCount, thisMasterSlot);
                // 这是一个很好的习惯。如果返回的集合是空的，那么就返回一个空的集合，而不是null。
                return Collections.emptyList();
            }
            //获取每次从数据库中获取command的数量
            int pageSize = masterConfig.getFetchCommandNum();
            //  每台shceduler 实例根据自己的槽位来获取自己的command,并且采用分页查询
            //q 这里返回值写成final有什么好处？
            final List<Command> result =
                    commandService.findCommandPageBySlot(pageSize, masterCount, thisMasterSlot);
            /***
             * 如果获取到command，则打印日志
             * 计算获取command的时间
             * 记录获取command的时间
             * 返回获取到的command
             */
            if (CollectionUtils.isNotEmpty(result)) {
                long cost = System.currentTimeMillis() - scheduleStartTime;
                log.info(
                        "Master schedule bootstrap loop command success, fetch command size: {}, cost: {}ms, current slot: {}, total slot size: {}",
                        result.size(), cost, thisMasterSlot, masterCount);

                ProcessInstanceMetrics.recordCommandQueryTime(cost);
            }
            return result;
        } catch (Exception ex) {
            throw new MasterException("Master loop command from database error", ex);
        }
    }

}
