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

import org.apache.dolphinscheduler.common.enums.SlotCheckState;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.graph.IWorkflowGraph;
import org.apache.dolphinscheduler.server.master.graph.WorkflowGraphFactory;
import org.apache.dolphinscheduler.server.master.metrics.ProcessInstanceMetrics;
import org.apache.dolphinscheduler.server.master.registry.MasterSlotManager;
import org.apache.dolphinscheduler.service.exceptions.CronParseException;
import org.apache.dolphinscheduler.service.process.ProcessService;

import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class WorkflowExecuteContextFactory {

    @Autowired
    private MasterSlotManager masterSlotManager;

    @Autowired
    private ProcessService processService;

    @Autowired
    private WorkflowGraphFactory workflowGraphFactory;

    @Autowired
    private MasterConfig masterConfig;

    public Optional<IWorkflowExecuteContext> createWorkflowExecuteRunnableContext(Command command) throws Exception {
        //创建工作流实例
        Optional<ProcessInstance> workflowInstanceOptional = createWorkflowInstance(command);
        //如果没有创建成功，就返回空容器回去
        if (!workflowInstanceOptional.isPresent()) {
            return Optional.empty();
        }
        ProcessInstance workflowInstance = workflowInstanceOptional.get();
        //根据工作流实例查询工作流定义
        ProcessDefinition workflowDefinition = processService.findProcessDefinition(
                workflowInstance.getProcessDefinitionCode(), workflowInstance.getProcessDefinitionVersion());
       //q:创建工作流实例的时候设置了工作流定义，这里为什么还要再设置一次？ a: 为了保证数据的一致性，这里再设置一次。
       //创建实例的过程中可能工作流定义发生了改变，所以这里再设置一次
        workflowInstance.setProcessDefinition(workflowDefinition);

        IWorkflowGraph workflowGraph = workflowGraphFactory.createWorkflowGraph(workflowInstance);
       //返回一个工作流执行上下文，工作流上下文包含了工作流定义，工作流实例，工作流图
        return Optional.of(new WorkflowExecuteContext(workflowDefinition, workflowInstance, workflowGraph));
    }

    private Optional<ProcessInstance> createWorkflowInstance(Command command) throws CronParseException {
        long commandTransformStartTime = System.currentTimeMillis();
        // Note: this check is not safe, the slot may change after command transform.
        // We use the database transaction in `handleCommand` so that we can guarantee the command will
        // always be executed
        // by only one master
        //检查该命令是不是是不是符合该机器的槽位，检查状态最好返回一个枚举
        SlotCheckState slotCheckState = slotCheck(command);
        //如果槽位发生改变或者master的机器数量重新统计发现发生了改变，就直接跑出异常
        //q: 为什么枚举比较不需要重写equals方法？ a: 枚举是单例的，所以可以直接用==来比较
        if (slotCheckState.equals(SlotCheckState.CHANGE) || slotCheckState.equals(SlotCheckState.INJECT)) {
            log.info("Master handle command {} skip, slot check state: {}", command.getId(), slotCheckState);
            throw new RuntimeException("Slot check failed the current state: " + slotCheckState);
        }
        //处理命令,创建工作流实例
        ProcessInstance processInstance = processService.handleCommand(masterConfig.getMasterAddress(), command);
        //工具类
        ProcessInstanceMetrics
                .recordProcessInstanceGenerateTime(System.currentTimeMillis() - commandTransformStartTime);
        // 这里是一个值得学习的地方，把一个对象放入到Optional中，然后返回。不需要提前去判空，在真正消费的地方再去判空
        return Optional.ofNullable(processInstance);
    }

    // 检查状态的时候返回一个枚举
    private SlotCheckState slotCheck(Command command) {
        int slot = masterSlotManager.getSlot();
        int masterSize = masterSlotManager.getMasterSize();
        SlotCheckState state;
        if (masterSize <= 0) {
            state = SlotCheckState.CHANGE;
        } else if (command.getId() % masterSize == slot) {
            state = SlotCheckState.PASS;
        } else {
            state = SlotCheckState.INJECT;
        }
        return state;
    }

}
