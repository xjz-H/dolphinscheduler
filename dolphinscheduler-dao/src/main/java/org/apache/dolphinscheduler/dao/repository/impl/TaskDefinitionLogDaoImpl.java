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

package org.apache.dolphinscheduler.dao.repository.impl;

import org.apache.dolphinscheduler.dao.entity.ProcessTaskRelation;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskDefinitionLog;
import org.apache.dolphinscheduler.dao.mapper.ProcessTaskRelationLogMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskDefinitionLogMapper;
import org.apache.dolphinscheduler.dao.repository.BaseDao;
import org.apache.dolphinscheduler.dao.repository.TaskDefinitionLogDao;

import org.apache.commons.collections4.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.NonNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 * Task Definition Log DAP implementation
 */
@Repository
public class TaskDefinitionLogDaoImpl extends BaseDao<TaskDefinitionLog, TaskDefinitionLogMapper>
        implements
            TaskDefinitionLogDao {

    @Autowired
    private ProcessTaskRelationLogMapper processTaskRelationLogMapper;

    public TaskDefinitionLogDaoImpl(@NonNull TaskDefinitionLogMapper taskDefinitionLogMapper) {
        super(taskDefinitionLogMapper);
    }

    @Override
    public List<TaskDefinitionLog> queryByWorkflowDefinitionCodeAndVersion(Long workflowDefinitionCode,
                                                                           Integer workflowDefinitionVersion) {

        List<ProcessTaskRelation> processTaskRelationLogs = processTaskRelationLogMapper
                .queryByProcessCodeAndVersion(workflowDefinitionCode, workflowDefinitionVersion)
                .stream()
                .map(p -> (ProcessTaskRelation) p)
                .collect(Collectors.toList());
        return queryTaskDefineLogList(processTaskRelationLogs);
    }

    @Override
    public List<TaskDefinitionLog> queryTaskDefineLogList(List<ProcessTaskRelation> processTaskRelations) {
        // 先对集合判空操作
        if (CollectionUtils.isEmpty(processTaskRelations)) {
            return Collections.emptyList();
        }
        // 过滤出后置任务，也就过滤出了所有的任务定义
        Set<TaskDefinition> taskDefinitionSet = processTaskRelations.stream()
                .filter(p -> p.getPostTaskCode() > 0)
                .map(p -> new TaskDefinition(p.getPostTaskCode(), p.getPostTaskVersion()))
                .collect(Collectors.toSet());
        // 如果后置任务集合为空，返回空集合
        if (CollectionUtils.isEmpty(taskDefinitionSet)) {
            return Collections.emptyList();
        }
        //q: t_ds_task_definition_log 该表在海豚调度器中的作用？ a: 该表用于记录任务定义的日志
        //q: 为什么不直接查询t_ds_task_definition表 而查询t_ds_task_definition_log表？ a: 该表用于记录任务定义的日志
        return mybatisMapper.queryByTaskDefinitions(taskDefinitionSet);
    }

    @Override
    public void deleteByTaskDefinitionCodes(Set<Long> taskDefinitionCodes) {
        if (CollectionUtils.isEmpty(taskDefinitionCodes)) {
            return;
        }
        mybatisMapper.deleteByTaskDefinitionCodes(taskDefinitionCodes);
    }
}
