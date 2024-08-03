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

package org.apache.dolphinscheduler.dao.repository;

import org.apache.commons.collections4.CollectionUtils;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import lombok.NonNull;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;

/***
 *
 * @param <ENTITY>
 * @param <MYBATIS_MAPPER>
 *     BaseMapper 是一个通用的基础接口或类，它通常用于定义一些通用的数据库操作方法，如增删改查等。
 *
 *     BaseDao是一个泛型类
 *
 *     在Java中，泛型类是指在类或接口中使用泛型类型参数的类。通过泛型类，可以实现对数据类型的参数化，使得类中的字段、方法或构造函数可以使用这个泛型类型参数来代表具体的数据类型。泛型类可以提高代码的灵活性和复用性，同时可以在编译期间进行类型检查，避免在运行时出现类型转换错误。
 *
 *    泛型类的定义格式如下：
 *    public class GenericClass<T> {
 *     private T data;
 *
 *     public GenericClass(T data) {
 *         this.data = data;
 *     }
 *
 *     public T getData() {
 *         return data;
 *     }
 *   }
 *
 *   在上述代码中，GenericClass 是一个泛型类，<T> 表示泛型类型参数，可以在类中使用这个类型参数。在类中，可以将泛型类型参数 T 用作字段类型、方法返回类型或方法参数类型。在实例化泛型类时，需要指定具体的类型参数，例如：
 *   GenericClass<Integer> intGeneric = new GenericClass<>(10);
 *   Integer data = intGeneric.getData();
 *   System.out.println(data); // 输出 10
 *   通过泛型类，可以实现对不同数据类型的通用处理，提高代码的重用性和可维护性。常见的泛型类有集合类如 ArrayList<T>、HashMap<K, V> 等，以及自定义的泛型类用于各种类型的数据处理。
 */
public abstract class BaseDao<ENTITY, MYBATIS_MAPPER extends BaseMapper<ENTITY>> implements IDao<ENTITY> {

    protected MYBATIS_MAPPER mybatisMapper;

    public BaseDao(@NonNull MYBATIS_MAPPER mybatisMapper) {
        this.mybatisMapper = mybatisMapper;
    }

    @Override
    public ENTITY queryById(@NonNull Serializable id) {
        return mybatisMapper.selectById(id);
    }

    @Override
    public Optional<ENTITY> queryOptionalById(@NonNull Serializable id) {
        return Optional.ofNullable(queryById(id));
    }

    @Override
    public List<ENTITY> queryByIds(Collection<? extends Serializable> ids) {
        if (CollectionUtils.isEmpty(ids)) {
            return Collections.emptyList();
        }
        return mybatisMapper.selectBatchIds(ids);
    }

    @Override
    public int insert(@NonNull ENTITY model) {
        return mybatisMapper.insert(model);
    }

    @Override
    public void insertBatch(Collection<ENTITY> models) {
        if (CollectionUtils.isEmpty(models)) {
            return;
        }
        for (ENTITY model : models) {
            insert(model);
        }
    }

    @Override
    public boolean updateById(@NonNull ENTITY model) {
        return mybatisMapper.updateById(model) > 0;
    }

    @Override
    public boolean deleteById(@NonNull Serializable id) {
        return mybatisMapper.deleteById(id) > 0;
    }

    @Override
    public boolean deleteByIds(Collection<? extends Serializable> ids) {
        if (CollectionUtils.isEmpty(ids)) {
            return true;
        }
        return mybatisMapper.deleteBatchIds(ids) > 0;
    }

}
