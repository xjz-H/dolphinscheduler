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

package org.apache.dolphinscheduler.common.log.remote;

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.factory.OssClientFactory;
import org.apache.dolphinscheduler.common.model.OssConnection;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;

import org.apache.commons.lang3.StringUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.PutObjectRequest;

@Slf4j
public class OssRemoteLogHandler implements RemoteLogHandler, Closeable {

    private OSS ossClient;

    private String bucketName;

    private static OssRemoteLogHandler instance;

    private OssRemoteLogHandler() {

    }
    // 一个简单的单例模式，直接在静态方法上加上synchronized ，遵循happens-before规则，解锁之前的操作对后序的加锁可见。整个方法加同步控制，instance 不需要加volatile
    // 加上这个类没有其他的地方提供获取instance 字段。所以不需要加volatile
    public static synchronized OssRemoteLogHandler getInstance() {
        if (instance == null) {
            instance = new OssRemoteLogHandler();
            instance.init();
        }

        return instance;
    }

    public void init() {
        String accessKeyId = readOssAccessKeyId();
        String accessKeySecret = readOssAccessKeySecret();
        String endpoint = readOssEndpoint();
        ossClient = OssClientFactory.buildOssClient(new OssConnection(accessKeyId, accessKeySecret, endpoint));

        bucketName = readOssBucketName();
        checkBucketNameExists(bucketName);
    }

    @Override
    public void sendRemoteLog(String logPath) {
        String objectName = RemoteLogUtils.getObjectNameFromLogPath(logPath);

        try {
            log.info("send remote log {} to OSS {}", logPath, objectName);
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectName, new File(logPath));
            ossClient.putObject(putObjectRequest);
        } catch (Exception e) {
            log.error("error while sending remote log {} to OSS {}", logPath, objectName, e);
        }
    }

    @Override
    public void getRemoteLog(String logPath) {
        String objectName = RemoteLogUtils.getObjectNameFromLogPath(logPath);

        try {
            log.info("get remote log on OSS {} to {}", objectName, logPath);
            // 把远程oss对象的远程对象写入到本地文件
            ossClient.getObject(new GetObjectRequest(bucketName, objectName), new File(logPath));
        } catch (Exception e) {
            log.error("error while getting remote log on OSS {} to {}", objectName, logPath, e);
        }
    }

    @Override
    public void close() throws IOException {
        if (ossClient != null) {
            ossClient.shutdown();
        }
    }

    private void checkBucketNameExists(String bucketName) {
        if (StringUtils.isBlank(bucketName)) {
            throw new IllegalArgumentException(Constants.REMOTE_LOGGING_OSS_BUCKET_NAME + " is empty");
        }

        boolean existsBucket = ossClient.doesBucketExist(bucketName);
        if (!existsBucket) {
            throw new IllegalArgumentException(
                    "bucketName: " + bucketName + " is not exists, you need to create them by yourself");
        }

        log.info("bucketName: {} has been found", bucketName);
    }

    private String readOssAccessKeyId() {
        return PropertyUtils.getString(Constants.REMOTE_LOGGING_OSS_ACCESS_KEY_ID);
    }

    private String readOssAccessKeySecret() {
        return PropertyUtils.getString(Constants.REMOTE_LOGGING_OSS_ACCESS_KEY_SECRET);
    }

    private String readOssEndpoint() {
        return PropertyUtils.getString(Constants.REMOTE_LOGGING_OSS_ENDPOINT);
    }

    private String readOssBucketName() {
        return PropertyUtils.getString(Constants.REMOTE_LOGGING_OSS_BUCKET_NAME);
    }
}
