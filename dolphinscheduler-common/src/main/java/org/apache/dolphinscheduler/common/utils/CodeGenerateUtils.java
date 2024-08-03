/** Copyright 2010-2012 Twitter, Inc.*/

package org.apache.dolphinscheduler.common.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

/**
 *  Rewriting based on Twitter snowflake algorithm
 */
public class CodeGenerateUtils {

    // start timestamp
    private static final long START_TIMESTAMP = 1609430400000L; // 2021-01-01 00:00:00
    // Each machine generates 32 in the same millisecond
    private static final long LOW_DIGIT_BIT = 5L;
    private static final long MIDDLE_BIT = 2L;
    /****
     *  -1L:     1111  1111
     *  -1L<<5:  1111  1111 00000
     *  ~取反：   0000 0000 11111  （低5位的最大值 16+8+4+2+1=31）
     *  32：     0000  0001 0000
     *  &-------------------------
     *  等于：   00000 0000 0000
     */
    private static final long MAX_LOW_DIGIT = ~(-1L << LOW_DIGIT_BIT);
    // The displacement to the left
    private static final long MIDDLE_LEFT = LOW_DIGIT_BIT;
    private static final long HIGH_DIGIT_LEFT = LOW_DIGIT_BIT + MIDDLE_BIT;
    private final long machineHash;
    private long lowDigit = 0L;
    private long recordMillisecond = -1L;
    // 毫秒
    private static final long SYSTEM_TIMESTAMP = System.currentTimeMillis();
    private static final long SYSTEM_NANOTIME = System.nanoTime();

    private CodeGenerateUtils() throws CodeGenerateException {
        try {
            this.machineHash =
                    Math.abs(Objects.hash(InetAddress.getLocalHost().getHostName())) % (2 << (MIDDLE_BIT - 1));
        } catch (UnknownHostException e) {
            throw new CodeGenerateException(e.getMessage());
        }
    }

    private static CodeGenerateUtils instance = null;
    // 创建一个单例的实例，直接加synchronized
    // 单例获取code 实例
    public static synchronized CodeGenerateUtils getInstance() throws CodeGenerateException {
        if (instance == null) {
            instance = new CodeGenerateUtils();
        }
        return instance;
    }
    // 雪花算法
    public synchronized long genCode() throws CodeGenerateException {
        // 获取当前时间
        long nowtMillisecond = systemMillisecond();
        // 如果当前时间小于上次记录的时间 非法情况抛出异常
        if (nowtMillisecond < recordMillisecond) {
            throw new CodeGenerateException("New code exception because time is set back.");
        }
        // 当前时间为同1毫秒
        if (nowtMillisecond == recordMillisecond) {

            lowDigit = (lowDigit + 1) & MAX_LOW_DIGIT;

            // 同一毫秒内已经达到了低5位的序列号最大值，需要在下一个毫秒内查询序列号
            if (lowDigit == 0L) {
                // 获取下一个毫秒值
                while (nowtMillisecond <= recordMillisecond) {
                    nowtMillisecond = systemMillisecond();
                }
            }
            // 下一个毫秒值 序列号从0还是计算
        } else {
            lowDigit = 0L;
        }
        // 存档上一个毫秒值
        recordMillisecond = nowtMillisecond;
        return (nowtMillisecond - START_TIMESTAMP) << HIGH_DIGIT_LEFT // 时间戳部分
                | machineHash << MIDDLE_LEFT // 机器部分
                | lowDigit; // 低位序列号部分
    }

    private long systemMillisecond() {
        // System.nanoTime() 返回纳秒 毫秒 微秒 纳秒
        return SYSTEM_TIMESTAMP + (System.nanoTime() - SYSTEM_NANOTIME) / 1000000;
    }

    public static class CodeGenerateException extends RuntimeException {

        public CodeGenerateException(String message) {
            super(message);
        }
    }
}
