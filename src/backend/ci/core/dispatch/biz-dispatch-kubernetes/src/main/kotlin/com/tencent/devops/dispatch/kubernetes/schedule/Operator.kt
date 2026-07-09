package com.tencent.devops.scheduler.schedule

/**
 * 基础接口
 */
interface Operator {
    /**
     * 操作名称
     */
    fun name(): String

    /**
     * 初始化操作
     */
    fun init()

    /**
     * 判断是否使用该操作
     * @param context 执行上下文
     */
    fun isUse(context: ScheduleContext): Boolean
}