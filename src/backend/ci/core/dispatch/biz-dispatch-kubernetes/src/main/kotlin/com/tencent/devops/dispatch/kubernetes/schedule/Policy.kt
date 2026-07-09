package com.tencent.devops.scheduler.schedule

/**
 * 策略接口
 */
interface Policy : Operator {
    /**
     * 执行策略
     * @param context 执行上下文
     * @return Pair<结果对象, 异常对象>
     */
    fun execute(context: ScheduleContext): Any?
}