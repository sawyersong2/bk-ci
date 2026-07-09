package com.tencent.devops.scheduler.schedule

/**
 * 资源过滤器接口
 */
interface ResourceFilter : Operator {
    /**
     * 执行过滤逻辑
     * @param context 执行上下文
     * @param inList 输入资源列表
     * @return Pair<过滤后的资源列表, 异常对象>
     */
    fun filter(context: ScheduleContext, inList: List<String>): List<String>
}