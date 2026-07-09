package com.tencent.devops.scheduler.schedule

/**
 * 资源分配器接口
 * @see Operator 继承基础操作接口
 */
interface Allocator : Operator {
    /**
     * 执行资源分配逻辑
     * @param context 执行上下文
     * @param inList 输入资源列表
     * @return Pair<分配结果, 异常对象>
     */
    fun alloc(context: ScheduleContext, inList: List<String>): Any
}