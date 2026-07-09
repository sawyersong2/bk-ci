package com.tencent.devops.scheduler.schedule

interface ListManager {
    fun run(ctx: ScheduleContext, inList: List<String>): List<String>
    fun runOneByName(name: String, ctx: ScheduleContext, inList: List<String>): List<String>
    fun runOneByType(type: Class<*>, ctx: ScheduleContext, inList: List<String>): List<String>
}