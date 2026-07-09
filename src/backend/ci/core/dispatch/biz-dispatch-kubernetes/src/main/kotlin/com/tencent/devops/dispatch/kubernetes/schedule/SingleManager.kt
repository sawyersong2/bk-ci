package com.tencent.devops.scheduler.schedule

interface SingleManager {
    fun run(scheduleContext: ScheduleContext, inList: List<String>): Any?
}