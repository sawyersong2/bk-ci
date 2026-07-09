package com.tencent.devops.scheduler.schedule

import com.tencent.devops.model.cloudgame.tables.records.TGameRecord
import com.tencent.devops.scheduler.pojo.DirectAllocRequest
import com.tencent.devops.scheduler.pojo.GameCfg
import com.tencent.devops.scheduler.pojo.ResourceResult
import com.tencent.devops.scheduler.pojo.SessionInstance
import com.tencent.devops.scheduler.pojo.proxy.AllocProxyResponse
import com.tencent.devops.scheduler.schedule.profile.MutableSearchProfile
import java.util.concurrent.ConcurrentHashMap

data class ScheduleContext(
    var req: DirectAllocRequest,
    var gameConfig: GameCfg? = null,
    var telecomType: Int = 0,
    var isMainTelecom: Boolean = true,
    var filterMgr: ListManager? = null,
    var filterMapMgr: ConcurrentHashMap<String, ListManager> = ConcurrentHashMap(),
    var allocMgr: SingleManager? = null,
    var skipProxyAllocFilter: Boolean = false,
    var proxyRegion: String = "",
    var allocProxyResponse: AllocProxyResponse? = null,
    var allocResult: ResourceResult? = null,
    var speedTestAllocProxyPolicy: Boolean = false,
    var proxyFilterTempRedisKey: String= "",
    var backupProxyList: List<String> = listOf(),
    var cdsFilterTempRedisKey: String = "",
    var sessionInstance: SessionInstance? = null,
    var isForPresentation: Boolean = false,
    var searchProfile: MutableSearchProfile? = null
)
