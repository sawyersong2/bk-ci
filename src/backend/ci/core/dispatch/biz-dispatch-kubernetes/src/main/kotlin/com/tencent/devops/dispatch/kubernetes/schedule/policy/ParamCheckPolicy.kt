package com.tencent.devops.scheduler.schedule.policy

import com.tencent.devops.common.api.exception.ErrorCodeException
import com.tencent.devops.scheduler.constant.ErrorMessageCode
import com.tencent.devops.scheduler.dao.GameDao
import com.tencent.devops.scheduler.pojo.ClientType
import com.tencent.devops.scheduler.schedule.Policy
import com.tencent.devops.scheduler.schedule.ScheduleContext
import com.tencent.devops.scheduler.schedule.TelecomDefines
import org.jooq.DSLContext
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

@Component
class ParamCheckPolicy @Autowired constructor(
    private val gameDao: GameDao,
    @Qualifier("cloudgameDslContext")
    private val cloudgameDslContext: DSLContext,
) : Policy {
    override fun execute(context: ScheduleContext): Any? {
        val req = context.req
        // 参数基础校验
        when {
            req.cdsId.isBlank() -> throw ErrorCodeException(
                    errorCode = ErrorMessageCode.SCHEDULE_ERROR,
                    defaultMessage = "CDS ID is empty"
            )
            req.userId.isBlank() && req.instanceId.isBlank() -> throw ErrorCodeException(
                errorCode = ErrorMessageCode.SCHEDULE_ERROR,
                defaultMessage = "UserId and InstanceId are both empty"
            )
            req.gameId.isBlank() || req.zoneId.isBlank() || req.clientVersion.isBlank() -> throw ErrorCodeException(
                errorCode = ErrorMessageCode.SCHEDULE_ERROR,
                defaultMessage = "GameId|region|clientVersion is empty"
            )
        }


        // 加载云桌面配置
        val gameConfig = gameDao.getGameResource(cloudgameDslContext, req.gameId.toInt()) ?: throw ErrorCodeException(
            errorCode = ErrorMessageCode.SCHEDULE_ERROR,
            defaultMessage = "Load game failed, gameId=${req.gameId}"
        )

        context.apply {
            context.gameConfig = gameConfig
            val (telecomType, isMain) = TelecomDefines.parseTelecomType(req.telecom)
            context.telecomType = telecomType
            context.isMainTelecom = isMain

        }

        // 这一步有待优化,可以前置处理 // TODO
        if (req.clientType == ClientType.UnknownClient) {
            req.clientType = ClientType.ClientTypeWindowsPC
        }

        return null
    }

    override fun name(): String {
        return "ParamCheck"
    }

    override fun init() {

    }

    override fun isUse(context: ScheduleContext): Boolean {
        return true
    }
}