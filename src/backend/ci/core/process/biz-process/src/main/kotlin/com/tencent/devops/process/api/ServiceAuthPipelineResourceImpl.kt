package com.tencent.devops.process.api

import com.tencent.devops.common.api.pojo.Result
import com.tencent.devops.common.pipeline.enums.ChannelCode
import com.tencent.devops.common.web.RestResource
import com.tencent.devops.process.api.service.ServiceAuthPipelineResource
import com.tencent.devops.process.engine.pojo.PipelineInfo
import com.tencent.devops.process.service.PipelineListFacadeService
import com.tencent.devops.process.pojo.classify.PipelineViewPipelinePage
import com.tencent.devops.process.pojo.pipeline.SimplePipeline
import org.springframework.beans.factory.annotation.Autowired

@RestResource
class ServiceAuthPipelineResourceImpl @Autowired constructor(
    private val pipelineListFacadeService: PipelineListFacadeService
) : ServiceAuthPipelineResource {

    override fun pipelineList(
        projectId: String,
        offset: Int?,
        limit: Int?,
        channelCode: ChannelCode?
    ): Result<PipelineViewPipelinePage<PipelineInfo>> {
        return Result(pipelineListFacadeService.getPipelinePage(projectId = projectId, limit = limit, offset = offset))
    }

    override fun pipelineInfos(pipelineIds: Set<String>): Result<List<SimplePipeline>?> {
        return Result(pipelineListFacadeService.getPipelineByIds(pipelineIds = pipelineIds))
    }

    override fun searchPipelineInstances(
        projectId: String,
        offset: Int?,
        limit: Int?,
        pipelineName: String
    ): Result<PipelineViewPipelinePage<PipelineInfo>> {
        return Result(pipelineListFacadeService.searchByPipelineName(
                projectId = projectId,
                pipelineName = pipelineName,
                limit = limit,
                offset = offset
        ))
    }
}
