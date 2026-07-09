package com.tencent.devops.scheduler.schedule.proxy

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.tencent.devops.scheduler.dao.ProxyTreeDao
import org.jooq.DSLContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service

@Component
class MetaMgr @Autowired constructor(
    private val proxyTreeDao: ProxyTreeDao,
    private val dslContext: DSLContext
) {

    companion object {
        private val logger = LoggerFactory.getLogger(MetaMgr::class.java)
    }

    // 加载元数据
    fun load(): List<Meta> {
        // 加载最大版本
        val (id, version) = loadMaxVersion()

        // 加载层级定义
        val levelDefines = proxyTreeDao.loadLevelDefines(dslContext, id)
        return definesToMeta(levelDefines)
    }

    private fun loadMaxVersion(): Pair<Int, String> {
/*        val maxVersion = transaction {
            treeTable.select { treeTable.bizType eq bizType }
                .orderBy(treeTable.version to false)
                .limit(1)
                .singleOrNull()
                ?.get(treeTable.version)
        } ?: throw IllegalStateException("未找到最大版本")

        val treeId = transaction {
            treeTable.select { (treeTable.bizType eq bizType) and (treeTable.version eq maxVersion) }
                .single()[treeTable.treeId]
        }

        return treeId to maxVersion*/
        return Pair(2, "1.0.0")
    }

    private fun definesToMeta(levelDefines: List<LevelDefine>): List<Meta> {
        val newLevelDefines = levelDefines.toMutableList()
        newLevelDefines.add(LevelDefine(
            defineId = 13,
            defineName = "single_sided_network",
            compareDatatype = 3,
            treeLevel = 1000,
            flags = 2,
            searchField = "singleSidedNetwork",
            loadField = "",
            loadFieldDataType = 0))

        val levels = mutableListOf<Meta>()
        var prev: Meta? = null
        newLevelDefines.forEachIndexed { index, define ->
            val meta = Meta(
                id = define.defineId,
                dataType = NodeDataType.fromValue(define.compareDatatype),
                name = define.defineName,
                level = define.treeLevel,
                flags = NodeFlag.fromValue(define.flags),
                searchFiled = define.searchField,
                parent = prev,
                loadFiled = define.loadField,
                loadFieldDataType = define.loadFieldDataType
            ).apply {
                prev?.child = this
            }

            levels.add(meta)
            prev = meta
        }

        return levels
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class LevelDefine(
    @JsonProperty("define_id")
    var defineId: Int = 0,
    @JsonProperty("define_name")
    var defineName: String = "",
    @JsonProperty("tree_level")
    var treeLevel: Int = 0,
    @JsonProperty("flags")
    var flags: Int = 0,
    @JsonProperty("compare_datatype")
    var compareDatatype: Int = 0,
    @JsonProperty("load_field")
    var loadField: String = "",
    @JsonProperty("load_field_datatype")
    var loadFieldDataType: Int = 0,
    @JsonProperty("search_field")
    var searchField: String = ""
)