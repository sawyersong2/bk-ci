USE devops_ci_dispatch_kubernetes;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for T_DISPATCH_MACHINE
-- ----------------------------

-- ----------------------------
-- Table structure for T_DISPATCH_KUBERNETES_BUILD
-- ----------------------------
CREATE TABLE IF NOT EXISTS `T_DISPATCH_KUBERNETES_BUILD` (
     `ID` bigint(20) NOT NULL AUTO_INCREMENT,
     `PROJECT_ID` varchar(64) NOT NULL DEFAULT '' COMMENT '蓝盾项目ID',
     `PIPELINE_ID` varchar(34) NOT NULL DEFAULT '' COMMENT '流水线ID',
     `VM_SEQ_ID` varchar(34) NOT NULL DEFAULT '' COMMENT '流水线CONTAINER_ID',
     `POOL_NO` int(11) NOT NULL DEFAULT 1 COMMENT '构件池ID',
     `CONTAINER_NAME` varchar(256) NOT NULL DEFAULT '' COMMENT '构建容器ID',
     `CONTAINER_IMAGE` varchar(1024) NOT NULL DEFAULT '' COMMENT '构建镜像',
     `STATUS` int(11) NOT NULL DEFAULT 0 COMMENT '构建状态',
     `USER_ID` varchar(34) NOT NULL DEFAULT '' COMMENT '用户标识',
     `DEBUG_STATUS` bit(1) DEFAULT b'0' COMMENT '是否处于debug状态',
     `DEBUG_TIME` timestamp NULL DEFAULT NULL COMMENT 'debug开始时间',
     `CPU` float NOT NULL DEFAULT 1 COMMENT '构建容器CPU配额',
     `MEMORY` varchar(64) NOT NULL DEFAULT 1 COMMENT '构建容器内存配额 单位M',
     `DISK` varchar(64) COMMENT '构建容器磁盘配额 单位G',
     `DISPATCH_TYPE` varchar(32) NOT NULL COMMENT 'k8s集群类型',
     `CREATED_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
     `UPDATE_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
     PRIMARY KEY (`ID`) USING BTREE,
     UNIQUE KEY `uni_1` (`PIPELINE_ID`,`VM_SEQ_ID`,`POOL_NO`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'KUBERNETES构建集群构建调度编排表';

-- ----------------------------
-- Table structure for T_DISPATCH_KUBERNETES_BUILD_POOL
-- ----------------------------
CREATE TABLE IF NOT EXISTS `T_DISPATCH_KUBERNETES_BUILD_POOL` (
      `ID` bigint(20) NOT NULL AUTO_INCREMENT,
      `BUILD_ID` varchar(64) NOT NULL DEFAULT '' COMMENT '流水线构建ID',
      `VM_SEQ_ID` varchar(64) NOT NULL DEFAULT '' COMMENT '流水线CONTAINER_ID',
      `CONTAINER_NAME` varchar(128) DEFAULT NULL DEFAULT '' COMMENT '构建容器ID',
      `EXECUTE_COUNT` int(11) DEFAULT '1' COMMENT '流水线重试次数',
      `POOL_NO` int(11) DEFAULT 1 NULL COMMENT '构建池编号',
      `DISPATCH_TYPE` varchar(32) NOT NULL COMMENT 'DISPATCH类型',
      `CREATE_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
      PRIMARY KEY (`ID`) USING BTREE,
      UNIQUE KEY `uni_1` (`BUILD_ID`,`VM_SEQ_ID`, `EXECUTE_COUNT`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='KUBERNETES构建集群构建容器池';

-- ----------------------------
-- Table structure for T_DISPATCH_KUBERNETES_BUILD_HISTORY
-- ----------------------------
CREATE TABLE IF NOT EXISTS `T_DISPATCH_KUBERNETES_BUILD_HISTORY` (
     `ID` bigint(20) NOT NULL AUTO_INCREMENT,
     `PROJECT_ID` varchar(64) NOT NULL DEFAULT '' COMMENT '蓝盾项目ID',
     `PIPELINE_ID` varchar(34) NOT NULL DEFAULT '' COMMENT '流水线ID',
     `BUILD_ID` varchar(64) NOT NULL DEFAULT '' COMMENT '流水线构建ID',
     `VM_SEQ_ID` varchar(34) NOT NULL DEFAULT '' COMMENT '流水线CONTAINER_ID',
     `POOL_NO` int(11) NOT NULL DEFAULT 1 COMMENT '构件池ID',
     `CONTAINER_NAME` varchar(256) NOT NULL DEFAULT '' COMMENT '构建容器ID',
     `SECRET_KEY` varchar(64) DEFAULT NULL COMMENT '构建密钥',
     `CPU` float NOT NULL DEFAULT 1 COMMENT '构建容器CPU配额',
     `MEMORY` varchar(64) NOT NULL DEFAULT 1 COMMENT '构建容器内存配额 单位M',
     `DISK` varchar(64) COMMENT '构建容器磁盘配额 单位G',
     `EXECUTE_COUNT` int(11) DEFAULT '1' COMMENT '流水线重试次数',
     `DISPATCH_TYPE` varchar(32) NOT NULL COMMENT 'DISPATCH类型',
     `POD_NAME` varchar(128) NOT NULL DEFAULT '' COMMENT '负载类型对应的PodName',
     `CLUSTER_ID` varchar(128) NOT NULL DEFAULT '' COMMENT '负载类型对应的集群',
     `NAMESPACE` varchar(128) NOT NULL DEFAULT '' COMMENT '负载类型对应的Namespace',
     `REAL_CPU_PERCENTILE` float NOT NULL DEFAULT 1 COMMENT '真实的CPU占用',
     `REAL_MEM_PERCENTILE` float NOT NULL DEFAULT 1 COMMENT '真实的内存占用,单位G',
     `REAL_CPU_METRICS` text COMMENT '真实的CPU占用详细',
     `REAL_MEM_METRICS` text COMMENT '真实的内存占用详细',
     `CREATE_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
     `UPDATE_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
     PRIMARY KEY (`ID`),
     UNIQUE KEY `uni_1` (`BUILD_ID`,`VM_SEQ_ID`, `EXECUTE_COUNT`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'KUBERNETES构建集群构建历史表';

-- ----------------------------
-- Table structure for T_DISPATCH_KUBERNETES_PERFORMANCE_OPTIONS
-- ----------------------------
CREATE TABLE IF NOT EXISTS `T_DISPATCH_KUBERNETES_PERFORMANCE_CONFIG` (
      `ID` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
      `DISPATCH_TYPE` varchar(32) CHARACTER SET utf8 NOT NULL COMMENT 'DISPATCH类型',
      `PROJECT_ID` varchar(64) NOT NULL COMMENT '蓝盾项目ID',
      `OPTION_ID` bigint(20) NOT NULL DEFAULT '0' COMMENT '基础配置ID',
      `GMT_CREATE` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
      `GMT_MODIFIED` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
      PRIMARY KEY (`ID`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='KUBERNETES构建集群配额配置表';

-- ----------------------------
-- Table structure for T_DISPATCH_KUBERNETES_PERFORMANCE_OPTIONS
-- ----------------------------
CREATE TABLE IF NOT EXISTS `T_DISPATCH_KUBERNETES_PERFORMANCE_OPTION` (
      `ID` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
      `DISPATCH_TYPE` varchar(32) CHARACTER SET utf8 NOT NULL COMMENT 'DISPATCH类型',
      `CPU` float DEFAULT '1' COMMENT 'CPU',
      `MEMORY` varchar(64) COMMENT '内存 单位M',
      `DISK` varchar(64) COMMENT '磁盘 单位G',
      `DESCRIPTION` varchar(128) NOT NULL DEFAULT '' COMMENT '描述',
      `GMT_CREATE` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
      `GMT_MODIFIED` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
      PRIMARY KEY (`ID`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='KUBERNETES构建集群基础配额表';

-- ----------------------------
-- Table structure for T_DISPATCH_KUBERNETES_JOB_HISTORY
-- ----------------------------
CREATE TABLE IF NOT EXISTS `T_DISPATCH_KUBERNETES_JOB_HISTORY`
(
    `ID`                  bigint(20)   NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `PROJECT_ID`          varchar(64)  NOT NULL COMMENT '项目ID',
    `PIPELINE_ID`         varchar(34)  NOT NULL COMMENT '流水线ID',
    `BUILD_ID`            varchar(34)  NOT NULL COMMENT '构建ID',
    `VM_SEQ_ID`           varchar(34)  NOT NULL DEFAULT '' COMMENT '构建序列号',
    `TASK_ID`             varchar(34)  NOT NULL DEFAULT '' COMMENT '插件taskId',
    `EXECUTE_COUNT`       int(11)      NOT NULL DEFAULT 1 COMMENT '重试次数',
    `JOB_NAME`            VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'JOB名称',
    `JOB_TAG`             VARCHAR(128) NOT NULL DEFAULT '' COMMENT '识别JOB类型TAG',
    `CPU`                 float        NOT NULL DEFAULT 1 COMMENT 'cpu limit',
    `MEMORY`              float        NOT NULL DEFAULT 1 COMMENT 'memory limit',
    `DISK`                varchar(64)  NOT NULL DEFAULT '' COMMENT 'memory limit',
    `POD_NAME`            VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'POD名称',
    `CLUSTER_ID` varchar(128) NOT NULL DEFAULT '' COMMENT '负载类型对应的集群',
    `NAMESPACE` varchar(128) NOT NULL DEFAULT '' COMMENT '负载类型对应的Namespace',
    `REAL_CPU_PERCENTILE` float        NOT NULL DEFAULT 1 COMMENT '真实的CPU占用',
    `REAL_MEM_PERCENTILE` float        NOT NULL DEFAULT 1 COMMENT '真实的内存占用,单位G',
    `REAL_CPU_METRICS`    text COMMENT '真实的CPU占用详细',
    `REAL_MEM_METRICS`    text COMMENT '真实的内存占用详细',
    `CREATED_TIME`        datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `UPDATE_TIME`         datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`ID`),
    UNIQUE KEY `IDX_BUILD_TASK` (`BUILD_ID`, `VM_SEQ_ID`, `TASK_ID`, `EXECUTE_COUNT`, `JOB_TAG`),
    KEY `IDX_PIPELINE_TASK_TAG` (`PIPELINE_ID`, `VM_SEQ_ID`, `TASK_ID`, `EXECUTE_COUNT`, `JOB_TAG`)
    ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT 'JOB构建历史表';

SET FOREIGN_KEY_CHECKS = 1;
