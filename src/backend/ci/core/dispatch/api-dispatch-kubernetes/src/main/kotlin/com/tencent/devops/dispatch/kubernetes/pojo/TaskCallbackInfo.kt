package com.tencent.devops.dispatch.kubernetes.pojo

data class TaskCallbackInfo(
    val taskId: String,
    val podName: String,
    val namespace: String,
    val status: TaskCallbackStatus,
    val action: TaskCallbackAction,
    val message: String = ""
)

enum class TaskCallbackStatus {
    waiting,
    running,
    succeeded,
    failed,
    abort,
    timeout
}

enum class TaskCallbackAction {
    create,
    stop,
    start,
    delete,
    unkown
}
