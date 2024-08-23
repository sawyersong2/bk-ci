package com.tencent.devops.dispatch.kubernetes.pojo

data class TaskCallbackInfo(
    val taskId: String,
    val podName: String,
    val status: CallbackTaskStatus,
    val message: String,
    val action: CallbackTaskAction
)

enum class CallbackTaskStatus {
    waiting,
    running,
    succeeded,
    failed,
    abort
}

enum class CallbackTaskAction {
    create,
    stop,
    start,
    delete
}
