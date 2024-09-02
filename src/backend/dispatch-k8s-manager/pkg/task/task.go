package task

import (
	"disaptch-k8s-manager/pkg/callback"
	"disaptch-k8s-manager/pkg/db/mysql"
	"disaptch-k8s-manager/pkg/logs"
	"disaptch-k8s-manager/pkg/types"
	v1 "k8s.io/api/core/v1"
)

func InitTask() {
	go WatchTaskPod()
	go WatchTaskDeployment()
}

func OkTask(taskId string, pod *v1.Pod, taskAction types.TaskAction) {
	err := mysql.UpdateTask(taskId, types.TaskSucceeded, "")
	if err != nil {
		logs.Errorf("save OkTask %s error %s", taskId, err.Error())
	}
	// 获取Pod的相关信息
	podName, namespace := getPodInfo(pod)
	callback.TaskCallback(&callback.TaskCallbackInfo{
		TaskId:    taskId,
		PodName:   podName,
		Namespace: namespace,
		Status:    types.TaskSucceeded,
		Message:   "",
		Action:    taskAction,
	})
}

func OkTaskWithMessage(taskId string, message string) {
	err := mysql.UpdateTask(taskId, types.TaskSucceeded, message)
	if err != nil {
		logs.Errorf("save OkTaskWithMessage %s %s error %s", taskId, message, err.Error())
	}

	callback.TaskCallback(&callback.TaskCallbackInfo{
		TaskId:    taskId,
		PodName:   "",
		Namespace: "",
		Status:    types.TaskSucceeded,
		Message:   message,
		Action:    types.TaskDockerActionInspect,
	})
}

func UpdateTask(taskId string, state types.TaskState) {
	err := mysql.UpdateTask(taskId, state, "")
	if err != nil {
		logs.Errorf("save UpdateTask %s %s error %s", taskId, state, err.Error())
	}
}

func FailTask(taskId string, message string, action types.TaskAction) {
	err := mysql.UpdateTask(taskId, types.TaskFailed, message)
	if err != nil {
		logs.Errorf("save FailTask %s %s error %s", taskId, message, err.Error())
	}
	callback.TaskCallback(&callback.TaskCallbackInfo{
		TaskId:    taskId,
		PodName:   "",
		Namespace: "",
		Status:    types.TaskFailed,
		Message:   message,
		Action:    action,
	})
}

// getPodInfo 获取Pod的相关信息
func getPodInfo(pod *v1.Pod) (podName, namespace string) {
	if pod != nil {
		return pod.Name, pod.Namespace
	}
	return "", ""
}
