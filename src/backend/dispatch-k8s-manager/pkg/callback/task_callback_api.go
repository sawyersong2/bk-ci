package callback

import (
	"bytes"
	"disaptch-k8s-manager/pkg/config"
	"disaptch-k8s-manager/pkg/logs"
	"disaptch-k8s-manager/pkg/types"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func TaskCallback(taskInfo *TaskCallbackInfo) {
	url := fmt.Sprintf("%s/api/external/kube-manager/task/callback", config.Config.ApiServer.TaskCallbackUrl)
	taskInfoJsonStr, err := json.Marshal(taskInfo)
	if err != nil {
		logs.Error(fmt.Sprintf("%s Format taskCallbackInfo error. %s", taskInfo.TaskId, err))
		return
	}

	logs.Info(fmt.Sprintf("%s TaskCallback taskInfo: %s", taskInfo.TaskId, taskInfoJsonStr))
	contentType := "application/json"
	resp, err := http.Post(url, contentType, bytes.NewBuffer(taskInfoJsonStr))
	if err != nil {
		logs.Error(fmt.Sprintf("%s TaskCallback error. %s", taskInfo.TaskId, err))
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		logs.Error(fmt.Sprintf("%s TaskCallback error. %s", taskInfo.TaskId, err))
		return
	}
	logs.Info(fmt.Sprintf("%s TaskCallback success. %s", taskInfo.TaskId, body))
}

type TaskCallbackInfo struct {
	TaskId    string           `json:"taskId"`
	PodName   string           `json:"podName"`
	Namespace string           `json:"namespace"`
	Status    types.TaskState  `json:"status"`
	Message   string           `json:"message"`
	Action    types.TaskAction `json:"action"`
}
