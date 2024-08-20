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

func TaskCallback(taskId string, taskStatus types.TaskState, podName string) {
	url := fmt.Sprintf("http://%s/api/external/kube-manager/task/callback", config.Config.ApiServer.TaskCallbackUrl)
	taskInfo := TaskCallbackInfo{TaskId: taskId, PodName: podName, Status: taskStatus}
	taskInfoJsonStr, err := json.Marshal(taskInfo)
	if err != nil {
		// 处理错误
	}

	contentType := "application/json"
	resp, err := http.Post(url, contentType, bytes.NewBuffer(taskInfoJsonStr))
	if err != nil {
		logs.Error(fmt.Sprintf("%s Format taskCallbackInfo error. %s", taskId, err))
		return
	}
	defer resp.Body.Close()

	fmt.Println(resp.StatusCode)
	fmt.Println(resp.Header)
	body, err := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		logs.Error(fmt.Sprintf("%s TaskCallback error. %s", taskId, err))
	}
	logs.Info(fmt.Sprintf("%s TaskCallback success. %s", taskId, body))
}

type TaskCallbackInfo struct {
	TaskId  string          `json:"taskId"`
	PodName string          `json:"podName"`
	Status  types.TaskState `json:"status"`
}
