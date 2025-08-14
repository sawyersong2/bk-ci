package kubeclient

import (
	"context"
	"disaptch-k8s-manager/pkg/config"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func CreateJob(job *Job) error {

	var containers []corev1.Container
	for _, con := range job.Pod.Containers {
		containers = append(containers, corev1.Container{
			Name:         job.Name,
			Image:        con.Image,
			Resources:    con.Resources,
			Env:          con.Env,
			Command:      con.Command,
			VolumeMounts: con.VolumeMounts,
			Args:         con.Args,
		})
	}

	var ImagePullSecrets []corev1.LocalObjectReference
	if job.Pod.PullImageSecret != nil {
		ImagePullSecrets = []corev1.LocalObjectReference{{Name: job.Pod.PullImageSecret.Name}}
	}

	_, err := kubeClient.BatchV1().Jobs(config.Config.Kubernetes.NameSpace).Create(
		context.TODO(),
		&batchv1.Job{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Job",
				APIVersion: "batch/v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      job.Name,
				Namespace: config.Config.Kubernetes.NameSpace,
			},
			Spec: batchv1.JobSpec{
				BackoffLimit: job.BackOffLimit,
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: job.Pod.Labels},
					Spec: corev1.PodSpec{
						Volumes:          job.Pod.Volumes,
						NodeName:         job.NodeName,
						RestartPolicy:    job.Pod.RestartPolicy,
						Containers:       containers,
						ImagePullSecrets: ImagePullSecrets,
					},
				},
				ActiveDeadlineSeconds:   job.ActiveDeadlineSeconds,
				TTLSecondsAfterFinished: job.TTLSecondsAfterFinished,
			},
		},
		metav1.CreateOptions{},
	)
	if err != nil {
		return err
	}

	return nil
}

func DeleteJob(jobName string) error {
	backGround := metav1.DeletePropagationBackground
	return kubeClient.BatchV1().Jobs(config.Config.Kubernetes.NameSpace).Delete(
		context.TODO(),
		jobName,
		metav1.DeleteOptions{
			PropagationPolicy: &backGround,
		},
	)
}

// ListCompletedJobs 列出所有已完成的Job
func ListCompletedJobs() (*batchv1.JobList, error) {
	return kubeClient.BatchV1().Jobs(config.Config.Kubernetes.NameSpace).List(
		context.TODO(),
		metav1.ListOptions{
			FieldSelector: "status.conditions.type=Complete,status.conditions.type=Failed",
		},
	)
}

// DeleteCompletedJobsOlderThan 删除完成时间超过指定天数的已完成Job
func DeleteCompletedJobsOlderThan(days int) (int, error) {
	jobs, err := ListCompletedJobs()
	if err != nil {
		return 0, err
	}

	deletedCount := 0
	cutoffTime := time.Now().AddDate(0, 0, -days)

	for _, job := range jobs.Items {
		// 检查Job是否已完成
		if isJobCompleted(&job) {
			// 检查完成时间是否超过指定天数
			if isJobCompletedBefore(&job, cutoffTime) {
				err := DeleteJob(job.Name)
				if err != nil {
					// 记录错误但继续处理其他Job
					continue
				}
				deletedCount++
			}
		}
	}

	return deletedCount, nil
}

// isJobCompleted 检查Job是否已完成（成功或失败）
func isJobCompleted(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return true
		}
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// isJobCompletedBefore 检查Job是否在指定时间之前完成
func isJobCompletedBefore(job *batchv1.Job, cutoffTime time.Time) bool {
	for _, condition := range job.Status.Conditions {
		if (condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed) &&
			condition.Status == corev1.ConditionTrue {
			if condition.LastTransitionTime.Time.Before(cutoffTime) {
				return true
			}
		}
	}
	return false
}
