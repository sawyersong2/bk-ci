package cron

import (
	"disaptch-k8s-manager/pkg/db/mysql"
	"disaptch-k8s-manager/pkg/kubeclient"
	"disaptch-k8s-manager/pkg/logs"

	"github.com/robfig/cron/v3"
)

func InitCronJob() error {
	if err := initClearExpiredTaskDBData(); err != nil {
		return err
	}

	if err := initClearCompletedKubernetesJobs(); err != nil {
		return err
	}

	return nil
}

const expiredDay = 5

func initClearExpiredTaskDBData() error {
	c := cron.New()

	if _, err := c.AddFunc("@every 2h10m", func() {
		logs.Info("start clear expired task db data")
		if err := mysql.DeleteTaskByUpdateTime(expiredDay); err != nil {
			logs.Error("clear expired task db data error ", err)
		}
	}); err != nil {
		return err
	}

	c.Start()

	return nil
}

func initClearCompletedKubernetesJobs() error {
	c := cron.New()

	if _, err := c.AddFunc("@every 5h", func() {
		logs.Info("start clear completed kubernetes jobs")
		if deletedCount, err := kubeclient.DeleteCompletedJobsOlderThan(1); err != nil {
			logs.Error("clear completed kubernetes jobs error ", err)
		} else {
			logs.Info("successfully deleted ", deletedCount, " completed kubernetes jobs")
		}
	}); err != nil {
		return err
	}

	c.Start()

	return nil
}
