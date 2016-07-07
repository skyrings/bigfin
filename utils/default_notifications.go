package utils

import (
	"github.com/skyrings/bigfin/bigfinmodels"
	"github.com/skyrings/skyring-common/models"
)

func GetDefaultNotifications() []models.NotificationSubscription {
	return []models.NotificationSubscription{
		{
			Name:        bigfinmodels.MONITOR_AVAILABILTY,
			Description: bigfinmodels.MONITOR_AVAILABILTY,
			Enabled:     true,
		},
		{
			Name:        bigfinmodels.OSD_AVAILABILITY,
			Description: "OSD Availability",
			Enabled:     true,
		},
		{
			Name:        models.CLUSTER_AVAILABILITY,
			Description: models.CLUSTER_AVAILABILITY,
			Enabled:     true,
		},
		{
			Name:        models.HOST_AVAILABILITY,
			Description: models.HOST_AVAILABILITY,
			Enabled:     true,
		},
		{
			Name:        models.QUORUM_LOSS,
			Description: models.QUORUM_LOSS,
			Enabled:     true,
		},
	}
}
