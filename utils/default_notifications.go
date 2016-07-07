package utils

import (
	"github.com/skyrings/bigfin/bigfinmodels"
	"github.com/skyrings/skyring-common/models"
)

func GetDefaultNotifications() ([]models.NotificationSubscription) {
	return []models.NotificationSubscription{
		{
			Name:   bigfinmodels.MONITOR_AVAILABILTY,
			Description: "",
			Enabled: true,
		},
		{
			Name:   bigfinmodels.OSD_AVAILABILITY,
			Description: "OSD_Availability",
			Enabled: true,
		},
		{
			Name:   models.CLUSTER_AVAILABILITY,
			Description: "",
			Enabled: true,
		},
		{
			Name:   models.HOST_AVAILABILITY,
			Description: "",
			Enabled: true,
		},
		{
			Name:   models.QUORUM_LOSS,
			Description: "",
			Enabled: true,
		},
	}
}
