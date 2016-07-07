package bigfinmodels

import (
	"github.com/skyrings/skyring-common/models"
)

var Notifications = []models.NotificationSubscription{
	{
		Name:        MONITOR_AVAILABILTY,
		Description: "Monitor Availability",
		Enabled:     true,
	},
	{
		Name:        OSD_AVAILABILITY,
		Description: "OSD Availability",
		Enabled:     true,
	},
}
