package bigfinmodels

import (
	"github.com/skyrings/skyring-common/monitoring"
	skyring_monitoring "github.com/skyrings/skyring-common/monitoring"
)

func GetProviderSpecificDefaultThresholdValues() (plugins []monitoring.Plugin) {
	return []skyring_monitoring.Plugin{
		{
			Name:   skyring_monitoring.SLU_UTILIZATION,
			Enable: true,
			Configs: []monitoring.PluginConfig{
				{Category: "threshold", Type: "critical", Value: "70"},
			},
		},
		{
			Name:   skyring_monitoring.STORAGE_UTILIZATION,
			Enable: true,
			Configs: []monitoring.PluginConfig{
				{Category: "threshold", Type: "critical", Value: "70"},
			},
		},
		{
			Name:   skyring_monitoring.CLUSTER_UTILIZATION,
			Enable: true,
			Configs: []monitoring.PluginConfig{
				{Category: "threshold", Type: "critical", Value: "70"},
			},
		},
	}
}
