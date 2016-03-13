package utils

import (
	"github.com/skyrings/skyring-common/monitoring"
	skyring_monitoring "github.com/skyrings/skyring-common/monitoring"
)

func GetProviderSpecificDefaultThresholdValues() (plugins []monitoring.Plugin) {
	return []skyring_monitoring.Plugin {
		{
			Name:   skyring_monitoring.SLU_UTILIZATION,
			Enable: true,
			Configs: []monitoring.PluginConfig{
				{Category: monitoring.THRESHOLD, Type: skyring_monitoring.CRITICAL, Value: "95"},
				{Category: monitoring.THRESHOLD, Type: skyring_monitoring.WARNING, Value: "85"},
			},
		},
		{
			Name:   skyring_monitoring.STORAGE_UTILIZATION,
			Enable: true,
			Configs: []monitoring.PluginConfig{
				{Category: monitoring.THRESHOLD, Type: skyring_monitoring.CRITICAL, Value: "90"},
				{Category: monitoring.THRESHOLD, Type: skyring_monitoring.WARNING, Value: "75"},
			},
		},
		{
			Name:   skyring_monitoring.CLUSTER_UTILIZATION,
			Enable: true,
			Configs: []monitoring.PluginConfig{
				{Category: monitoring.THRESHOLD, Type: skyring_monitoring.CRITICAL, Value: "90"},
				{Category: monitoring.THRESHOLD, Type: skyring_monitoring.WARNING, Value: "75"},
			},
		},
		{
			Name:   skyring_monitoring.STORAGE_PROFILE_UTILIZATION,
			Enable: true,
			Configs: []monitoring.PluginConfig{
				{Category: monitoring.THRESHOLD, Type: skyring_monitoring.CRITICAL, Value: "85"},
				{Category: monitoring.THRESHOLD, Type: skyring_monitoring.WARNING, Value: "65"},
			},
		},
	}
}
