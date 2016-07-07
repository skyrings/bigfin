package utils

import (
	"github.com/skyrings/skyring-common/monitoring"
)

func GetProviderSpecificDefaultThresholdValues() (plugins []monitoring.Plugin) {
	return []monitoring.Plugin{
		{
			Name:        monitoring.SLU_UTILIZATION,
			Description: "OSD Utilization",
			Enable:      true,
			Configs: []monitoring.PluginConfig{
				{Category: monitoring.THRESHOLD, Type: monitoring.CRITICAL, Value: "95"},
				{Category: monitoring.THRESHOLD, Type: monitoring.WARNING, Value: "85"},
			},
		},
		{
			Name:        monitoring.STORAGE_UTILIZATION,
			Description: "Storage Utilization",
			Enable:      true,
			Configs: []monitoring.PluginConfig{
				{Category: monitoring.THRESHOLD, Type: monitoring.CRITICAL, Value: "90"},
				{Category: monitoring.THRESHOLD, Type: monitoring.WARNING, Value: "75"},
			},
		},
		{
			Name:        monitoring.CLUSTER_UTILIZATION,
			Description: "Cluster",
			Enable:      true,
			Configs: []monitoring.PluginConfig{
				{Category: monitoring.THRESHOLD, Type: monitoring.CRITICAL, Value: "90"},
				{Category: monitoring.THRESHOLD, Type: monitoring.WARNING, Value: "75"},
			},
		},
		{
			Name:        monitoring.STORAGE_PROFILE_UTILIZATION,
			Description: "Storage Profile",
			Enable:      true,
			Configs: []monitoring.PluginConfig{
				{Category: monitoring.THRESHOLD, Type: monitoring.CRITICAL, Value: "85"},
				{Category: monitoring.THRESHOLD, Type: monitoring.WARNING, Value: "65"},
			},
		},
		{
			Name:        monitoring.BLOCK_DEVICE_UTILIZATION,
			Description: "Block Device Utilization",
			Enable:      true,
			Configs: []monitoring.PluginConfig{
				{Category: monitoring.THRESHOLD, Type: monitoring.CRITICAL, Value: "85"},
				{Category: monitoring.THRESHOLD, Type: monitoring.WARNING, Value: "65"},
			},
		},
	}
}
