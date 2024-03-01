package settings

import (
	"strings"

	"github.com/JustinTimperio/gpq/schema"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/structs"
	"github.com/knadh/koanf/v2"
)

// Settings is the default configuration struct
var Settings = &schema.Settings{
	// Server
	Port:     4040,
	HostName: "localhost",
	// Paths
	LogPath:        "gpq.log",
	SettingsDBPath: "/opt/gpq/gpq.db",

	AuthTopics:     false,
	AuthSettings:   true,
	AuthManagement: true,

	// Ding Ding
	AdminUser: "admin",
	AdminPass: "admin",
}

func LoadSettings() error {

	var k = koanf.New(".")

	configTypes := []interface{}{Settings}

	// Load all default configuration values from config structs
	for _, configType := range configTypes {
		err := k.Load(structs.Provider(configType, "koanf"), nil)

		if err != nil {
			return err
		}
	}

	// Load all environment variables into Koanf map overriding default values
	err := k.Load(env.Provider("", ".", func(s string) string {
		return strings.ToLower(s)
	}), nil)

	if err != nil {
		return err
	}

	// Marshal Koanf map into config structs
	for _, configType := range configTypes {
		if err := k.Unmarshal("", configType); err != nil {
			return err
		}
	}

	return nil

}
