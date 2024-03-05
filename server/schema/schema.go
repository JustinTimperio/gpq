package schema

import (
	"reflect"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/cornelk/hashmap"
)

type Credentials struct {
	Password string `json:"password"`
	Username string `json:"username"`
}

type Token struct {
	Token    string    `json:"token"`
	Timeout  time.Time `json:"timeout"`
	IsAdmin  bool      `json:"is_admin"`
	Username string    `json:"username"`
}

type Settings struct {
	// Server
	Port     int    `koanf:"port"`
	HostName string `koanf:"host_name"`

	// Paths
	SettingsDBPath string `koanf:"settings_db_path"`
	ConfigPath     string `koanf:"config_path"`
	LogPath        string `koanf:"log_path"`

	// Auth
	AuthTopics     bool `koanf:"auth_topics"`
	AuthSettings   bool `koanf:"auth_settings"`
	AuthManagement bool `koanf:"auth_management"`

	// Admin
	AdminUser string `koanf:"admin_user"`
	AdminPass string `koanf:"admin_pass"`
}

type Topic struct {
	Name             string
	Buckets          int
	SyncToDisk       bool
	DiskPath         string
	LazyDiskSync     bool
	RePrioritize     bool
	RePrioritizeRate time.Duration
}

type User struct {
	ID            string
	Username      string
	Password      string
	Token         string
	IsAdmin       bool
	AllowedTopics hashmap.Map[string, []byte]
}

type AvroDataEntry struct {
	Schema string
	Data   interface{}
}

type ArrowDataEntry struct {
	Schema []ArrowSchema
	Meta   map[string]string
	Data   []byte
}

type ArrowSchema struct {
	Type arrow.DataType
	Name string
}

type ParquetDataEntry struct {
	Schema []ParquetSchema
	Data   interface{}
}

type ParquetSchema struct {
	Name string
	Type reflect.Type
	Tag  string
}
