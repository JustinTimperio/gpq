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
	Port     int    `koanf:"gpq_server_port"`
	HostName string `koanf:"gpq_server_hostname"`

	// Paths
	SettingsDBPath string `koanf:"gpq_settings_path"`
	ConfigPath     string `koanf:"gpq_config_path"`
	LogPath        string `koanf:"gpq_log_path"`
	STDOut         bool   `koanf:"gpq_stdout"`

	// Topics
	DiskEncryptionEnabled bool   `koanf:"gpq_disk_encryption_enabled"`
	DiskEncryptionKey     string `koanf:"gpq_disk_encryption_key"`
	DiskCacheCompression  bool   `koanf:"gpq_disk_cache_compression"`

	// Auth
	AuthTopics     bool `koanf:"gpq_auth_topics"`
	AuthSettings   bool `koanf:"gpq_auth_settings"`
	AuthManagement bool `koanf:"gpq_auth_management"`

	// Admin
	AdminUser string `koanf:"gpq_admin_user"`
	AdminPass string `koanf:"gpq_admin_pass"`
}

type Topic struct {
	Name             string
	Buckets          int
	SyncToDisk       bool
	DiskPath         string
	LazyDiskSync     bool
	RePrioritize     bool
	RePrioritizeRate time.Duration
	BatchSize        int64
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
