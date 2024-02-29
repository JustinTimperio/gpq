package schema

import (
	"time"

	"github.com/cornelk/hashmap"
)

type Item[d any] struct {
	// User
	Priority       int64
	Data           d
	DiskUUID       []byte
	ShouldEscalate bool
	EscalationRate time.Duration
	CanTimeout     bool
	Timeout        time.Duration

	// Internal
	SubmittedAt   time.Time
	LastEscalated time.Time
	Index         int
}

type Settings struct {
	// Server
	Port     int    `koanf:"port"`
	HostName string `koanf:"host_name"`

	// Paths
	SettingsDBPath string `koanf:"settings_db_path"`
	LogPath        string `koanf:"log_path"`

	// Admin
	AdminUser string `koanf:"admin_user"`
	AdminPass string `koanf:"admin_pass"`
}

type Topic struct {
	Name             string
	Buckets          int
	SyncToDisk       bool
	DiskPath         string
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
