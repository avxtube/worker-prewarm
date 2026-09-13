package models

import (
	"time"

	"github.com/zergolf1994/goose"
)

// Storage contains only fields worker-prewarm needs from the current platform
// storage contract. Credentials are intentionally not read by this worker.
type Storage struct {
	ID        string     `bson:"_id" json:"id" goose:"required,default:uuid"`
	Name      string     `bson:"name" json:"name"`
	Provider  string     `bson:"provider" json:"provider"`
	Enabled   bool       `bson:"enabled" json:"enabled"`
	Purposes  []string   `bson:"purposes" json:"purposes"`
	Kinds     []string   `bson:"kinds" json:"kinds"`
	PublicURL *string    `bson:"publicUrl,omitempty" json:"publicUrl,omitempty"`
	OriginURL *string    `bson:"originUrl,omitempty" json:"originUrl,omitempty"`
	Status    string     `bson:"status" json:"status"`
	DeletedAt *time.Time `bson:"deletedAt,omitempty" json:"deletedAt,omitempty"`
	CreatedAt time.Time  `bson:"createdAt" json:"createdAt"`
	UpdatedAt time.Time  `bson:"updatedAt" json:"updatedAt"`
}

var StorageModel = goose.NewModel[Storage]("storages")
