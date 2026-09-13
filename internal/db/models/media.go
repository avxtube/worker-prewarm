package models

import (
	"time"

	"github.com/zergolf1994/goose"
)

// PrewarmData holds cache prewarm statistics.
type PrewarmData struct {
	Total   int `bson:"total" json:"total"`
	Hit     int `bson:"hit" json:"hit"`
	Miss    int `bson:"miss" json:"miss"`
	Expired int `bson:"expired" json:"expired"`
	Failed  int `bson:"failed" json:"failed"`
}

// PrewarmEntry holds a single prewarm entry per CDN/region.
type PrewarmEntry struct {
	Data      *PrewarmData `bson:"data,omitempty" json:"data,omitempty"`
	PrewarmAt *time.Time   `bson:"prewarmAt,omitempty" json:"prewarmAt,omitempty"`
}

// Media represents a transcoded/processed media record.
// Collection: "medias" | _id: String (UUID)
type Media struct {
	ID        string                  `bson:"_id" json:"id" goose:"required,default:uuid"`
	Type      string                  `bson:"type" json:"type" goose:"default:video"`
	FileID    string                  `bson:"fileId" json:"fileId" goose:"required,ref:files,index"`
	StorageID string                  `bson:"storageId" json:"storageId" goose:"required,ref:storages,index"`
	Quality   *string                 `bson:"quality,omitempty" json:"quality,omitempty"`
	Key       string                  `bson:"key" json:"key" goose:"required"`
	Mime      string                  `bson:"mime" json:"mime" goose:"required"`
	Size      interface{}             `bson:"size,omitempty" json:"size,omitempty"`
	Width     *int                    `bson:"width,omitempty" json:"width,omitempty"`
	Height    *int                    `bson:"height,omitempty" json:"height,omitempty"`
	Slug      string                  `bson:"slug" json:"slug" goose:"unique,default:random(11)"`
	Prewarm   map[string]PrewarmEntry `bson:"prewarm,omitempty" json:"prewarm,omitempty"`
	DeletedAt *time.Time              `bson:"deletedAt,omitempty" json:"deletedAt,omitempty"`
	CreatedAt time.Time               `bson:"createdAt" json:"createdAt" goose:"default:now"`
	UpdatedAt time.Time               `bson:"updatedAt" json:"updatedAt" goose:"default:now"`
}

// MediaModel is the goose model for the "medias" collection.
var MediaModel = goose.NewModel[Media]("medias")
