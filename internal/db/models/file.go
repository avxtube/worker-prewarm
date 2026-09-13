package models

import (
	"time"

	"github.com/zergolf1994/goose"
)

// FileMetadata holds embedded metadata for a File.
type FileMetadata struct {
	Duration       *float64   `bson:"duration,omitempty" json:"duration,omitempty"`
	HighestQuality *int       `bson:"highestQuality,omitempty" json:"highestQuality,omitempty"`
	Playlists      *string    `bson:"playlists,omitempty" json:"playlists,omitempty"`
	Source         *string    `bson:"source,omitempty" json:"source,omitempty"`
	TrashedAt      *time.Time `bson:"trashedAt,omitempty" json:"trashedAt,omitempty"`
	DeletedAt      *time.Time `bson:"deletedAt,omitempty" json:"deletedAt,omitempty"`
}

// File represents a file/folder/space record.
// Collection: "files" | _id: String (UUID)
//
// Mongoose equivalent:
//
//	_id:       { type: String, required: true, default: uuidv4 }
//	slug:      { type: String, unique: true, default: () => randomString(11) }
//	status:    { type: String, enum: FileStatus, default: "waiting" }
//	type:      { type: String, enum: FileType, default: "video" }
//	parentId:  { type: String, ref: "File", index: true }
//	spaceId:   { type: String, ref: "File", index: true }
//	timestamps: true
type File struct {
	ID        string        `bson:"_id" json:"id" goose:"required,default:uuid"`
	Status    string        `bson:"status" json:"status" goose:"default:waiting"`
	Type      string        `bson:"type" json:"type" goose:"default:video"`
	Name      string        `bson:"name" json:"name" goose:"required"`
	OwnerType *string       `bson:"ownerType,omitempty" json:"ownerType,omitempty"`
	OwnerID   *string       `bson:"ownerId,omitempty" json:"ownerId,omitempty"`
	Kind      string        `bson:"kind" json:"kind" goose:"default:stream"`
	Slug      string        `bson:"slug" json:"slug" goose:"unique,default:random(11),index"`
	Metadata  *FileMetadata `bson:"metadata,omitempty" json:"metadata,omitempty"`
	CreatedAt time.Time     `bson:"createdAt" json:"createdAt" goose:"default:now"`
	UpdatedAt time.Time     `bson:"updatedAt" json:"updatedAt" goose:"default:now"`
}

// FileModel is the goose model for the "files" collection.
var FileModel = goose.NewModel[File]("files")
