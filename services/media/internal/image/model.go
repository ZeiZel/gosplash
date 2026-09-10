package models

import "gorm.io/gorm"

type Image struct {
	gorm.Model
	UserID     int64  `gorm:"not null;index:idx_images_user,priority:1"`
	StorageKey string `gorm:"not null`
	Width      string `gorm:"not null`
	Height     string `gorm:"not null`
	SizeBytes  string `gorm:"not null`
	Status     string `gorm:"not null;default:uploaded`
}

type Thumbnail struct {
	gorm.Model
}
