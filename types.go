package main

import (
	"time"

	"gorm.io/gorm"

	"github.com/CyCoreSystems/ari/v6"
)

// DBの構造体なのです
type ReservedCall struct {
	gorm.Model
	CalleeID	int				`gorm:"not null"`
	RunAt 		time.Time	`gorm:"not null;index"`
}

type Application struct {
	cl	ari.Client
	db	*gorm.DB
}
