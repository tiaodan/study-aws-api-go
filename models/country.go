package models

// 定义国家模型
type Country struct {
	ID     uint   `gorm:"primaryKey;autoIncrement"`
	NameId int    `gorm:"not null;unique"`
	Name   string `gorm:"not null"`
}
