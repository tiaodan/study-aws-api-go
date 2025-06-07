package models

// 定义书籍类型，玄幻、悬疑、科幻等
type Type struct {
	ID     uint   `gorm:"primaryKey;autoIncrement"`
	NameId int    `gorm:"not null;unique"`
	Name   string `gorm:"not null"`
	Level  int    `gorm:"not null"` // 1:一级分类，2:二级分类，3:三级分类
	Parent int    `gorm:"not null"` // 父级分类id
}
