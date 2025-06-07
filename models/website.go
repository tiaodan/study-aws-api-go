package models

// 定义网站模型
type Website struct {
	ID        uint   `gorm:"primaryKey;autoIncrement"`
	NameId    int    `gorm:"not null;unique"`
	Name      string `gorm:"not null"`
	URL       string `gorm:"not null"`
	NeedProxy uint   `gorm:"not null"` // 是否需要翻墙
}
