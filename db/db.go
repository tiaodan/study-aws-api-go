package db

import (
	"study-aws-api-go/log"
	"study-aws-api-go/models"
	"sync"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var DB *gorm.DB
var once sync.Once // 使用 sync.Once 确保单例

// 初始化数据库连接
/*
参数：
	dbType string 数据库类型 如 mysql、sqlite3、postgres 等
	dbName string 数据库名
	dbUser string 数据库用户名
	dbPass string 数据库密码
*/
func InitDB(dbType, dbName, dbUser, dbPass string) {
	once.Do(func() { // 使用 sync.Once 确保只执行一次
		// dsn := "root:password@tcp(127.0.0.1:3306)/pdd_order?charset=utf8mb4&parseTime=True&loc=Local"
		dsn := dbUser + ":" + dbPass + "@tcp(127.0.0.1:3306)/" + dbName + "?charset=utf8mb4&parseTime=True&loc=Local"
		var err error

		var dbOpen gorm.Dialector // 用什么数据库打开
		if dbType == "mysql" {
			dbOpen = mysql.Open(dsn)
		}
		// DB, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
		DB, err = gorm.Open(dbOpen, &gorm.Config{})
		if err != nil {
			log.Error("单例: 数据库连接失败, 是不是数据库名+密码没配对？ 数据库没创建？ err= ", err)
			panic(err)
		}
		log.Debug("单例: 数据库连接成功")
	})
}

// 插入默认数据
/*
思路:
1. 插入默认数据-1
2. 插入默认数据-2
*/
func InsertDefaultData() {
	// 插入默认数据-website
	// 插入默认数据-website
	websiteDefaultNoClass := &models.Website{Name: "待分类", NameId: 0, URL: "未知"}
	websiteDefaultJ88d := &models.Website{Name: "j88d", NameId: 1, URL: "www.j88d.com"} // 请求url 时带上http://
	defaultWebsites := []*models.Website{websiteDefaultNoClass, websiteDefaultJ88d}
	WebsiteBatchAdd(defaultWebsites)

	// 插入默认数据-category 类别
	classDefaultNoCategory := &models.Category{Name: "待分类", NameId: 0}
	classDefaultCartoonNormal := &models.Category{Name: "普通漫画", NameId: 1}
	classDefaultCartoonSex := &models.Category{Name: "色漫", NameId: 2}
	classes := []*models.Category{classDefaultNoCategory, classDefaultCartoonNormal, classDefaultCartoonSex}
	CategoriesBatchAdd(classes)

	// 插入默认数据-country
	countryDefaultNoType := &models.Country{Name: "待分类", NameId: 0}
	countryDefaultChina := &models.Country{Name: "中国", NameId: 1}
	countryDefaultKoren := &models.Country{Name: "韩国", NameId: 2}
	countryDefaultAmerica := &models.Country{Name: "欧美", NameId: 3}
	countryDefaultJapan := &models.Country{Name: "日本", NameId: 4}
	countries := []*models.Country{countryDefaultNoType, countryDefaultChina, countryDefaultKoren, countryDefaultAmerica, countryDefaultJapan}
	CountriesBatchAdd(countries)

	// 插入默认数据-type
	// 一级分类
	typeDefaultNoTypeLevel1 := &models.Type{NameId: 0, Name: "待分类", Level: 1}
	typeDefaultKoren := &models.Type{NameId: 1, Name: "韩漫", Level: 1}
	typeDefaultJapan := &models.Type{NameId: 2, Name: "日漫", Level: 1}
	typeDefaultRealPerson := &models.Type{NameId: 3, Name: "真人漫画", Level: 1}
	typeDefault3D := &models.Type{NameId: 4, Name: "3D漫画", Level: 1}
	typeDefaultAmeraica := &models.Type{NameId: 5, Name: "欧美漫画", Level: 1}
	typeDefaultSameSex := &models.Type{NameId: 6, Name: "同性", Level: 1}

	defaultTypes := []*models.Type{
		// 一级分类
		typeDefaultNoTypeLevel1, typeDefaultKoren, typeDefaultJapan,
		typeDefaultRealPerson, typeDefault3D, typeDefaultAmeraica,
		typeDefaultSameSex,
	}
	TypesBatchAdd(defaultTypes)

}
