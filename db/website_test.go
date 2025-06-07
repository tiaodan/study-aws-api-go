package db

import (
	"os"
	"study-aws-api-go/errorutil"
	"study-aws-api-go/log"
	"study-aws-api-go/models"
	"testing"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var testDB *gorm.DB

func TestMain(m *testing.M) {
	// 使用 MySQL 数据库进行测试
	var err error
	dsn := "root:password@tcp(127.0.0.1:3306)/audio?charset=utf8mb4&parseTime=True&loc=Local"
	testDB, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})

	errorutil.ErrorPanic(err, "Failed to connect to test database: ")

	// 自动迁移表结构
	testDB.AutoMigrate(&models.Website{})

	// 运行测试
	os.Exit(m.Run())
}

func TestCreateWebsite(t *testing.T) {
	website := &models.Website{
		NameId: 1,
		Name:   "Test Website",
		URL:    "http://test.com",
	}
	log.Debug("Creating website...", website)
	WebsiteAdd(website)

	var createdWebsite models.Website
	testDB.First(&createdWebsite, website.ID)
	if createdWebsite.ID != website.ID || createdWebsite.Name != website.Name || createdWebsite.URL != website.URL {
		t.Errorf("Expected website to be created with correct values, got %v", createdWebsite)
	}
}

func TestDeleteWebsite(t *testing.T) {
	website := &models.Website{
		NameId: 2,
		Name:   "Test Website for Delete",
		URL:    "http://delete.com",
	}
	WebsiteAdd(website)

	WebsiteDelete(website.ID)

	var deletedWebsite models.Website
	result := testDB.First(&deletedWebsite, website.ID)
	if result.Error == nil {
		t.Errorf("Expected website to be deleted, but found %v", deletedWebsite)
	}
}

func TestUpdateWebsite(t *testing.T) {
	website := &models.Website{
		NameId: 3,
		Name:   "Test Website for Update",
		URL:    "http://update.com",
	}
	WebsiteAdd(website)

	updates := map[string]interface{}{
		"Name": "Updated Website",
		"URL":  "http://updated.com",
	}
	WebsiteUpdate(website.ID, updates)

	var updatedWebsite models.Website
	testDB.First(&updatedWebsite, website.ID)
	if updatedWebsite.Name != "Updated Website" || updatedWebsite.URL != "http://updated.com" {
		t.Errorf("Expected website to be updated with correct values, got %v", updatedWebsite)
	}
}

func TestQueryWebsiteById(t *testing.T) {
	website := &models.Website{
		NameId: 4,
		Name:   "Test Website for Query",
		URL:    "http://query.com",
	}
	WebsiteAdd(website)

	retrievedWebsite := WebsiteQueryById(website.ID)
	if retrievedWebsite == nil || retrievedWebsite.Name != website.Name || retrievedWebsite.URL != website.URL {
		t.Errorf("Expected website to be queried with correct values, got %v", retrievedWebsite)
	}
}
