// db website 相关操作
package db

import (
	"study-aws-api-go/log"
	"study-aws-api-go/models"

	// 导入 clause 包
	"gorm.io/gorm/clause"
)

// 增
func WebsiteAdd(website *models.Website) error {
	result := DB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "NameId"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"name": website.Name, "url": website.URL}),
	}).Create(website)
	if result.Error != nil {
		log.Error("创建失败:", result.Error)
		return result.Error
	} else {
		log.Info("创建成功:", website)
	}
	return nil
}

// 批量增
func WebsiteBatchAdd(websites []*models.Website) {
	for i, website := range websites {
		err := WebsiteAdd(website)
		if err == nil {
			log.Debugf("批量创建第%d条成功, website: %v", i+1, website.Name)
		} else {
			log.Errorf("批量创建第%d条失败, err: %v", i+1, err)
		}
	}
}

// 删
func WebsiteDelete(id uint) {
	var website models.Website
	result := DB.Delete(&website, id)
	if result.Error != nil {
		log.Error("删除失败:", result.Error)
	} else {
		log.Info("删除成功:", id)
	}
}

// 批量删
func WebsitesBatchDelete(ids []uint) {
	var websites []models.Website
	result := DB.Delete(&websites, ids)
	if result.Error != nil {
		log.Error("批量删除失败:", result.Error)
	} else {
		log.Debug("批量删除成功:", ids)
	}
}

// 改
func WebsiteUpdate(nameId uint, updates map[string]interface{}) {
	var website models.Website
	// 解决0值不更新
	result := DB.Model(&website).Where("name_id = ?", nameId).Select("name", "url").Updates(updates)
	if result.Error != nil {
		log.Error("修改失败:", result.Error)
	} else {
		log.Info("修改成功:", nameId)
	}
}

// 批量改
func WebsitesBatchUpdate(updates map[uint]map[string]interface{}) {
	for nameId, update := range updates {
		var website models.Website
		// 解决0值不更新
		result := DB.Model(&website).Where("name_id = ?", nameId).Select("name", "url").Updates(update)
		if result.Error != nil {
			log.Errorf("更新网站 %d 失败: %v", nameId, result.Error)
		} else {
			log.Debugf("更新网站 %d 成功", nameId)
		}
	}
}

// 查
func WebsiteQueryById(id uint) *models.Website {
	var website models.Website
	result := DB.First(&website, id)
	if result.Error != nil {
		log.Error("查询失败:", result.Error)
		return nil
	}
	log.Info("查询成功:", website)
	return &website
}

// 批量查
func WebsitesBatchQuery(ids []uint) ([]*models.Website, error) {
	var websites []*models.Website
	result := DB.Find(&websites, ids)
	if result.Error != nil {
		log.Error("批量查询失败: ", result.Error)
		return websites, result.Error
	}
	log.Debugf("批量查询成功, 查询到 %d 条记录", len(websites)) // 原 log.Debug无需修改
	return websites, nil
}
