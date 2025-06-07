// db type 相关操作
package db

import (
	"study-aws-api-go/log"
	"study-aws-api-go/models"

	// 导入 clause 包
	"gorm.io/gorm/clause"
)

// 增
func TypeAdd(typeData *models.Type) error {
	result := DB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "NameId"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"name": typeData.Name, "level": typeData.Level, "parent": typeData.Parent}),
	}).Create(typeData)
	if result.Error != nil {
		log.Error("创建失败:", result.Error)
		return result.Error
	} else {
		log.Info("创建成功:", typeData)
	}
	return nil
}

// 批量增
func TypesBatchAdd(types []*models.Type) {
	for i, typeData := range types {
		err := TypeAdd(typeData)
		if err == nil {
			log.Debugf("批量创建第%d条成功, type: %v", i+1, typeData.Name) // 之前填的&typeData
		} else {
			log.Errorf("批量创建第%d条失败, err: %v", i+1, err)
		}
	}
}

// 删
func TypeDelete(id uint) {
	var typeData models.Type
	result := DB.Delete(&typeData, id)
	if result.Error != nil {
		log.Error("删除失败:", result.Error)
	} else {
		log.Info("删除成功:", id)
	}
}

// 批量删
func TypesBatchDelete(ids []uint) {
	var types []models.Type
	result := DB.Delete(&types, ids)
	if result.Error != nil {
		log.Error("批量删除失败:", result.Error)
	} else {
		log.Info("批量删除成功:", ids)
	}
}

// 改
func TypeUpdate(nameId uint, updates map[string]interface{}) {
	var typeData models.Type
	// 解决0值不更新
	result := DB.Model(&typeData).Where("name_id = ?", nameId).Select("name", "level", "parent").Updates(updates)
	if result.Error != nil {
		log.Error("修改失败:", result.Error)
	} else {
		log.Info("修改成功:", nameId)
	}
}

// 批量改
func TypesBatchUpdate(updates map[uint]map[string]interface{}) {
	for nameId, update := range updates {
		var typeData models.Type
		// 解决0值不更新
		result := DB.Model(&typeData).Where("name_id = ?", nameId).Select("name", "level", "parent").Updates(update)
		if result.Error != nil {
			log.Errorf("更新类型 %d 失败: %v", nameId, result.Error)
		} else {
			log.Debugf("更新类型 %d 成功", nameId)
		}
	}
}

// 查
func TypeQueryById(id uint) *models.Type {
	var typeData models.Type
	result := DB.First(&typeData, id)
	if result.Error != nil {
		log.Error("查询失败:", result.Error)
		return nil
	}
	log.Info("查询成功:", typeData)
	return &typeData
}

// 批量查
func TypesBatchQuery(ids []uint) ([]*models.Type, error) {
	var types []*models.Type
	result := DB.Find(&types, ids)
	if result.Error != nil {
		log.Error("批量查询失败: ", result.Error)
		return types, result.Error
	}
	log.Infof("批量查询成功, 查询到 %d 条记录", len(types))
	return types, nil
}
