// 功能: 封装restfult api - order模块
package order

import (
	"strconv"
	"study-aws-api-go/db"
	"study-aws-api-go/errorutil"
	"study-aws-api-go/log"
	"study-aws-api-go/models"

	"github.com/gin-gonic/gin"
)

// 增
func OrderAdd(c *gin.Context) {
	log.Debug("增加订单")
	var order models.Order
	if err := c.ShouldBindJSON(&order); err != nil {
		log.Error("解析请求体失败, err: ", err)
		c.JSON(400, gin.H{"error": err.Error()})
		return // 必须保留 return，确保绑定失败时提前退出
	}
	err := db.OrderAdd(&order)
	if err != nil {
		log.Error("增加订单失败, err: ", err)
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, "添加成功")
}

// 删
func OrderDelete(c *gin.Context) {
	// 提取前端传递的 id 参数
	idStr := c.Param("id")
	log.Debug("删除订单, 参数= ", idStr)
	id, err := strconv.ParseUint(idStr, 10, 64) // 转换为 ​十进制 64 位无符号整数
	if err != nil {
		log.Error("删除订单, 参数错误")
		c.JSON(400, gin.H{"error": "删除订单, 参数错误"})
		return
	}

	// 调用数据库删除方法
	err = db.OrderDelete(uint(id))
	// err := db.OrderDelete(1)
	if err != nil {
		log.Error("删除订单失败, err: ", err)
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, "删除成功")
}

// 改
func OrderUpdate(c *gin.Context) {
	log.Debug("修改订单")
	// 绑定前端数据
	var order models.Order
	if err := c.ShouldBindJSON(&order); err != nil {
		log.Error("解析请求体失败, err: ", err)
		c.JSON(400, gin.H{"error": err.Error()})
		return // 必须保留 return，确保绑定失败时提前退出
	}
	err := db.OrderUpdate(order.PddOrderId, &order)

	if err != nil {
		log.Error("修改订单失败, err: ", err)
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, "修改成功")
}

// 查
/*
返回: json对象
{
	"total": 0,
	"data": []
}
*/
func OrdersQuery(c *gin.Context) {
	log.Debug("查询所有订单")
	total, err := db.OrdersTotal() // 补充总数获取
	errorutil.ErrorPrint(err, "查询订单总数失败")
	orders, _ := db.OrdersQueryAll()

	c.JSON(200, gin.H{
		"total": total,
		"data":  orders,
	})
}

// 查-分页
/*
返回: json对象
{
	"total": 0,
	"data": []
}

思路:
1. 获取前端传参,并做校验。没传page和size, 不处理, 返回
2. 参数缺失校验
3. 参数类型校验
4. 业务逻辑
*/
func OrdersPageQuery(c *gin.Context) {
	log.Debug("分页查询订单")

	// 强校验参数类型
	pageStr := c.DefaultQuery("page", "") // 之前写法默认为 1, pageStr := c.DefaultQuery("page", "1")
	sizeStr := c.DefaultQuery("size", "") // 之前写法默认为 10 ,所以不存在类型不是string类型, sizeStr := c.DefaultQuery("size", "10")
	log.Debugf("前端传参, page=%v, size=%v", pageStr, sizeStr)

	// 参数缺失校验
	if pageStr == "" || sizeStr == "" {
		c.JSON(400, gin.H{"error": "参数缺失"})
		return
	}

	// 参数类型校验
	if _, err := strconv.Atoi(pageStr); err != nil {
		c.JSON(400, gin.H{"error": "page参数类型错误"})
		return
	}
	if _, err := strconv.Atoi(sizeStr); err != nil {
		c.JSON(400, gin.H{"error": "size参数类型错误"})
		return
	}

	// 业务逻辑
	total, err := db.OrdersTotal() // 总数
	errorutil.ErrorPrint(err, "查询订单总数失败")

	page, _ := strconv.Atoi(pageStr) // 因为默认都是数字str了，所以不存在报错情况
	size, _ := strconv.Atoi(sizeStr) // 因为默认都是数字str了，所以不存在报错情况
	orders, _ := db.OrdersPageQuery(page, size)

	// 构造指定的返回结构
	c.JSON(200, gin.H{
		"total": total,
		"data":  orders,
	})
}
