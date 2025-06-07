// 拼多多订单数据模型, 存数据用的
package models

// 订单数据
type Order struct {
	// 拼多多相关信息
	Id                uint    `json:"id" gorm:"primaryKey;autoIncrement;column:id"`                                                 // 数据库id,主键、自增
	PddOrderId        string  `json:"pddOrderId" gorm:"not null; uniqueIndex:pdd_drop_shipping_unique;size:50;column:pdd_order_id"` // PDD订单号 数据库唯一索引
	PddOrderTime      string  `json:"pddOrderTime" gorm:"not null;column:pdd_order_time"`                                           // 购买时间
	PddOrderPrice     float64 `json:"pddOrderPrice" gorm:"not null;column:pdd_order_price"`                                         // 购买价格
	PddProductType    string  `json:"pddProductType" gorm:"not null;column:pdd_product_type"`                                       // 产品类型
	PddProductColor   string  `json:"pddProductColor" gorm:"not null;column:pdd_product_color"`                                     // 颜色
	PddOrderStatus    string  `json:"pddOrderStatus" gorm:"column:pdd_order_status"`                                                // 订单状态
	PddBuyerInfo      string  `json:"pddBuyerInfo" gorm:"not null;column:pdd_buyer_info"`                                           // 买家信息
	PddExpressCompany string  `json:"pddExpressCompany" gorm:"column:pdd_express_company"`                                          // 快递公司
	PddExpressId      string  `json:"pddExpressId" gorm:"column:pdd_express_id"`                                                    // 物流编号
	PddIsBlackList    bool    `json:"pddIsBlackList" gorm:"column:pdd_is_black_list"`                                               // 买家拉黑
	PddRemark         string  `json:"pddRemark" gorm:"column:pdd_remark"`                                                           // pdd备注

	// 代发平台相关信息
	DropShippingPlatform      string  `json:"dropShippingPlatform" gorm:"column:drop_shipping_platform"`                                                       // 代发平台
	DropShippingOrderId       string  `json:"dropShippingOrderId" gorm:"not null; uniqueIndex:pdd_drop_shipping_unique;size:50;column:drop_shipping_order_id"` // 代发订单号
	DropShippingOrderTime     string  `json:"dropShippingOrderTime" gorm:"column:drop_shipping_order_time"`                                                    // 代发订单时间
	DropShippingFactoryName   string  `json:"dropShippingFactoryName" gorm:"column:drop_shipping_factory_name"`                                                // 代发厂家名
	DropShippingRealPrice     float64 `json:"dropShippingRealPrice" gorm:"column:drop_shipping_real_price"`                                                    // 代发实际价
	DropShippingPrice         float64 `json:"dropShippingPrice" gorm:"column:drop_shipping_price"`                                                             // 购买价格
	DropShippingDiscountPrice float64 `json:"dropShippingDiscountPrice" gorm:"column:drop_shipping_discount_price"`                                            // 优惠
	DropShippingRemark        string  `json:"dropShippingRemark" gorm:"column:drop_shipping_remark"`                                                           // 代发备注
}
