package main

import (
	"context"
	"io"
	"os"
	"study-aws-api-go/business/mys3"
	"study-aws-api-go/business/order"
	"study-aws-api-go/db"
	"study-aws-api-go/errorutil"
	"study-aws-api-go/log"
	"study-aws-api-go/models"
	"study-aws-api-go/myconfig"

	// 三方

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// 变量
// 分类：
// - config配置文件
// - aws相关
var (
	// config 配置文件相关
	cfg *myconfig.Config // 配置文件

	// aws 相关
	s3Basic   mys3.BucketBasics // 替代 s3Client
	s3Client  *s3.Client
	s3Manager *manager.Uploader
	ctx       context.Context
)

// 初始化, 默认main会自动调用本方法
// 思路：
// 1. 读取配置文件
// 2. 设置日志级别
// 3. 初始化数据库连接
// 4. 自动迁移表结构
// 5. db插入默认数据
// 6. 初始化aws s3配置,创建s3客户端
func init() {
	// 1. 读取配置文件， (如果配置文件不填, 自动会有默认值)
	cfg = myconfig.GetConfig(".", "config", "yaml")

	// 2. 根据配置文件,设置日志相关,现在用logrus框架
	log.InitLog()

	// 获取日志实例
	log := log.GetLogger()

	// 设置日志级别
	switch cfg.Log.Level {
	case "debug":
		log.SetLevel(logrus.DebugLevel)
	case "info":
		log.SetLevel(logrus.InfoLevel)
	case "warn":
		log.SetLevel(logrus.WarnLevel)
	case "error":
		log.SetLevel(logrus.ErrorLevel)
	default:
		log.SetLevel(logrus.InfoLevel)
	}

	// 创建一个文件用于写入日志
	file, err := os.OpenFile(cfg.Log.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666) // os.OpenFile("app.log"
	if err != nil {
		log.Printf("Failed to open log file: %v", err)
	}
	// defer file.Close() // 关闭日志文件 报错，写不进去

	// 使用 io.MultiWriter 实现多写入器功能
	multiWriter := io.MultiWriter(os.Stdout, file)
	log.SetOutput(multiWriter)

	// 打印配置
	log.Info("配置-------------")
	log.Info("[log] 相关")
	log.Info("log.level: ", cfg.Log.Level)
	log.Info("log.path: ", cfg.Log.Path)
	log.Info("[network] 相关---")
	log.Info("network.ximalayaIIp_ip: ", cfg.Network.XimalayaIIp)
	log.Info("[db] 相关")
	log.Info("db.name: ", cfg.DB.Name)
	log.Info("db.user: ", cfg.DB.User)
	log.Info("db.password: ", cfg.DB.Password)
	log.Info("[gin] 相关")
	log.Info("gin.mode: ", cfg.Gin.Mode)
	log.Info("region: ", cfg.AWS_S3.Region)
	log.Info("access_key_id: ", cfg.AWS_S3.AccessKeyId)
	log.Info("access_key_secret: ", cfg.AWS_S3.AccessKeySecret)

	// 初始化数据库连接
	db.InitDB("mysql", cfg.DB.Name, cfg.DB.User, cfg.DB.Password)

	// 自动迁移表结构
	db.DB.AutoMigrate(&models.Website{}, &models.Country{}, &models.Category{}, &models.Type{}, &models.Order{})

	// 插入默认数据
	db.InsertDefaultData()

	// 6. 初始化aws s3配置,创建s3客户端 ->  实际使用s3Basic
	// s3Client := mys3.InitS3Client("ap-northeast-1", "11keyId", "keySecret", "")
	ctx, s3Client = mys3.InitS3Client(cfg.AWS_S3.Region, cfg.AWS_S3.AccessKeyId, cfg.AWS_S3.AccessKeySecret, "")
	s3Manager = manager.NewUploader(s3Client) // init
	s3Basic = mys3.BucketBasics{
		S3Client:  s3Client,
		S3Manager: s3Manager,
	}
}

// main函数
// 思路：
// 1. 初始化myconfig配置文件
// 2. 初始化aws s3配置,创建s3客户端
// 3. s3 增删改查、上传、下载
// ？. 初始化gin框架
func main() {
	// 3. s3 增删改查、上传、下载
	// err := s3Basic.BucketDelete(ctx, "mytesttest12234") // 存储桶 - add
	// err := s3Basic.BucketAdd(ctx, "mytesttest12234", cfg.AWS_S3.Region) // 存储桶 - delete
	// _, err := s3Basic.BucketQueryAll(ctx) // 存储桶 - query
	// exists, err := s3Basic.BucketExists(ctx, "sexcomic") // 查询桶是否存在

	// object 操作
	// err := s3Basic.FileUploadLowApi(ctx, "sexcomic", "充满各种变态行为的家-1.jpg", "C://home/manhua/亲家四姊妹/充满各种变态行为的家/1.jpg")     // 上传文件
	// outKey, err := s3Basic.ObjectUpload(ctx, "sexcomic", "充满各种变态行为的家-2.jpg", "C://home/manhua/亲家四姊妹/充满各种变态行为的家/2.jpg") // 上传文件
	// _, err := s3Basic.ObjectDelete(ctx, "sexcomic", "充满各种变态行为的家-2.jpg", "", false) // 上传文件

	// 批量删 文件
	// objs := []types.ObjectIdentifier{
	// 	{Key: aws.String("充满各种变态行为的家-1.jpg")},
	// 	{Key: aws.String("充满各种变态行为的家-2.jpg")},
	// }
	// err := s3Basic.ObjectDeleteBatch(ctx, "sexcomic", objs, false) // 上传文件
	// results, err := s3Basic.ObjectQueryAll(ctx, "sexcomic") // 查所有
	// for _, result := range results {
	// 	log.Info("查询到 ", *result.Key)
	// }

	// 下载
	err := s3Basic.ObjectDownload(ctx, "sexcomic", "充满各种变态行为的家-1.jpg", "C://home/test/1.jpg") // 上传文件

	errorutil.ErrorPrint(err, " 报错 err= ")

	// ？. 初始化gin框架
	// 后面如果用不到，可以删除
	gin.SetMode(gin.ReleaseMode) // 关键代码：切换到 release 模式
	r := gin.Default()
	r.Use(cors.Default()) // 允许所有跨域

	// 封装api
	r.POST("/orders", order.OrderAdd)
	r.DELETE("/orders/:id", order.OrderDelete)
	r.PUT("/orders", order.OrderUpdate)
	r.GET("/orders", order.OrdersPageQuery) // 分页查询

	r.Run(":8888") // 启动服务

}
