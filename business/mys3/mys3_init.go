package mys3

import (
	"context"
	"study-aws-api-go/errorutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// 变量
type BucketBasics struct {
	S3Client *s3.Client
}

// 生成s3客户端,用New方式
// 参数
// - area string s3所在区域，如ap-northeast-1 ->  日本 东京
// - accesKeyId string s3访问密钥Id
// - secretKey string s3访问密钥
// - sessionToken string s3 session 的token (可选) 一般不填，一般默认""
// 思路：
// - 初始化aws s3配置
// - 初始化s3客户端
// 返回 1 context 上下文 2 s3client s3客户端
func InitS3Client(area, accessKeyId, secretKey, sessionToken string) (context.Context, *s3.Client) {
	// 2. 初始化aws s3配置
	ctx := context.Background()
	options := s3.Options{
		Region:      area,                                                                                                    // 区域, eg: ap-northeast-1
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(accessKeyId, secretKey, sessionToken)), // 凭证
	}

	clinet := s3.New(options) // 创建s3客户端
	return ctx, clinet
}

// 生成s3客户端,不用了,这样写有点麻烦
// 思路：
// 1. 初始化aws s3配置
// 2. 初始化s3客户端
func InitS3Client_NoUse() *s3.Client {
	// 2. 初始化aws s3配置
	ctx := context.Background()
	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		errorutil.ErrorPanic(err, "出错了,不能加载aws s3默认配置,你设置好aws账号了吗?")
	}
	s3Client := s3.NewFromConfig(sdkConfig) // 创建s3客户端
	return s3Client
}
