// 功能: 存储桶中对象的增删改查、上传、下载
package mys3

import (
	"context"
	"os"
	"study-aws-api-go/errorutil"
)

// 增 - 没sdk api接口
// 删
// 改
// 查

// 上传 - 低级api 需要file.Open()方式
// 参数:
// - ctx context.Contex
// - bucketName string        桶名称
// - objectKey string         对象key (文件key)
// - fileName string          文件名，看情况使用相对路径/绝对路径，看起来要常用绝对路径
// 返回值:
// - error
// func (basics BucketBasics) BucketQuery(ctx context.Context, s3Client *s3.Client) error { // 这种写法不够灵活
// 思路：
// 1. 创建存储桶f
// 2. 判断错误类型
// 3. 等待一段时间，看存储桶是否创建成功，并可用
func (basics BucketBasics) FileUploadLowApi(ctx context.Context, bucketName string, objectKey string, fileName string) error {
	_, err := os.Open(fileName)
	errorutil.ErrorPrintf(err, "打开文件: %s 失败", fileName)
	return err
}

// 下载
