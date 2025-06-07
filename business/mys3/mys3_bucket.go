// 功能：存储桶的增删改查
package mys3

import (
	"context"
	"errors"
	"study-aws-api-go/log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// 变量
type BucketBasics struct {
	S3Client *s3.Client
}

// 增
// 参数:
// - ctx context.Contex
// - bucketName string        桶名称
// - region string           桶区域
// 返回值:
// - []types.Bucket
// - error
// func (basics BucketBasics) BucketQuery(ctx context.Context, s3Client *s3.Client) error { // 这种写法不够灵活
func (basics BucketBasics) BucketAdd(ctx context.Context, bucketName string, region string) error {
	basics.S3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{ // 创建桶的区域
			LocationConstraint: types.BucketLocationConstraint(region),
		},
	})
	return nil
}

// 删
// 改

// 查
// 参数:
// - ctx context.Contex
// 返回值:
// - []types.Bucket
// - error
// func (basics BucketBasics) BucketQuery(ctx context.Context, s3Client *s3.Client) error { // 这种写法不够灵活
func (basics BucketBasics) BucketQuery(ctx context.Context) ([]types.Bucket, error) {
	result, err := basics.S3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) && ae.ErrorCode() == "AccessDenied" {
			log.Error("账户无权限访问。You don't have permission to list buckets for this account.")
		} else {
			log.Error("无法查看存储桶. 原因: ", err)
		}
		return result.Buckets, err // 提前返回
	}
	// 判断有没有存储桶
	if len(result.Buckets) == 0 {
		log.Info("没有存储桶。No buckets.")
		return result.Buckets, err
	}

	// 打印查询结果
	for _, bucket := range result.Buckets {
		log.Debugf("存储桶名称: %s, 创建时间: %s", *bucket.Name, *bucket.CreationDate)
	}
	return result.Buckets, err
}
