// 功能：存储桶的增删改查
package mys3

import (
	"context"
	"errors"
	"study-aws-api-go/log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// 变量

// 增
// 参数:
// - ctx context.Contex
// - bucketName string        桶名称
// - region string           桶区域
// 返回值:
// - error
// func (basics BucketBasics) BucketQuery(ctx context.Context, s3Client *s3.Client) error { // 这种写法不够灵活
// 思路：
// 1. 创建存储桶
// 2. 判断错误类型
// 3. 等待一段时间，看存储桶是否创建成功，并可用
func (basics BucketBasics) BucketAdd(ctx context.Context, bucketName string, region string) error {
	// 1. 创建存储桶
	_, err := basics.S3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{ // 创建桶的区域
			LocationConstraint: types.BucketLocationConstraint(region), // 把string 转成指定类型
		},
	})

	// 2. 判断错误类型
	if err != nil {
		var owned *types.BucketAlreadyOwnedByYou // 已经被你创建了
		var existed *types.BucketAlreadyExists   // 可能被别人创建了
		if errors.As(err, &owned) {
			log.Errorf("存储桶 %s 已经存在,被你创建了。Bucket already exists by you.", bucketName)
		} else if errors.As(err, &existed) {
			log.Errorf("存储桶 %s 已经存在,被别人创建了。Bucket already exists by others.", bucketName)
		}
		return err
	}

	// 3. 等待一段时间-写死1分钟，看存储桶是否创建成功，并可用
	log.Info("等待存储桶可用。Wait bucket can use.")
	err = s3.NewBucketExistsWaiter(basics.S3Client).Wait(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucketName)}, time.Minute)
	if err != nil {
		log.Errorf("等待存储桶 %s 可用, 失败。Wait bucket can use failed.", bucketName)
		return err
	}

	// 说明创建成功
	log.Infof("创建存储桶成功。Bucket %s created successfully.", bucketName)
	return nil
}

// 删
// 参数:
// - ctx context.Contex
// - bucketName string        桶名称
// - region string           桶区域 - 用不着，默认就是用配置里的
// 返回值:
// - error
// 思路：
// 1. 删除错误
// 2. 判断错误
// 3. 等待bucket 真被删除 (s3 上没有这个bucket)
func (basics BucketBasics) BucketDelete(ctx context.Context, bucketName string) error {
	// 1. 删除错误
	_, err := basics.S3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})
	// 2. 判断错误
	if err != nil {
		var noBucket *types.NoSuchBucket
		if errors.As(err, &noBucket) {
			log.Errorf("要删除的存储桶 %s 不存在。Bucket does not exist.", bucketName)
			err = noBucket
		} else {
			log.Errorf("删除存储桶 %s 失败。reason: %v", bucketName, err)
		}
		return err
	}

	// 3. 等待bucket 真被删除 (s3 上没有这个bucket)
	err = s3.NewBucketNotExistsWaiter(basics.S3Client).Wait(
		ctx, &s3.HeadBucketInput{Bucket: aws.String(bucketName)}, time.Minute)
	if err != nil {
		log.Errorf("等待。。。 存储桶 %s 确实不存在, 失败。Wait bucket deleted failed.", bucketName)
	}

	log.Infof("删除存储桶 %s 成功", bucketName)
	return err
}

// 改 - 没有sdk方法

// 查 所有 - 有权限
// 参数:
// - ctx context.Contex
// 返回值:
// - []types.Bucket
// - error
func (basics BucketBasics) BucketQueryAll(ctx context.Context) ([]types.Bucket, error) {
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

// 检测是否存在
/*
参数:
	ctx context.Contex : 上下文
	bucketName string : 存储桶名称
返回值:
	bool 是否存在
	error: 错误
思路:
	1. 准备
	2. 判断
	3. 处理错误
	4. 返回
*/
func (basics BucketBasics) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	// 1. 准备
	exists := true // 默认存在

	// 2. 判断
	_, err := basics.S3Client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucketName)})

	// 3. 处理错误
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.(type) {
			case *types.NotFound:
				log.Errorf("存储桶 %s 不存在", bucketName)
			default:
				log.Errorf("存储桶 %s 不存在。发生其f他错误,可鞥你没有权限访问, err= %v", bucketName, err)
			}
		}
		exists = false
		return exists, err
	}

	// 4. 返回
	log.Infof("存储桶 %s 存在。", bucketName)
	return exists, err
}
