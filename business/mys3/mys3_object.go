// 功能: 存储桶中对象的增删改查、上传、下载
package mys3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"study-aws-api-go/errorutil"
	"study-aws-api-go/log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// 增 - 没sdk api接口

// 删
// 参数:
// - ctx context.Contex
// - bucketName string        桶名称
// - awsFileName  string      对象key (文件key).使用时 xx【objectKey】
// - versionId string         对象版本id, 可以是""
// - bypassGovernance  bool    s3 的管理策略开关 , false -> 就是关
// 返回值:
// - bool // 是否删除成功
// - error
// 思路：
// 1. 准备
// 2. 删除文件
// 3. 判断错误
// 4. 等待文件确实成功,默认1分钟
func (basics BucketBasics) ObjectDelete(ctx context.Context, bucketName string, awsFileName string, versionId string, bypassGovernance bool) (bool, error) {
	// 1. 准备
	deleted := false // 是否删除成功
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(awsFileName),
	}
	if versionId != "" {
		input.VersionId = aws.String(versionId) // 如果有版本id, 使用指定版本
	}
	if bypassGovernance {
		input.BypassGovernanceRetention = aws.Bool(true) // 是否绕过 s3 的管理策略
	}

	// 2. 删除文件
	_, err := basics.S3Client.DeleteObject(ctx, input)

	// 3. 判断错误
	if err != nil {
		var noKey *types.NoSuchKey         // 没有对象错误
		var apiErr *smithy.GenericAPIError // api 错误
		if errors.As(err, &noKey) {
			log.Errorf("删除文件%s:%s 失败. reason: %v", bucketName, awsFileName, err)
			err = noKey
		} else if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "AccessDenied":
				log.Errorf("权限错误: 删除文件%s:%s 失败", bucketName, awsFileName)
			case "InvalidArgument":
				log.Errorf("参数错误: 删除文件%s:%s 失败", bucketName, awsFileName)
			}
		}
		return deleted, err
	}

	// 4. 等待文件确实成功,默认1分钟
	err = s3.NewObjectNotExistsWaiter(basics.S3Client).Wait(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(awsFileName),
	}, time.Minute)
	if err != nil {
		log.Errorf("等待失败。删除文件%s:%s 失败. reason: %v", bucketName, awsFileName, err)
		return deleted, err
	}

	// 5 删除成功
	deleted = true
	log.Infof("删除文件成功。文件%s:%s", bucketName, awsFileName)
	return deleted, err
}

// 删 - 批量
// 参数:
// - ctx context.Contex
// - bucketName string        桶名称
// - objs  []types.ObjectIdentifier      对象key数组 (文件key)
// - bypassGovernance  bool    s3 的管理策略开关 , false -> 就是关
// 返回值:
// - bool // 是否删除成功
// - error
// 思路：
// 1. 准备
// 2. 删除文件
// 3. 判断错误
// 4. 等待文件确实成功,默认1分钟
func (basics BucketBasics) ObjectDeleteBatch(ctx context.Context, bucketName string, objs []types.ObjectIdentifier, bypassGovernance bool) error {
	// 1. 准备
	// 判断数组是否空
	if len(objs) == 0 {
		return fmt.Errorf("批量删除错误。%s:%v ,err = objs 数组为空", bucketName, objs)
	}

	input := s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &types.Delete{
			Objects: objs,
			Quiet:   aws.Bool(true),
		},
	}

	//  s3 的管理策略 开关
	if bypassGovernance {
		input.BypassGovernanceRetention = aws.Bool(true)
	}

	// 2. 删除文件
	delOut, err := basics.S3Client.DeleteObjects(ctx, &input)

	// 3. 判断错误
	// objs 数组都错愕了，或者其中一个删除错了
	if err != nil {
		var noBucket *types.NoSuchBucket // 没有桶错误
		if errors.As(err, &noBucket) {
			log.Error("批量删除文件失败。err= 没有存储桶 ", bucketName)
			err = noBucket
		}
		log.Error("批量删除文件失败。err= ", err)
		return err
	}

	// objs 数组都其中一个删除错了
	if len(delOut.Errors) > 0 {
		for i, outErr := range delOut.Errors {
			log.Errorf("批量删除文件失败, 删除某一条 %s 失败。err= %s", *outErr.Key, *outErr.Message)
			err = fmt.Errorf("%s", *delOut.Errors[i].Message)
			return err
		}

	}

	// 4. 等待文件确实成功,默认1分钟
	for _, delObj := range delOut.Deleted {
		err = s3.NewObjectNotExistsWaiter(basics.S3Client).Wait(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(*delObj.Key),
		}, time.Minute)
		if err != nil {
			log.Errorf("等待失败。删除文件%s:%v 失败. reason: %v", bucketName, delObj, err)
			return err
		}
	}

	// 5 删除成功
	for _, obj := range objs {
		log.Infof("批量删除文件成功。文件%s:%+v", bucketName, obj.Key)
	}
	return err
}

// 改
// 查 某个bucket下所有
/*
参数:
	ctx context.Contex : 上下文
	bucketName string : 存储桶名称
返回值:
	[]types.Object 是否存在
	error: 错误
思路:
	1. 准备
	2. 查询
	3. 处理错误
	4. 返回
*/
func (basics BucketBasics) ObjectQueryAll(ctx context.Context, bucketName string) ([]types.Object, error) {
	// 1. 准备
	var err error
	var output *s3.ListObjectsV2Output
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}
	var objects []types.Object

	// 2. 查询
	objectPaginator := s3.NewListObjectsV2Paginator(basics.S3Client, input) // 分页器
	for objectPaginator.HasMorePages() {                                    // 循环
		output, err = objectPaginator.NextPage(ctx) // 查询
		// 3. 处理错误
		if err != nil {
			var noBucket *types.NoSuchBucket
			if errors.As(err, &noBucket) {
				log.Errorf("存储桶bucekt %s 不存在", bucketName)
				err = noBucket
			}
			log.Errorf("查询存储桶bucket %s 所有对象失败. reason: %v", bucketName, err)
			return objects, err
		}
		// 运行到这里，就算查询成功了
		objects = append(objects, output.Contents...)
	}

	// 4. 返回
	return objects, err
}

// 上传 - 低级api 需要file.Open()方式
// 参数:
// - ctx context.Contex
// - bucketName string        桶名称
// - awsFileName  string         对象key (文件key).使用时 xx【objectKey】
// - fileName string          文件名，看情况使用相对路径/绝对路径，看起来要常用绝对路径
// 返回值:
// - error
// func (basics BucketBasics) BucketQuery(ctx context.Context, s3Client *s3.Client) error { // 这种写法不够灵活
// 思路：
// 1. 打开文件
// 2. 上传文件
// 3. 判断错误
// 4. 等待文件确实上传成功,默认1分钟
func (basics BucketBasics) FileUploadLowApi(ctx context.Context, bucketName string, awsFileName string, fileName string) error {
	// 1. 打开文件
	file, err := os.Open(fileName)
	defer file.Close()
	errorutil.ErrorPrintf(err, "打开文件: %s 失败", fileName)

	// 2. 上传文件
	_, err = basics.S3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(awsFileName),
		Body:   file,
	})

	// 3. 判断错误
	if err != nil {
		var apiErr smithy.APIError
		// 如果是api错误, && 文件过大
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "EntityTooLarge" {
			log.Errorf("Error while uploading object to %s. 文件太大 (>5GB) .\n"+
				"To upload objects larger than 5GB, use the S3 console (160GB max)\n"+
				"or the multipart upload API (5TB max).", bucketName)
		}
		log.Errorf("上传文件%s 到 %s:%s 失败. reason: %v", fileName, bucketName, awsFileName, err)
	}

	// 4. 等待文件确实上传成功,默认1分钟
	err = s3.NewObjectExistsWaiter(basics.S3Client).Wait(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(awsFileName),
	}, time.Minute)
	if err != nil {
		log.Errorf("等待失败。上传文件%s 到 %s:%s 失败. reason: %v", fileName, bucketName, awsFileName, err)
		return err
	}

	// 上传成功
	log.Infof("上传文件%s 到 %s:%s 成功", fileName, bucketName, awsFileName)
	return err
}

// 上传 - 高级api 通过传输管理器
// 参数:
// - ctx context.Contex
// - bucketName string        桶名称
// - awsFileName  string         对象key (文件key).使用时 xx【objectKey】
// - uploadFileName string    要上传的文件名。可以是相对路径/绝对路径，一般是绝对路径
// - contents string  - delete        文件上下文. 也就算上传的文件路径。原本是io形式类型的contents，
// 返回值:
// - string  ?啥东西
// - error
// 思路：
// 1. 准备
// 2. 上传文件
// 3. 判断错误
// 4. 等待文件确实上传成功,默认1分钟
// func (basics BucketBasics) ObjectUpload(ctx context.Context, bucketName string, awsFileName string, contents string) (string, error) { // 官方推荐写法
func (basics BucketBasics) ObjectUpload(ctx context.Context, bucketName string, awsFileName string, uploadFileName string) (string, error) {
	// 1. 准备
	var outKey string // 返回值
	contents, err := os.ReadFile(uploadFileName)
	input := &s3.PutObjectInput{
		Bucket:            aws.String(bucketName),
		Key:               aws.String(awsFileName),
		Body:              bytes.NewReader([]byte(contents)),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256, // 校验算法
	}

	// 2. 上传文件
	output, err := basics.S3Manager.Upload(ctx, input)

	// 3. 判断错误
	if err != nil {
		var noBucket *types.NoSuchBucket // 无存储桶 错误
		if errors.As(err, &noBucket) {
			log.Errorf("存储桶 %s 不存在", bucketName)
			err = noBucket
		}
		return "", err
	}

	// 4. 等待文件确实上传成功,默认1分钟
	err = s3.NewObjectExistsWaiter(basics.S3Client).Wait(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(awsFileName),
	}, time.Minute)

	if err != nil {
		log.Errorf("等待失败。上传文件%s 到 %s:%s 失败. reason: %v", uploadFileName, bucketName, awsFileName, err)
		return "", err
	}

	// 上传成功
	outKey = *output.Key
	log.Infof("上传文件%s 到 %s:%s 成功", uploadFileName, bucketName, awsFileName)
	return outKey, nil
}

// 下载
/*
参数:
	ctx context.Contex : 上下文
	bucketName string : 存储桶名称
	awsFileName string : 对象key (文件key).使用时 xx【objectKey】
	downloadFileName string : 下载的文件名。可以是相对路径/绝对路径，一般是绝对路径
返回值:
	error: 错误
思路:
	1. 准备
	2. 处理错误
	3. 如果目录不存在，就创建
	4. 下载
	5. 默认成功
	6. 返回
*/
func (basics BucketBasics) ObjectDownload(ctx context.Context, bucketName string, awsFileName string, downloadFileName string) error {
	// 1. 准备
	// 读取aws文件
	result, err := basics.S3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(awsFileName),
	})

	// 2. 处理错误
	if err != nil {
		var noKey *types.NoSuchKey
		if errors.As(err, &noKey) {
			log.Errorf("文件下载失败-文件不存在。%s: %s 不存在", bucketName, awsFileName)
			err = noKey
		}
		log.Errorf("文件下载失败- %s: %s 无法下载, err = %v", bucketName, awsFileName, err)
		return err
	}

	// 3. 如果目录不存在，就创建
	// 获取文件的目录
	downloadDir := filepath.Dir(downloadFileName)
	// 如果目录不存在，则创建
	if _, err := os.Stat(downloadDir); os.IsNotExist(err) {
		if err := os.MkdirAll(downloadDir, 0755); err != nil {
			log.Errorf("创建目录失败 %s, err= %v", downloadDir, err)
			return err
		}
	}

	// 4. 下载
	defer result.Body.Close() // 关闭aws 文件
	downloadFile, err := os.Create(downloadFileName)
	if err != nil {
		log.Errorf("创建文件失败 %s, err= %v", downloadFileName, err)
		return err
	}
	defer downloadFile.Close()                    // 关闭下载文件
	awsFileStream, err := io.ReadAll(result.Body) // 读取aws文件内容
	if err != nil {
		log.Errorf("读取aws文件流失败 %s, err= %v", awsFileName, err)
		return err
	}
	_, err = downloadFile.Write(awsFileStream) // 写入下载文件
	if err != nil {
		log.Errorf("写入下载文件失败 %s, err= %v", downloadFileName, err)
		return err
	}

	// 5. 默认成功
	log.Infof("下载aws文件 [%s] 到 -> [%s] 成功", awsFileName, downloadFileName)
	return err
}
