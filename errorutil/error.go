// 功能: 封装错误处理
package errorutil

import (
	"study-aws-api-go/log"
)

// 封装panic 的错误处理
/*
参数:
	err: 错误
	msg: 错误信息
返回值:
	errorCode int 错误码 1 - 正常，0 - 异常
*/
func ErrorPanic(err error, msg string) int {
	errorCode := 1
	// 异常, 有错误
	if err != nil {
		errorCode = 0
		panic(msg + ": " + err.Error())
	}
	return errorCode
}

// 封装纯打印错误
/*
参数:
	err: 错误
	msg: 错误信息
返回值:
	errorCode int 错误码 1 - 正常，0 - 异常  // 弃用
	error: 错误
*/
func ErrorPrint(err error, msg string) error {
	// errorCode := 1 // 弃用
	if err != nil {
		// errorCode = 0 // 弃用
		log.Error(msg + ": " + err.Error())
	}
	// return errorCode // 弃用
	return err
}

// 封装纯打印错误.可带参数的
/*
参数:
	err: 错误
	msg: 错误信息,可带参数如 %s ,%v
返回值:
	errorCode int 错误码 1 - 正常，0 - 异常 // 弃用
	error: 错误
*/
func ErrorPrintf(err error, msg string, args ...any) error {
	// errorCode := 1 // 弃用
	if err != nil {
		// errorCode = 0 // 弃用
		log.Error(msg, args, " reason : ", err.Error())
	}
	// return errorCode // 弃用
	return err
}
