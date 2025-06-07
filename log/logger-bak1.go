// 自己封装go自带log库, 分 debug、info、warn、error级别日志
// package logger  // 原来叫logger,后来改成log了
package log

// ----------------------------------------- v0.0.0.4 start
/*
import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"runtime"
)

// 定义日志级别
type LogLevel int

// 常量, 具体日志级别
const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
)

// 变量
var (
	debugLogger *log.Logger
	infoLogger  *log.Logger
	warnLogger  *log.Logger
	errorLogger *log.Logger
	logLevel    LogLevel = LevelInfo // 默认日志级别为info
	logFile     *os.File             // 新增文件句柄
)

// 初始化, 设置日志，都打印到文件里
func init() {
	file, _ := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

	// 关闭旧文件（如果有）
	if logFile != nil {
		logFile.Close()
	}
	logFile = file

	// 创建组合Writer：同时输出到文件和控制台
	multiDebug := io.MultiWriter(os.Stdout, logFile)
	multiInfo := io.MultiWriter(os.Stdout, logFile)
	multiWarn := io.MultiWriter(os.Stdout, logFile)
	multiError := io.MultiWriter(os.Stdout, logFile)

	// 打印文件位置是logger.go 非源文件位置,原来的写法，弃用
	// debugLogger = log.New(multiDebug, "[DEBUG] ", log.Ldate|log.Ltime|log.Lshortfile)
	// infoLogger = log.New(multiInfo, "[INFO ] ", log.Ldate|log.Ltime|log.Lshortfile)
	// warnLogger = log.New(multiWarn, "[WARN ] ", log.Ldate|log.Ltime|log.Lshortfile)
	// errorLogger = log.New(multiError, "[ERROR] ", log.Ldate|log.Ltime|log.Lshortfile)

	// 打印文件位置是 源文件位置。如: [DEBUG] main.go:72: xx
	debugLogger = log.New(multiDebug, "[DEBUG] ", log.Ldate|log.Ltime)
	infoLogger = log.New(multiInfo, "[INFO ] ", log.Ldate|log.Ltime)
	warnLogger = log.New(multiWarn, "[WARN ] ", log.Ldate|log.Ltime)
	errorLogger = log.New(multiError, "[ERROR] ", log.Ldate|log.Ltime)
}

// SetLogLevel 设置日志级别
func SetLogLevel(level LogLevel) {
	logLevel = level
}

// 打印debug级别日志, 对应封装log.Printf
func Debug(format string, v ...interface{}) {
	if logLevel <= LevelDebug {
		// 获取调用者位置
		_, fullFile, line, _ := runtime.Caller(1)
		shortFile := path.Base(fullFile) // 截取锻路径
		debugLogger.Printf("%s:%d %s %s", shortFile, line, format, fmt.Sprint(v...))
	}
}

// 打印info级别日志, 对应封装log.Printf
func Info(format string, v ...interface{}) {
	if logLevel <= LevelDebug {
		// 获取调用者位置
		_, fullFile, line, _ := runtime.Caller(1)
		shortFile := path.Base(fullFile) // 截取锻路径
		infoLogger.Printf("%s:%d %s %s", shortFile, line, format, fmt.Sprint(v...))
	}
}

// 打印warn级别日志
func Warn(format string, v ...interface{}) {
	if logLevel <= LevelDebug {
		// 获取调用者位置
		_, fullFile, line, _ := runtime.Caller(1)
		shortFile := path.Base(fullFile) // 截取锻路径
		warnLogger.Printf("%s:%d %s %s", shortFile, line, format, fmt.Sprint(v...))
	}
}

// 打印error级别日志
func Error(format string, v ...interface{}) {
	if logLevel <= LevelDebug {
		// 获取调用者位置
		_, fullFile, line, _ := runtime.Caller(1)
		shortFile := path.Base(fullFile) // 截取锻路径
		errorLogger.Printf("%s:%d %s %s", shortFile, line, format, fmt.Sprint(v...))
	}
}
*/
// ----------------------------------------- v0.0.0.4 end

// ----------------------------------------- 只能通过Debugf() 和Debug()2个函数实现 start
/*
// 打印日志级别 v0.1

// 打印debug级别日志, 对应封装log.Printf
func Debugf(format string, v ...interface{}) {
	if logLevel <= LevelDebug {
		// debugLogger.Printf(format, v...) // 原来写法
		// 获取调用者位置
		_, file, line, _ := runtime.Caller(1)
		debugLogger.Printf("%s:%d %s", file, line, fmt.Sprint(v...))
	}
}

// 封装log.Println
func Debug(v ...interface{}) {
	if logLevel <= LevelError {
		// debugLogger.Println(v...) // 原来写法
		_, file, line, _ := runtime.Caller(1) // 跳过当前函数层级
		debugLogger.Printf("%s:%d %s", file, line, fmt.Sprint(v...))
	}
}

// 打印info级别日志, 对应封装log.Printf
func Infof(format string, v ...interface{}) {
	if logLevel <= LevelInfo {
		infoLogger.Printf(format, v...)
	}
}

// 封装log.Println
func Info(v ...interface{}) {
	if logLevel <= LevelError {
		infoLogger.Println(v...)
	}
}

// 打印warn级别日志, 对应封装log.Printf
func Warnf(format string, v ...interface{}) {
	if logLevel <= LevelWarn {
		warnLogger.Printf(format, v...)
	}
}

// 封装log.Println
func Warn(v ...interface{}) {
	if logLevel <= LevelError {
		warnLogger.Println(v...)
	}
}

// 打印error级别日志, 对应封装log.Printf
// logger.Error("创建失败:", result.Error) 会警告,只能带上%v才对. logger.Error("创建失败:", result.Error)
func Errorf(format string, v ...interface{}) {
	if logLevel <= LevelError {
		errorLogger.Printf(format, v...)
	}
}

// 封装log.Println
func Error(v ...interface{}) {
	if logLevel <= LevelError {
		errorLogger.Println(v...)
	}
}
*/
// ----------------------------------------- 只能通过Debugf() 和Debug()2个函数实现 end
