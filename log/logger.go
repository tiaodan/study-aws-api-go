package log

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

var (
	logInstance *logrus.Logger
	once        sync.Once
)

// InitLog 初始化logrus 日志, 单例
func InitLog() {
	once.Do(func() {
		logInstance = logrus.New()

		// 设置自定义日志格式（控制台输出）
		logInstance.SetFormatter(&CustomFormatter{}) // 带颜色输出
		// logInstance.SetFormatter(&CustomFileFormatter{}) // 不带颜色输出

		// 设置日志级别
		logInstance.SetLevel(logrus.DebugLevel)

		// 创建一个文件用于写入日志
		file, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			logInstance.Printf("Failed to open log file: %v", err)
		}
		defer file.Close() // 关闭日志文件

		// 使用 io.MultiWriter 实现多写入器功能
		multiWriter := io.MultiWriter(os.Stdout, file)
		logInstance.SetOutput(multiWriter)
	})
}

// GetLogger 返回日志实例
func GetLogger() *logrus.Logger {
	if logInstance == nil {
		InitLog()
	}
	return logInstance
}

// Debug 记录调试信息
func Debug(args ...interface{}) {
	GetLogger().Debug(args...)
}
func Debugf(format string, args ...interface{}) {
	GetLogger().Debugf(format, args...)
}

// Info 记录信息
func Info(args ...interface{}) {
	GetLogger().Info(args...)
}
func Infof(format string, args ...interface{}) {
	GetLogger().Infof(format, args...)
}

// Warn 记录警告信息
func Warn(args ...interface{}) {
	GetLogger().Warn(args...)
}
func Warnf(format string, args ...interface{}) {
	GetLogger().Warnf(format, args...)
}

// Error 记录错误信息
func Error(args ...interface{}) {
	GetLogger().Error(args...)
}
func Errorf(format string, args ...interface{}) {
	GetLogger().Errorf(format, args...)
}

// 定义自定义格式化器（控制台输出）
type CustomFormatter struct{}

// 实现 logrus.Formatter 接口
func (f *CustomFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	// 获取文件名和行号
	_, file, line, ok := runtime.Caller(8) // 调用者的堆栈深度，通常是日志调用的位置
	if !ok {
		file = "unknown"
		line = 0
	}

	// 提取短文件名
	shortFile := filepath.Base(file)

	// 定义日志级别颜色
	var color string
	switch entry.Level {
	case logrus.DebugLevel:
		color = "\033[34m" // 蓝色
	case logrus.InfoLevel:
		color = "\033[32m" // 绿色
	case logrus.WarnLevel:
		color = "\033[33m" // 黄色
	case logrus.ErrorLevel:
		color = "\033[31m" // 红色
	default:
		color = "\033[0m" // 默认颜色
	}

	// 格式化日志输出：[BUG等级] 日期 时间 go文件
	logFormat := fmt.Sprintf("%s[%-5s] %s %s %s:%d %s \033[0m \n",
		color, // 颜色前缀
		strings.ToUpper(shortenLevel(entry.Level.String())), // bug等级转为大写并左对齐
		time.Now().Format("2006-01-02"),                     // 日期
		time.Now().Format("15:04:05"),                       // 时间
		shortFile,                                           // 短文件名
		line,                                                // 行号
		entry.Message)                                       // 日志消息内容

	return []byte(logFormat), nil
}

// 定义自定义格式化器（文件输出）
type CustomFileFormatter struct{}

// 实现 logrus.Formatter 接口
func (f *CustomFileFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	// 获取文件名和行号
	_, file, line, ok := runtime.Caller(8) // 调用者的堆栈深度，通常是日志调用的位置, AI默认给的是7, 算的不对
	if !ok {
		file = "unknown"
		line = 0
	}

	// 提取短文件名
	shortFile := filepath.Base(file)

	// 格式化日志输出：[BUG等级] 日期 时间 go文件
	logFormat := fmt.Sprintf("[%-5s] %s %s %s:%d %s \n",
		strings.ToUpper(shortenLevel(entry.Level.String())), // bug等级转为大写并左对齐
		time.Now().Format("2006-01-02"),                     // 日期
		time.Now().Format("15:04:05"),                       // 时间
		shortFile,                                           // 短文件名
		line,                                                // 行号
		entry.Message)                                       // 日志消息内容

	return []byte(logFormat), nil
}

// 缩短日志级别名称
func shortenLevel(level string) string {
	switch level {
	case "warning":
		return "warn"
	default:
		return level
	}
}
