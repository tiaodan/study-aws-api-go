# v0.0.0.0 
- 整合配置文件, from github项目: study-config-viper
- 整合日志文件, from github项目: study-log-go-original
- 整合错误处理, lian, from github项目: study-error-go-original
- 整合数据库  , from github项目: study-db-gorm
- 参考项目: study-restful-api-gin
- 做成一个gin服务,前后端分离

# 待办
- 插入默认数据country , category, website, √
- log打印，全换成 logger。config.go 不能换，因为先读取了配置, 日志级别才能生效 √
- gorm 项目，把addType 都改为 TypeAdd √
- 插入默认数据 √
- 插入默认数据types(types还不知道加哪些,先简单按国家分) √
- 删除order数据库，保留,别的项目参考 √
- 解决日志打印xx.go 文件名不对, 都是logger.go文件问题 √
- 处理gin日志和自己写的日志冲突 √ gin设置成release模式即可
- 日志分Debug,和Debugf,为了对应 log.Println 和log.Printf，后来整合成一个Debug √
- 如何通过lock的mod文件, 引入新项目, 防止版本冲突。用的时候拷贝go.mod+go.sum就行 √
- 更新study-log-go-original 代码 √
- 做一个整合好所有项目的项目, 保证拿来就能用 √

- gorm 漫画 项目，没配置外键关联
- http请求传输，考虑关键字防屏蔽。如改为 混乱的pinyin,或者混乱的英文
- 做一个待办列表项目
- 调研网站, 哪个适合爬,分类好

# v0.0.0.1
- 做好一个整合好所有项目的项目, 保证拿来就能用 

# v0.0.0.2
- db相关操作,变成info级别日志

# v0.0.0.3
- 解决logger.Debug("xxxxx") 不打印

# v0.0.0.4
- 自己封装总是有点问题, 改用logrus框架
- 但是无法控制 输出到控制台(带颜色), 输出到文件(不带颜色)
- 能用配置文件,控制打印带颜色,还是不带颜色的
- 打印带占位符%v 就用log.Debugf(); 不带占位符用log.Debug()
- 日志全用log. 开头打印
- 用了自定义logrus,就不建议用go 自带的log库了,因为可能会冲突,如果非要打印,用fmt.Println()
- 试下不用 log.Debugf 有没有问题？？

# v0.0.0.5
- 修复bug: 打印文件名不对,显示logger.go, untime.Caller(8) 就行了

# v0.0.0.6
- 把批量处理的log.Info -> log.Debug

# v0.0.0.7
- 修改app.log路径为配置文件

# v0.0.0.8
- fix-bug: 把db.catefory名称 -> db.category

# v0.0.0.9
- main.go 去除defer app.Close(),因为写不进去文件

# v0.0.0.10
- 修改README

# v0.0.0.11
- fix-bug: 解决db.Update()0值不更新问题

# v1.0.0.1
- aws s3能连了，写了查询存储桶的代码

# v1.0.0.2
- 本来 bucket的 增删改 是没有权限的, 开了 IAM Access Analyzer for S3 权限就可以了
- object操作，刚写到上传
- 修改错误打印，增加带参数打印。errorutil.errorPrintf()

---------------------------------------- 上传github替换 s3 keyId
---------------------------------------- 上传github替换 删除log中关于aws的key


# 待办
- 存在问题: 批量添加日志,打印文件名不对,显示logger.go 如:[DEBUG] 2025-04-29 22:39:43 logger.go:59 批量创建第1条成功, website: 待分类  √ untime.Caller(8) 就行 √
- 修改app.log路径为配置文件 √
- 整理配置文件logrus