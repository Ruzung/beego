# beego
developer-v1.12.1-dm

## 注意点
* 在初始化项目orm的时候需要设置(如果不为0，则会报sql参数不正确)
```go
 orm.SetMaxIdleConns("default", 0) 
```
* 在使用自定义struct的时候，需要在结构体后，json或者orm定义字段与数据库字段一样，否则无法识别导致字段无法获取或者插入更新。
```go
type json struct {
    ResourceId     int         `json:"resource_id"`
    CpuRate        interface{} `json:"cpu_rate"`
    MemoryRate     interface{} `json:"memory_rate"`
    DiskRate       interface{} `json:"disk_rate"`
    Hosts          string      `json:"hosts"`
    Message        string      `json:"message"`
}
```
* 在使用在定义struct时候，必须在orm中进行注册
```go
func init() {
	orm.RegisterModel(new(Host))
}
```
* 在使用自定义sql，Row去执行的时候，一定要把dm关键字大写，并用双引号引起来。
* 在使用自定义sql，Row去执行的时候，一定不能带有 ` 

