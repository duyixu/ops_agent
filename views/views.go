package views

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"ops_client/ssh"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/astaxie/beego"
	"go.etcd.io/etcd/clientv3"
)

//创建租约注册服务
type ServiceReg struct {
	client        *clientv3.Client
	lease         clientv3.Lease
	leaseResp     *clientv3.LeaseGrantResponse
	canclefunc    func()
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
	key           string
}

func NewServiceReg(addr []string, timeNum int64) (*ServiceReg, error) {
	conf := clientv3.Config{
		Endpoints:   addr,
		DialTimeout: 5 * time.Second,
	}

	var (
		client *clientv3.Client
	)

	if clientTem, err := clientv3.New(conf); err == nil {
		client = clientTem
	} else {
		return nil, err
	}

	ser := &ServiceReg{
		client: client,
	}

	if err := ser.setLease(timeNum); err != nil {
		return nil, err
	}
	go ser.ListenLeaseRespChan()
	return ser, nil
}

//设置租约
func (this *ServiceReg) setLease(timeNum int64) error {
	lease := clientv3.NewLease(this.client)

	//设置租约时间
	leaseResp, err := lease.Grant(context.TODO(), timeNum)
	if err != nil {
		return err
	}

	//设置续租
	ctx, cancelFunc := context.WithCancel(context.TODO())
	leaseRespChan, err := lease.KeepAlive(ctx, leaseResp.ID)

	if err != nil {
		return err
	}

	this.lease = lease
	this.leaseResp = leaseResp
	this.canclefunc = cancelFunc
	this.keepAliveChan = leaseRespChan
	return nil
}

//监听 续租情况
func (this *ServiceReg) ListenLeaseRespChan() {
	for {
		select {
		case leaseKeepResp := <-this.keepAliveChan:
			if leaseKeepResp == nil {
				beego.Error("agent续租失败\n")
				return
			} else {
				// beego.Informational("agent续租成功\n")
			}
		}
	}
}

//通过租约 注册服务
func (this *ServiceReg) PutService(key, val string) error {
	kv := clientv3.NewKV(this.client)
	_, err := kv.Put(context.TODO(), key, val, clientv3.WithLease(this.leaseResp.ID))
	return err
}

//撤销租约
func (this *ServiceReg) RevokeLease() error {
	this.canclefunc()
	time.Sleep(2 * time.Second)
	_, err := this.lease.Revoke(context.TODO(), this.leaseResp.ID)
	return err
}
func (this *ServiceReg) Init() error {
	// ser,_ := views.NewServiceReg([]string{"192.168.30.9:2379"},100)
	var err error
	return err

}

// MainExec
func (this *ServiceReg) MainExec(key string) {
	task_id, task := this.GetTasks(key, "dir2")
	if task_id != "" && task != "" {
		beego.Notice("task_id：" + task_id)
		// 标记任务
		if err := this.LabelTask(task_id, task, 100); err != nil {
			beego.Error(task_id + "任务标记失败")
		}
		//执行任务
		if status := this.TodoTask(task_id, task, 30); status {
			this.LabelTask(task_id, "WellDone", 30)
		} else {
			this.LabelTask(task_id, "Error", 30)
		}
	}
}

// 获取任务列表
func (this *ServiceReg) GetTasks(key, dir string) (string, string) {
	var key_new string
	var val_new string
	kv := clientv3.NewKV(this.client)
	resp, err := kv.Get(context.TODO(), key)
	if dir == "dir" {
		resp, err = kv.Get(context.TODO(), key, clientv3.WithPrefix())
	}
	if err != nil {
		fmt.Printf("get from etcd failed, err:%v\n", err)
	}
	for _, ev := range resp.Kvs {
		// fmt.Printf("%s:%s\n", ev.Key, ev.Value)
		if ev.Lease > 0 {
			continue
		} else {
			key_new = string(ev.Key)
			val_new = string(ev.Value)
			break
			// fmt.Printf("%s:%s\n",ev.Key,ev.Value)
		}
	}
	return key_new, val_new
}

// 标记任务
func (this *ServiceReg) LabelTask(key string, label string, ttl int64) error {
	kv := clientv3.NewKV(this.client)
	lease := clientv3.NewLease(this.client) // 定义ttl
	leaseResp, _ := lease.Grant(context.TODO(), ttl)
	_, err := kv.Put(context.TODO(), key, label, clientv3.WithLease(leaseResp.ID))
	if err != nil {
		fmt.Printf("Put from etcd failed, err:%v\n", err)
	}
	return err
}

// 返回日志
func (this *ServiceReg) LogReturn(key, value string) error {
	kv := clientv3.NewKV(this.client)
	_, err := kv.Put(context.TODO(), key, value)
	if err != nil {
		fmt.Printf("Pet from etcd failed, err:%v\n", err)
	}
	return err
}

// 监听同步任务
func (this *ServiceReg) RsyncTask(key string) {
	this.MainExec(key)
	// 使用watch监听key
	rch := this.client.Watch(context.Background(), key)
	var (
		task_key   string
		task_value string
	)
	for wresp := range rch {
		for _, ev := range wresp.Events {
			var str string
			str = fmt.Sprintf("%v", ev.Type)
			if ev.Kv.Lease == 0 && str == "PUT" {
				fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
				task_key = string(ev.Kv.Key)
				task_value = string(ev.Kv.Value)
				// 标记任务
				this.LabelTask(task_key, task_value, 100)
				// 执行任务
				if status := this.TodoTask(task_key, task_value, 30); status {
					this.LabelTask(task_key, "WellDone", 30)
				} else {
					this.LabelTask(task_key, "Error", 30)
				}
			}
		}
	}
}

// 执行日志返回
func (this *ServiceReg) TodoLog(reader *bufio.Reader, Log_key string) {
	count := 0
	LogPath := Log_key + strconv.Itoa(count)
	this.LogReturn(LogPath, "==============="+beego.AppConfig.String("AgentName")+"===============")
	for {
		count += 1
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		LogPath = Log_key + strconv.Itoa(count)
		// 返回日志
		this.LogReturn(LogPath, string(line))
		// _, v := this.GetTasks(LogPath, "dir2")
		beego.Informational(line)
	}
	return
}

// 执行任务
func (this *ServiceReg) TodoTask(key string, task string, ttl int64) bool {
	beego.SetLogger("file", `{"filename":"logs/test.log"}`)
	go this.LabelTask(key, task, ttl)
	// 处理错误
	defer func() {
		if err := recover(); err != nil {
			beego.Error(err)
		}
	}()
	// 输出获取的任务
	beego.Notice("task：" + task)
	var tmp map[string]interface{}
	json.Unmarshal([]byte(task), &tmp)
	// 取出任务脚本
	shell := tmp["Script"].(string)
	// 取出主机名
	Host := tmp["Hostname"].(string)
	// 取出任务类型
	Type := tmp["Ttype"].(string)
	// 根据任务类型替换相应的日志路径
	// 输出要执行的命令
	beego.Notice("Script：" + shell)
	var Log_key string
	if strings.Contains(key, "tasklist") {
		Log_key = strings.Replace(key, "tasklist", "tasklogs", 1)
	} else {
		Log_key = strings.Replace(key, "rsynctask", "tasklogs", 1)
	}

	switch Type {
	case "deploy":
		// 暂定
	case "test":
		// 暂定
	case "command":
		// 暂定
	}
	if Host == "localhost" || Host == "127.0.0.1" || Host == "" {
		// 定义执行任务
		cmd := exec.Command("/bin/sh", "-c", shell)
		// 定义标准输出和错误输出为一个通道
		stdout, err := cmd.StdoutPipe()
		cmd.Stderr = cmd.Stdout
		if err != nil {
			beego.Error("与任务建立输出通道失败" + shell)
		}
		// 函数结束后关闭输出连接
		defer stdout.Close()
		// 执行任务
		if err := cmd.Start(); err != nil {
			beego.Error(err)
		}
		reader := bufio.NewReader(stdout)
		this.TodoLog(reader, Log_key)
		// 判断任务执行是否有错误
		if err := cmd.Wait(); err != nil {
			beego.Error(key + "\t任务执行错误")
			return false
		}
	} else {
		// 初始化实例
		ser := ssh.Cli_ssh{
			User:    beego.AppConfig.String("RemoteUser"),
			Auth:    beego.AppConfig.String("RemotePass"),
			Address: beego.AppConfig.String("RemoteHost"),
		}
		// 建立ssh连接
		ser.Connec()
		// 创建会话
		session, err := ser.Client.NewSession()
		defer session.Close()
		if err != nil {
			beego.Error(err)
			return false
		}
		// 绑定标准输出管道
		stdout, _ := session.StdoutPipe()
		stderr, _ := session.StderrPipe()
		// 执行脚本
		if err := session.Start(shell); err != nil {
			beego.Error(err)
		}
		reader1 := bufio.NewReader(stdout)
		this.TodoLog(reader1, Log_key)
		reader2 := bufio.NewReader(stderr)
		this.TodoLog(reader2, Log_key)
		//判断执行过程中是否有错误
		if err := session.Wait(); err != nil {
			beego.Error(key + "\t任务执行错误")
			return false
		}
	}
	return true
}
