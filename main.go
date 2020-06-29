package main

import (
	_ "ops_client/routers"

	"github.com/astaxie/beego"

	// "context"

	// "go.etcd.io/etcd/clientv3"
	"ops_client/views"
)

var (
	// 从beego conf中获取变量
	agent_name string = beego.AppConfig.String("AgentName")
	server_con string = beego.AppConfig.String("EtcdAddress")
	// 自定义
	agent_key string = "/go_ops/agent/" + agent_name
	agent_val string = agent_name
	task_key  string = "/go_ops/tasklist/agent/" + agent_name
	sync_key  string = "/go_ops/rsynctask/agent/" + agent_name + "/0/"
)

func main() {
	// 连接etcd
	ser, _ := views.NewServiceReg([]string{server_con}, 100)
	// 注册服务
	ser.PutService(agent_key, agent_val)
	defer func() {
		if err := recover(); err != nil {
			beego.Error(err)
		}
	}()
	go ser.RsyncTask(sync_key)
	// 存放任务的channel
	TaskList := make(chan map[string]string, 10)
	// 启动一个协程获取任务并存放至channel
	go func() {
		for {
			task_id, task := ser.GetTasks(task_key, "dir")
			if task_id != "" && task != "" {
				// 标记任务
				if err := ser.LabelTask(task_id, task, 100); err != nil {
					beego.Error(task_id + "任务标记失败~")
					continue
				}
				res := make(map[string]string)
				res[task_id] = task
				TaskList <- res
			}
		}
	}()
	// 启动四个协程以并行的方式到channel获取任务并处理任务
	for i := 0; i < 4; i++ {
		go func() {
			for {
				// 取出任务
				Get_TaskList, ok := <-TaskList
				if ok {
					var (
						task_id string
						task    string
					)
					// 遍历map取出任务的key和value
					for key := range Get_TaskList {
						task_id = key
						task = Get_TaskList[key]
					}
					//执行任务
					if status := ser.TodoTask(task_id, task, 30); status {
						ser.LabelTask(task_id, "WellDone", 30)
					} else {
						ser.LabelTask(task_id, "Error", 30)
					}
				}
			}
		}()
	}
	beego.Run()
}
