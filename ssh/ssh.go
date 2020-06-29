package ssh

import (
	"net"

	"github.com/astaxie/beego"
	"golang.org/x/crypto/ssh"
)

// 定义ssh结构体
type Cli_ssh struct {
	User    string
	Auth    string
	Address string
	Client  *ssh.Client
	// session    *ssh.Session
	LastResult string
}

// ssh connection
func (this *Cli_ssh) Connec() {
	config := &ssh.ClientConfig{
		User: this.User,
		Auth: []ssh.AuthMethod{
			ssh.Password(this.Auth),
		},
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error { return nil },
	}
	conn, err := ssh.Dial("tcp", this.Address, config)
	if err != nil {
		beego.Error(err)
		return
	}
	this.Client = conn
}
