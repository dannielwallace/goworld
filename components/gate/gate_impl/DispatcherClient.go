package gate_impl

import (
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/netutil"
	"github.com/dannielwallace/goworld/engine/proto"
	"os"
	"syscall"
)

type dispatcherClientDelegate struct {
	m_gateSrv   *GateService
	m_singnalCh chan os.Signal
}

func NewDispatcherClientDelegate(gateSrv *GateService, signalCh chan os.Signal) *dispatcherClientDelegate {
	return &dispatcherClientDelegate{
		m_gateSrv:   gateSrv,
		m_singnalCh: signalCh,
	}
}

func (this *dispatcherClientDelegate) HandleDispatcherClientPacket(msgType proto.MsgType, packet *netutil.Packet) {
	this.m_gateSrv.AddDispatcherPacket(msgType, packet)
}

func (this *dispatcherClientDelegate) HandleDispatcherClientDisconnect() {
	//gwlog.Errorf("Disconnected from dispatcher, try reconnecting ...")
	// if gate is disconnected from dispatcher, we just quit
	gwlog.Infof("Disconnected from dispatcher, gate has to quit.")
	this.m_singnalCh <- syscall.SIGTERM // let gate quit
}
