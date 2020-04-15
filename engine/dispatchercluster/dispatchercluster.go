package dispatchercluster

import (
	"github.com/dannielwallace/goworld/engine/common"
	"github.com/dannielwallace/goworld/engine/config"
	"github.com/dannielwallace/goworld/engine/dispatchercluster/dispatcherclient"
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/netutil"
	"github.com/dannielwallace/goworld/engine/proto"
)

var (
	dispatcherConns []*dispatcherclient.DispatcherConnMgr
	dispatcherNum   int
	gid             uint16
)

func Initialize(_gid uint16, dctype dispatcherclient.DispatcherClientType, isRestoreGame, isBanBootEntity bool,
	delegate dispatcherclient.IDispatcherClientDelegate) {
	gid = _gid
	if gid == 0 {
		gwlog.Fatalf("gid is 0")
	}

	dispIds := config.GetDispatcherIDs()
	dispatcherNum = len(dispIds)
	if dispatcherNum == 0 {
		gwlog.Fatalf("dispatcher number is 0")
	}

	dispatcherConns = make([]*dispatcherclient.DispatcherConnMgr, dispatcherNum)
	for _, dispid := range dispIds {
		dispatcherConns[dispid-1] = dispatcherclient.NewDispatcherConnMgr(gid, dctype, dispid, isRestoreGame, isBanBootEntity, delegate)
	}
	for _, dispConn := range dispatcherConns {
		dispConn.Connect()
	}
}

func SendCallFilterClientProxies(op proto.FilterClientsOpType, key, val string, method string) {
	pkt := proto.AllocCallFilterClientProxiesPacket(op, key, val, method)
	broadcast(pkt)
	pkt.Release()
	return
}

func broadcast(packet *netutil.Packet) {
	for _, dcm := range dispatcherConns {
		dcm.GetDispatcherClientForSend().SendPacket(packet)
	}
}

func SendGameLBCInfo(lbcCpuUsageInPercent uint16) {
	packet := proto.AllocGameLBCInfoPacket(lbcCpuUsageInPercent)
	broadcast(packet)
	packet.Release()
}

func ClientIDToDispatcherID(clientId common.ClientID) uint16 {
	return uint16((hashClientID(clientId) % dispatcherNum) + 1)
}

func SrvIDToDispatcherID(srvid string) uint16 {
	return uint16((hashSrvID(srvid) % dispatcherNum) + 1)
}

func SelectByClientID(clientId common.ClientID) *dispatcherclient.DispatcherClient {
	idx := hashClientID(clientId) % dispatcherNum
	return dispatcherConns[idx].GetDispatcherClientForSend()
}

func SelectByGateID(gateid uint16) *dispatcherclient.DispatcherClient {
	idx := hashGateID(gateid) % dispatcherNum
	return dispatcherConns[idx].GetDispatcherClientForSend()
}

func SelectByDispatcherID(dispid uint16) *dispatcherclient.DispatcherClient {
	return dispatcherConns[dispid-1].GetDispatcherClientForSend()
}

func SelectBySrvID(srvid string) *dispatcherclient.DispatcherClient {
	idx := hashSrvID(srvid) % dispatcherNum
	return dispatcherConns[idx].GetDispatcherClientForSend()
}
