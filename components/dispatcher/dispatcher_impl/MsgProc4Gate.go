package dispatcher_impl

import (
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/netutil"
	"github.com/dannielwallace/goworld/engine/proto"
)

func (ds *DispatcherService) handleGateDisconnected(dcp *dispatcherClientProxy) {
	gateid := dcp.gateid
	gwlog.Warnf("Gate %d connection %s is down!", gateid, dcp)

	curdcp := ds.m_gates[gateid]
	if curdcp != dcp {
		gwlog.Errorf("Gate %d connection %s is down, but the current connection is %s", gateid, dcp, curdcp)
		return
	}

	// should always goes here
	delete(ds.m_gates, gateid)
	// notify all games of gate down
	pkt := netutil.NewPacket()
	pkt.AppendUint16(proto.MT_NOTIFY_GATE_DISCONNECTED)
	pkt.AppendUint16(gateid)
	ds.broadcastToGamesRelease(pkt)
}

func (ds *DispatcherService) handleSetGateID(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	gateid := pkt.ReadUint16()
	if gateid <= 0 {
		gwlog.Panicf("invalid gateid: %d", gateid)
	}
	if dcp.gameid > 0 || dcp.gateid > 0 {
		gwlog.Panicf("already set gameid=%d, gateid=%d", dcp.gameid, dcp.gateid)
	}

	dcp.gateid = gateid
	gwlog.Infof("[DispatcherService-handleSetGateID] Gate %d is connected: %s", gateid, dcp)

	olddcp := ds.m_gates[gateid]
	if olddcp != nil {
		gwlog.Warnf("Gate %d connection %s is replaced by new connection %s", gateid, olddcp, dcp)
		_ = olddcp.Close()
		ds.handleGateDisconnected(olddcp)
	}

	ds.m_gates[gateid] = dcp
	ds.checkDeploymentReady()
}

func (ds *DispatcherService) handleNotifyClientConnected(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	if dcp.gateid <= 0 {
		panic(dcp.gateid)
	}

	clientID := pkt.ReadClientID()
	// round robin a gs to send
	gameId := ds.pollBootGameId()
	if gameId <= 0 {
		gwlog.Errorf("[DispatcherService-handleNotifyClientConnected]%s: pollBootGameId ret invalid gameId:[%v]",
			ds, gameId)
		return
	}

	// cache client dispatch info
	cdi := NewClientDispatchInfo(ds, dcp.gateid, gameId)
	ds.m_clients[clientID] = cdi

	pkt.AppendUint16(dcp.gateid)
	_ = cdi.dispatchPacket(pkt)
}

func (ds *DispatcherService) handleNotifyClientDisconnected(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	if dcp.gateid <= 0 {
		panic(dcp.gateid)
	}

	clientID := pkt.ReadClientID()
	cdi := ds.m_clients[clientID]
	if cdi != nil {
		_ = cdi.dispatchPacket(pkt)
		delete(ds.m_clients, clientID)
	} else {
		gwlog.Errorf("%s: client %s is disconnected, but cdi %s not found", ds, dcp, clientID)
	}
}
