package dispatcher_impl

import (
	"github.com/dannielwallace/goworld/engine/common"
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/netutil"
)

func (ds *DispatcherService) handleMsgClient2Gs(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	if dcp.gateid <= 0 {
		panic(dcp.gateid)
	}

	// TODO, check msg len for all msg types
	if pkt.GetPayloadLen() < common.CLIENTID_LENGTH {
		panic(pkt.GetPayloadLen())
	}

	clientID := pkt.PeekClientIDFromEnd()
	cdi := ds.m_clients[clientID]
	if cdi != nil {
		_ = cdi.dispatchPacket(pkt)
	} else {
		gwlog.Errorf("[DispatcherService-handleMsgClient2Gs]%s: client %s is send msg, but cdi %s not found",
			ds, dcp, clientID)
	}
}
