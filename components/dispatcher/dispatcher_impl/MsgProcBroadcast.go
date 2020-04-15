package dispatcher_impl

import (
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/netutil"
)

func (ds *DispatcherService) broadcastToGames(pkt *netutil.Packet) {
	for _, gdi := range ds.m_games {
		_ = gdi.dispatchPacket(pkt)
	}
}
func (ds *DispatcherService) broadcastToGamesRelease(pkt *netutil.Packet) {
	ds.broadcastToGames(pkt)
	pkt.Release()
}
func (ds *DispatcherService) broadcastToGamesExcept(pkt *netutil.Packet, exceptGameID uint16) {
	for gameid, gdi := range ds.m_games {
		if gameid == exceptGameID {
			continue
		}
		_ = gdi.dispatchPacket(pkt)
	}
}

func (ds *DispatcherService) broadcastToGates(pkt *netutil.Packet) {
	for gateid, dcp := range ds.m_gates {
		if dcp != nil {
			_ = dcp.SendPacket(pkt)
		} else {
			gwlog.Errorf("Gate %d is not connected to dispatcher when broadcasting", gateid)
		}
	}
}