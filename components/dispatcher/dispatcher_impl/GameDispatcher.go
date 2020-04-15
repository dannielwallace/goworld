package dispatcher_impl

import (
	"github.com/dannielwallace/goworld/engine/consts"
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/netutil"
	"github.com/pkg/errors"
	"time"
)

type gameDispatchInfo struct {
	gameid             uint16
	clientProxy        *dispatcherClientProxy
	isBlocked          bool
	blockUntilTime     time.Time // game can be blocked
	pendingPacketQueue []*netutil.Packet
	isBanBootEntity    bool
	lbcheapentry       *lbcheapentry
	m_dispatcherSrv    *DispatcherService
}

func NewGameDispatcherInfo(gameId uint16, isBanBootEntity bool, lbcheapentry *lbcheapentry, dispatcherSrv *DispatcherService) *gameDispatchInfo {
	return &gameDispatchInfo{
		gameid:          gameId,
		isBanBootEntity: isBanBootEntity,
		lbcheapentry:    lbcheapentry,
		m_dispatcherSrv: dispatcherSrv,
	}
}

func (gdi *gameDispatchInfo) setClientProxy(clientProxy *dispatcherClientProxy) {
	gdi.clientProxy = clientProxy
	if gdi.clientProxy != nil && !gdi.isBlocked {
		gdi.sendPendingPackets()
	}
	return
}

func (gdi *gameDispatchInfo) block(duration time.Duration) {
	gdi.isBlocked = true
	gdi.blockUntilTime = time.Now().Add(duration)
}

func (gdi *gameDispatchInfo) checkBlocked() bool {
	if gdi.isBlocked {
		if time.Now().After(gdi.blockUntilTime) {
			gdi.isBlocked = false
			return true
		}
	}
	return false
}

func (gdi *gameDispatchInfo) isConnected() bool {
	return gdi.clientProxy != nil
}

func (gdi *gameDispatchInfo) dispatchPacket(pkt *netutil.Packet) error {
	if gdi.checkBlocked() && gdi.clientProxy == nil {
		// blocked from true -> false, and game is already disconnected before
		// in this case, the game should be cleaned up
		gdi.m_dispatcherSrv.handleGameDown(gdi)
	}

	if !gdi.isBlocked && gdi.clientProxy != nil {
		return gdi.clientProxy.SendPacket(pkt)
	} else {
		if len(gdi.pendingPacketQueue) < consts.GAME_PENDING_PACKET_QUEUE_MAX_LEN {
			gdi.pendingPacketQueue = append(gdi.pendingPacketQueue, pkt)
			pkt.AddRefCount(1)

			if len(gdi.pendingPacketQueue)%1 == 0 {
				gwlog.Warnf("game %d pending packet count = %d, blocked = %v, clientProxy = %s", gdi.gameid, len(gdi.pendingPacketQueue), gdi.isBlocked, gdi.clientProxy)
			}
			return nil
		} else {
			return errors.Errorf("packet to game %d is dropped", gdi.gameid)
		}
	}
}

func (gdi *gameDispatchInfo) unblock() {
	if !gdi.blockUntilTime.IsZero() {
		gdi.blockUntilTime = time.Time{}

		if gdi.clientProxy != nil {
			gdi.sendPendingPackets()
		}
	}
}

func (gdi *gameDispatchInfo) sendPendingPackets() {
	// send the cached calls to target game
	var pendingPackets []*netutil.Packet
	pendingPackets, gdi.pendingPacketQueue = gdi.pendingPacketQueue, nil
	for _, pkt := range pendingPackets {
		_ = gdi.clientProxy.SendPacket(pkt)
		pkt.Release()
	}
}

func (gdi *gameDispatchInfo) clearPendingPackets() {
	var pendingPackets []*netutil.Packet
	pendingPackets, gdi.pendingPacketQueue = gdi.pendingPacketQueue, nil
	for _, pkt := range pendingPackets {
		pkt.Release()
	}
}

