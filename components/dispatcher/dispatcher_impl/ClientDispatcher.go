package dispatcher_impl

import (
	"github.com/dannielwallace/goworld/engine/consts"
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/netutil"
	"github.com/pkg/errors"
	"time"
)

type clientDispatchInfo struct {
	m_gateId             uint16
	m_gameId             uint16
	m_blockUntilTime     time.Time
	m_pendingPacketQueue []*netutil.Packet
	m_dispatcherSrv      *DispatcherService
}

func NewClientDispatchInfo(dispatcherSrv *DispatcherService, gateId uint16, gameId uint16) *clientDispatchInfo {
	return &clientDispatchInfo{
		m_gateId:             gateId,
		m_gameId:             gameId,
		m_blockUntilTime:     time.Time{},
		m_pendingPacketQueue: []*netutil.Packet{},
		m_dispatcherSrv:      dispatcherSrv,
	}
}

func (cdi *clientDispatchInfo) blockRPC(d time.Duration) {
	t := time.Now().Add(d)
	if cdi.m_blockUntilTime.Before(t) {
		cdi.m_blockUntilTime = t
	}
}

func (cdi *clientDispatchInfo) dispatchPacket(pkt *netutil.Packet) error {
	if cdi.m_blockUntilTime.IsZero() {
		// most common case. handle it quickly
		return cdi.m_dispatcherSrv.dispatchPacketToGame(cdi.m_gameId, pkt)
	}

	// blockUntilTime is set, need to check if block should be released
	now := time.Now()
	if now.Before(cdi.m_blockUntilTime) {
		// keep blocking, just put the call to wait
		if len(cdi.m_pendingPacketQueue) < consts.CLIENT_PENDING_PACKET_QUEUE_MAX_LEN {
			pkt.AddRefCount(1)
			cdi.m_pendingPacketQueue = append(cdi.m_pendingPacketQueue, pkt)
			return nil
		} else {
			gwlog.Errorf("%s.dispatchPacket: packet queue too long, packet dropped", cdi)
			return errors.Errorf("%s: packet of entity dropped", cdi.m_dispatcherSrv)
		}
	} else {
		// time to unblock
		cdi.unblock()
		return nil
	}
}

func (cdi *clientDispatchInfo) unblock() {
	if !cdi.m_blockUntilTime.IsZero() { // entity is loading, it's done now
		//gwlog.Infof("entity is loaded now, clear loadTime")
		cdi.m_blockUntilTime = time.Time{}

		targetGame := cdi.m_gameId
		// send the cached calls to target game
		pendingPackets := cdi.m_pendingPacketQueue
		cdi.m_pendingPacketQueue = []*netutil.Packet{}
		for _, pkt := range pendingPackets {
			_ = cdi.m_dispatcherSrv.dispatchPacketToGame(targetGame, pkt)
			pkt.Release()
		}
	}
}

