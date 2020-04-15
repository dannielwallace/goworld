package dispatcher_impl

import (
	"container/heap"
	"github.com/dannielwallace/goworld/engine/consts"
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/netutil"
	"github.com/dannielwallace/goworld/engine/proto"
	"math/rand"
)

func (ds *DispatcherService) handleGameDisconnected(dcp *dispatcherClientProxy) {
	gameid := dcp.gameid
	gwlog.Errorf("%s: game %d is down: %s", ds, gameid, dcp)
	gdi := ds.m_games[gameid]
	if gdi == nil {
		gwlog.Errorf("%s handleGameDisconnected: game%d is not found", ds, gameid)
		return
	}

	if dcp != gdi.clientProxy {
		// ds connection is not the current connection
		gwlog.Errorf("%s: game%d connection %s is disconnected, but the current connection is %s", ds, gameid, dcp, gdi.clientProxy)
		return
	}

	gdi.clientProxy = nil // connection down, set clientProxy = nil
	if !gdi.isBlocked {
		// game is down, we need to clear all
		ds.handleGameDown(gdi)
	} else {
		// game is freezed, wait for restore, setup a timer to cleanup later if restore is not success
	}
}

func (ds *DispatcherService) handleGameDown(gdi *gameDispatchInfo) {
	gameid := gdi.gameid
	gwlog.Infof("%s: game%d is down, cleaning up...", ds, gameid)
	gdi.clearPendingPackets()

	// send game down packet to all games
	ds.broadcastToGamesRelease(proto.MakeNotifyGameDisconnectedPacket(gameid))
}

func (ds *DispatcherService) handleSetGameID(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	gameid := pkt.ReadUint16()
	isReconnect := pkt.ReadBool()
	isRestore := pkt.ReadBool()
	isBanBootEntity := pkt.ReadBool()

	gwlog.Infof("%s: connection %s set gameid=%d, isReconnect=%v, isRestore=%v, isBanBootEntity=%v",
		ds, dcp, gameid, isReconnect, isRestore, isBanBootEntity)

	if gameid <= 0 {
		gwlog.Panicf("invalid gameid: %d", gameid)
	}
	if dcp.gameid > 0 || dcp.gateid > 0 {
		gwlog.Panicf("already set gameid=%d, gateid=%d", dcp.gameid, dcp.gateid)
	}
	dcp.gameid = gameid

	if consts.DEBUG_PACKETS {
		gwlog.Debugf("%s.handleSetGameID: dcp=%s, gameid=%d, isReconnect=%v", ds, dcp, gameid, isReconnect)
	}

	gdi := ds.m_games[gameid]
	if gdi == nil {
		// new game connected, create dispatch info for the game
		lbcheapentry := &lbcheapentry{gameid, len(ds.m_lbcHeap), 0, 0}
		gdi = NewGameDispatcherInfo(gameid, isBanBootEntity, lbcheapentry, ds)
		ds.m_games[gameid] = gdi
		heap.Push(&ds.m_lbcHeap, lbcheapentry)
		ds.m_lbcHeap.validateHeapIndexes()

		if !isBanBootEntity {
			ds.m_bootGames = append(ds.m_bootGames, gameid)
		}
	} else if gdi.clientProxy != nil {
		_ = gdi.clientProxy.Close()
		ds.handleGameDisconnected(gdi.clientProxy)
	}

	oldIsBanBootEntity := gdi.isBanBootEntity
	gdi.isBanBootEntity = isBanBootEntity
	gdi.setClientProxy(dcp) // should be nil, unless reconnect
	gdi.unblock()           // unlock game dispatch info if new game is connected
	if oldIsBanBootEntity != isBanBootEntity {
		ds.recalcBootGames() // recalc if necessary
	}

	gwlog.Infof("[DispatcherService-handleSetGameID]%s: %s set gameid = %d", ds, dcp, gameid)
	// reuse the packet to send SET_GAMEID_ACK with all connected gameids
	connectedGameIDs := ds.getConnectedGameIDs()

	_ = dcp.SendSetGameIDAck(ds.m_dispId, ds.m_bDeploymentReady, connectedGameIDs)
	ds.sendNotifyGameConnected(gameid)
	ds.checkDeploymentReady()
	return
}

func (ds *DispatcherService) sendNotifyGameConnected(gameid uint16) {
	pkt := proto.MakeNotifyGameConnectedPacket(gameid)
	ds.broadcastToGamesExcept(pkt, gameid)
	pkt.Release()
}

func (ds *DispatcherService) handleGameLBCInfo(dcp *dispatcherClientProxy, packet *netutil.Packet) {
	// handle game LBC info from game
	var cpuUsageInPercent float64
	cpuUsageInPercent = float64(packet.ReadUint16())
	// gwlog.Debugf("Game %d Load Balancing Info: %+v", dcp.gameid, cpuUsageInPercent)

	cpuUsageInPercent *= 1 + (rand.Float64() * 0.1) // multiply CPUPercent by a random factor 1.0 ~ 1.1
	gdi := ds.m_games[dcp.gameid]
	gdi.lbcheapentry.update(cpuUsageInPercent)
	heap.Fix(&ds.m_lbcHeap, gdi.lbcheapentry.heapidx)
	ds.m_lbcHeap.validateHeapIndexes()
}
