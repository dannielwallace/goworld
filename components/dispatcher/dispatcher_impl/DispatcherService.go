package dispatcher_impl

import (
	"fmt"
	"net"

	"time"

	"os"

	"math/rand"

	"container/heap"

	"github.com/dannielwallace/goworld/engine/binutil"
	"github.com/dannielwallace/goworld/engine/common"
	"github.com/dannielwallace/goworld/engine/config"
	"github.com/dannielwallace/goworld/engine/consts"
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/gwutils"
	"github.com/dannielwallace/goworld/engine/netutil"
	"github.com/dannielwallace/goworld/engine/post"
	"github.com/dannielwallace/goworld/engine/proto"
	"github.com/pkg/errors"
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

type dispatcherMessage struct {
	dcp *dispatcherClientProxy
	proto.Message
}

// DispatcherService implements the dispatcher service
type DispatcherService struct {
	m_dispId           uint16
	m_config           *config.DispatcherConfig
	m_clients          map[common.ClientID]*clientDispatchInfo
	m_games            map[uint16]*gameDispatchInfo
	m_bootGames        []uint16
	m_gates            map[uint16]*dispatcherClientProxy
	m_msgQueue         chan dispatcherMessage
	m_clientMsgs2Game  map[uint16]*netutil.Packet // cache client sync infos to games
	m_lbcHeap          lbcheap                    // heap for game load balancing
	m_chooseGameIdx    int                        // choose game in a round robin way
	m_bDeploymentReady bool                       // whether or not the deployment is ready
}

func NewDispatcherService(dispid uint16) *DispatcherService {
	cfg := config.GetDispatcher(dispid)
	ds := &DispatcherService{
		m_dispId:           dispid,
		m_config:           cfg,
		m_msgQueue:         make(chan dispatcherMessage, consts.DISPATCHER_SERVICE_PACKET_QUEUE_SIZE),
		m_clients:          map[common.ClientID]*clientDispatchInfo{},
		m_games:            map[uint16]*gameDispatchInfo{},
		m_gates:            map[uint16]*dispatcherClientProxy{},
		m_clientMsgs2Game:  map[uint16]*netutil.Packet{},
		m_lbcHeap:          nil,
		m_bDeploymentReady: false,
	}

	ds.recalcBootGames()

	return ds
}

func (ds *DispatcherService) messageLoop() {
	ticker := time.Tick(consts.DISPATCHER_SERVICE_TICK_INTERVAL)

	for {
		select {
		case msg := <-ds.m_msgQueue:
			dcp := msg.dcp
			msgtype := msg.MsgType
			pkt := msg.Packet
			if msgtype >= proto.MT_REDIRECT_TO_GATEPROXY_MSG_TYPE_START && msgtype <= proto.MT_REDIRECT_TO_GATEPROXY_MSG_TYPE_STOP {
				ds.handleMsg2SpecClient(dcp, pkt)
			} else {
				switch msgtype {
				/*case proto.MT_QUERY_SPACE_GAMEID_FOR_MIGRATE:
					ds.handleQuerySpaceGameIDForMigrate(dcp, pkt)
				case proto.MT_MIGRATE_REQUEST:
					ds.handleMigrateRequest(dcp, pkt)
				case proto.MT_REAL_MIGRATE:
					ds.handleRealMigrate(dcp, pkt)
				case proto.MT_CANCEL_MIGRATE:
					ds.handleCancelMigrate(dcp, pkt)*/
				case proto.MT_CALL_FILTERED_CLIENTS:
					ds.handleCallFilteredClientProxies(dcp, pkt)
				case proto.MT_NOTIFY_CLIENT_CONNECTED:
					ds.handleNotifyClientConnected(dcp, pkt)
				case proto.MT_NOTIFY_CLIENT_DISCONNECTED:
					ds.handleNotifyClientDisconnected(dcp, pkt)
				case proto.MT_GAME_LBC_INFO:
					ds.handleGameLBCInfo(dcp, pkt)
				case proto.MT_SET_GAME_ID:
					// ds is a game server
					ds.handleSetGameID(dcp, pkt)
				case proto.MT_SET_GATE_ID:
					// ds is a gate
					ds.handleSetGateID(dcp, pkt)
				default:
					gwlog.TraceError("unknown msgtype %d from %s", msgtype, dcp)
				}
			}

			pkt.Release()
			break
		case <-ticker:
			post.Tick()
			ds.sendClientSyncMsgs2Games()
			break
		}
	}
}

func (ds *DispatcherService) Terminate() {
	gwlog.Infof("Dispatcher terminated gracefully.")
	os.Exit(0)
}

func (ds *DispatcherService) String() string {
	return fmt.Sprintf("DispatcherService<%d>", ds.m_dispId)
}

func (ds *DispatcherService) Run() {
	binutil.PrintSupervisorTag(consts.DISPATCHER_STARTED_TAG)
	go gwutils.RepeatUntilPanicless(ds.messageLoop)
	netutil.ServeTCPForever(ds.m_config.ListenAddr, ds)
}

// ServeTCPConnection handles dispatcher client connections to dispatcher
func (ds *DispatcherService) ServeTCPConnection(conn net.Conn) {
	tcpConn := conn.(*net.TCPConn)
	_ = tcpConn.SetReadBuffer(consts.DISPATCHER_CLIENT_PROXY_READ_BUFFER_SIZE)
	_ = tcpConn.SetWriteBuffer(consts.DISPATCHER_CLIENT_PROXY_WRITE_BUFFER_SIZE)

	client := newDispatcherClientProxy(ds, conn)
	client.serve()
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

	gwlog.Infof("%s: %s set gameid = %d", ds, dcp, gameid)
	// reuse the packet to send SET_GAMEID_ACK with all connected gameids
	connectedGameIDs := ds.getConnectedGameIDs()

	_ = dcp.SendSetGameIDAck(ds.m_dispId, ds.m_bDeploymentReady, connectedGameIDs)
	ds.sendNotifyGameConnected(gameid)
	ds.checkDeploymentReady()
	return
}

func (ds *DispatcherService) getConnectedGameIDs() (gameids []uint16) {
	for _, gdi := range ds.m_games {
		if gdi.clientProxy != nil {
			gameids = append(gameids, gdi.gameid)
		}
	}
	return
}

//
//func (service *DispatcherService) sendSetGameIDAck(pkt *netutil.Packet) {
//}

func (ds *DispatcherService) sendNotifyGameConnected(gameid uint16) {
	pkt := proto.MakeNotifyGameConnectedPacket(gameid)
	ds.broadcastToGamesExcept(pkt, gameid)
	pkt.Release()
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
	gwlog.Infof("Gate %d is connected: %s", gateid, dcp)

	olddcp := ds.m_gates[gateid]
	if olddcp != nil {
		gwlog.Warnf("Gate %d connection %s is replaced by new connection %s", gateid, olddcp, dcp)
		_ = olddcp.Close()
		ds.handleGateDisconnected(olddcp)
	}

	ds.m_gates[gateid] = dcp
	ds.checkDeploymentReady()
}

func (ds *DispatcherService) checkDeploymentReady() {
	if ds.m_bDeploymentReady {
		// if deployment was ever ready, it is already ready forever
		return
	}

	deployCfg := config.GetDeployment()
	numGates := len(ds.m_gates)
	if numGates < deployCfg.DesiredGates {
		gwlog.Infof("%s check deployment ready: %d/%d gates", ds, numGates, deployCfg.DesiredGates)
		return
	}
	numGames := 0
	for _, gdi := range ds.m_games {
		if gdi.isBlocked || gdi.clientProxy != nil {
			numGames += 1
		}
	}
	gwlog.Infof("%s check deployment ready: %d/%d games %d/%d gates", ds, numGames, deployCfg.DesiredGames, numGates, deployCfg.DesiredGates)
	if numGames < deployCfg.DesiredGames {
		// games not ready
		return
	}

	// now deployment is ready
	ds.m_bDeploymentReady = true
	// now the deployment is ready for only once
	// broadcast deployment ready to all games
	pkt := proto.MakeNotifyDeploymentReadyPacket()
	ds.broadcastToGamesRelease(pkt)
}

func (ds *DispatcherService) isAllGameClientsConnected() bool {
	for _, gdi := range ds.m_games {
		if !gdi.isConnected() {
			return false
		}
	}
	return true
}

func (ds *DispatcherService) connectedGameClientsNum() int {
	num := 0
	for _, gdi := range ds.m_games {
		if gdi.isConnected() {
			num += 1
		}
	}
	return num
}

func (ds *DispatcherService) dispatchPacketToGame(gameid uint16, pkt *netutil.Packet) error {
	gdi := ds.m_games[gameid]
	if gdi != nil {
		return gdi.dispatchPacket(pkt)
	} else {
		return errors.Errorf("%s: dispatchPacketToGame: game%d is not found", ds, gameid)
	}
}

func (ds *DispatcherService) dispatcherClientOfGate(gateid uint16) *dispatcherClientProxy {
	return ds.m_gates[gateid]
}

// Choose a dispatcher client for sending Anywhere packets
func (ds *DispatcherService) chooseIdleGame() *gameDispatchInfo {
	if len(ds.m_lbcHeap) == 0 {
		return nil
	}

	top := ds.m_lbcHeap[0]
	gwlog.Infof("%s: choose game by lbc: gameid=%d", ds, top.gameid)
	gdi := ds.m_games[top.gameid]

	// after game is chosen, udpate CPU percent by a bit
	ds.m_lbcHeap.chosen(0)
	ds.m_lbcHeap.validateHeapIndexes()
	return gdi
}

// Choose a dispatcher client for sending Anywhere packets
func (ds *DispatcherService) pollBootGameId() uint16 {
	if len(ds.m_bootGames) > 0 {
		gameid := ds.m_bootGames[ds.m_chooseGameIdx%len(ds.m_bootGames)]
		ds.m_chooseGameIdx += 1
		return gameid
	} else {
		gwlog.Errorf("%s pollGsForNewClient: no game server", ds)
		panic(ds.m_bootGames)
		return 0
	}
}

func (ds *DispatcherService) handleDispatcherClientDisconnect(dcp *dispatcherClientProxy) {
	// nothing to do when client disconnected
	defer func() {
		if err := recover(); err != nil {
			gwlog.Errorf("handleDispatcherClientDisconnect paniced: %v", err)
		}
	}()
	gwlog.Warnf("%s disconnected", dcp)
	if dcp.gateid > 0 {
		// gate disconnected, notify all clients disconnected
		ds.handleGateDisconnected(dcp)
	} else if dcp.gameid > 0 {
		ds.handleGameDisconnected(dcp)
	}
}

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
	clientID := pkt.ReadClientID()
	cdi := ds.m_clients[clientID]
	if cdi != nil {
		_ = cdi.dispatchPacket(pkt)
	} else {
		gwlog.Warnf("%s: client %s is disconnected, but cdi %s not found", ds, dcp, clientID)
	}
}

//func (service *DispatcherService) handleServiceDown(gameid uint16, serviceName string, eid common.EntityID) {
//	gwlog.Warnf("%s: service %s: entity %s is down!", service, serviceName, eid)
//	pkt := netutil.NewPacket()
//	pkt.AppendUint16(proto.MT_UNDECLARE_SERVICE)
//	pkt.AppendEntityID(eid)
//	pkt.AppendVarStr(serviceName)
//	service.broadcastToGamesExcept(pkt, gameid)
//	pkt.Release()
//}

func (ds *DispatcherService) sendClientSyncMsgs2Games() {
	if len(ds.m_clientMsgs2Game) == 0 {
		return
	}

	for gameId, pkt := range ds.m_clientMsgs2Game {
		// send the entity sync infos to ds game
		_ = ds.m_games[gameId].dispatchPacket(pkt)
		pkt.Release()
	}
	ds.m_clientMsgs2Game = map[uint16]*netutil.Packet{}
}

func (ds *DispatcherService) handleMsg2SpecClient(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	gid := pkt.ReadUint16()
	_ = ds.dispatcherClientOfGate(gid).SendPacket(pkt)
}

func (ds *DispatcherService) handleCallFilteredClientProxies(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	ds.broadcastToGates(pkt)
}

/*
func (this *DispatcherService) handleQuerySpaceGameIDForMigrate(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	spaceid := pkt.ReadEntityID()
	if consts.DEBUG_PACKETS {
		gwlog.Debugf("%s.handleQuerySpaceGameIDForMigrate: spaceid=%s", this, spaceid)
	}

	spaceDispatchInfo := this.entityDispatchInfos[spaceid]
	var gameid uint16
	if spaceDispatchInfo != nil {
		gameid = spaceDispatchInfo.gameid
	}
	pkt.AppendUint16(gameid)
	// send the packet back
	_ = dcp.SendPacket(pkt)
}

func (this *DispatcherService) handleMigrateRequest(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	entityID := pkt.ReadEntityID()
	spaceID := pkt.ReadEntityID()
	spaceGameID := pkt.ReadUint16()

	if consts.DEBUG_PACKETS {
		gwlog.Debugf("Entity %s is migrating to space %s @ game%d", entityID, spaceID, spaceGameID)
	}

	entityDispatchInfo := this.setEntityDispatcherInfoForWrite(entityID)
	entityDispatchInfo.blockRPC(consts.DISPATCHER_MIGRATE_TIMEOUT)
	_ = dcp.SendPacket(pkt)
}

func (this *DispatcherService) handleCancelMigrate(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	entityid := pkt.ReadEntityID()

	if consts.DEBUG_PACKETS {
		gwlog.Debugf("Entity %s cancelled migrating", entityid)
	}

	entityDispatchInfo := this.entityDispatchInfos[entityid]
	if entityDispatchInfo != nil {
		entityDispatchInfo.unblock()
	}
}

func (this *DispatcherService) handleRealMigrate(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	// get spaceID and make sure it exists
	eid := pkt.ReadEntityID()
	targetGame := pkt.ReadUint16() // target game of migration
	// target space is not checked for existence, because we relay the packet anyway

	// mark the eid as migrating done
	entityDispatchInfo := this.setEntityDispatcherInfoForWrite(eid)

	entityDispatchInfo.gameid = targetGame

	_ = this.dispatchPacketToGame(targetGame, pkt)
	// send the cached calls to target game
	entityDispatchInfo.unblock()
}
*/

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

func (ds *DispatcherService) recalcBootGames() {
	var candidates []uint16
	for gameid, gdi := range ds.m_games {
		if !gdi.isBanBootEntity {
			candidates = append(candidates, gameid)
		}
	}
	ds.m_bootGames = candidates
}

func (ds *DispatcherService) handleGameLBCInfo(dcp *dispatcherClientProxy, packet *netutil.Packet) {
	// handle game LBC info from game
	var cpuUsageInPercent float64
	cpuUsageInPercent = float64(packet.ReadUint16())
	gwlog.Debugf("Game %d Load Balancing Info: %+v", dcp.gameid, cpuUsageInPercent)

	cpuUsageInPercent *= 1 + (rand.Float64() * 0.1) // multiply CPUPercent by a random factor 1.0 ~ 1.1
	gdi := ds.m_games[dcp.gameid]
	gdi.lbcheapentry.update(cpuUsageInPercent)
	heap.Fix(&ds.m_lbcHeap, gdi.lbcheapentry.heapidx)
	ds.m_lbcHeap.validateHeapIndexes()
}
