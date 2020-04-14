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

type entityDispatchInfo struct {
	gameid				uint16
	blockUntilTime		time.Time
	pendingPacketQueue	[]*netutil.Packet
	m_dispatcherSrv		*DispatcherService
}

func NewEntityDispatchInfo(dispatcherSrv *DispatcherService) *entityDispatchInfo {
	return &entityDispatchInfo{
		gameid:             0,
		blockUntilTime:     time.Time{},
		pendingPacketQueue: nil,
		m_dispatcherSrv:	dispatcherSrv,
	}
}

func (this *entityDispatchInfo) blockRPC(d time.Duration) {
	t := time.Now().Add(d)
	if this.blockUntilTime.Before(t) {
		this.blockUntilTime = t
	}
}

func (edi *entityDispatchInfo) dispatchPacket(pkt *netutil.Packet) error {
	if edi.blockUntilTime.IsZero() {
		// most common case. handle it quickly
		return edi.m_dispatcherSrv.dispatchPacketToGame(edi.gameid, pkt)
	}

	// blockUntilTime is set, need to check if block should be released
	now := time.Now()
	if now.Before(edi.blockUntilTime) {
		// keep blocking, just put the call to wait
		if len(edi.pendingPacketQueue) < consts.ENTITY_PENDING_PACKET_QUEUE_MAX_LEN {
			pkt.AddRefCount(1)
			edi.pendingPacketQueue = append(edi.pendingPacketQueue, pkt)
			return nil
		} else {
			gwlog.Errorf("%s.dispatchPacket: packet queue too long, packet dropped", edi)
			return errors.Errorf("%s: packet of entity dropped", edi.m_dispatcherSrv)
		}
	} else {
		// time to unblock
		edi.unblock()
		return nil
	}
}

func (this *entityDispatchInfo) unblock() {
	if !this.blockUntilTime.IsZero() { // entity is loading, it's done now
		//gwlog.Infof("entity is loaded now, clear loadTime")
		this.blockUntilTime = time.Time{}

		targetGame := this.gameid
		// send the cached calls to target game
		var pendingPackets []*netutil.Packet
		pendingPackets, this.pendingPacketQueue = this.pendingPacketQueue, nil
		for _, pkt := range pendingPackets {
			_ = this.m_dispatcherSrv.dispatchPacketToGame(targetGame, pkt)
			pkt.Release()
		}
	}
}

type gameDispatchInfo struct {
	gameid				uint16
	clientProxy			*dispatcherClientProxy
	isBlocked			bool
	blockUntilTime		time.Time // game can be blocked
	pendingPacketQueue	[]*netutil.Packet
	isBanBootEntity		bool
	lbcheapentry		*lbcheapentry
	m_dispatcherSrv		*DispatcherService
}

func NewGameDispatcherInfo(gameId uint16, isBanBootEntity bool, lbcheapentry *lbcheapentry, dispatcherSrv *DispatcherService) *gameDispatchInfo {
	return &gameDispatchInfo{
		gameid: gameId,
		isBanBootEntity: isBanBootEntity,
		lbcheapentry: lbcheapentry,
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
	dispid                uint16
	config                *config.DispatcherConfig
	games                 map[uint16]*gameDispatchInfo
	bootGames             []uint16
	gates                 map[uint16]*dispatcherClientProxy
	messageQueue          chan dispatcherMessage
	entityDispatchInfos   map[common.EntityID]*entityDispatchInfo
	kvregRegisterMap      map[string]string
	entitySyncInfosToGame map[uint16]*netutil.Packet // cache entity sync infos to gates
	ticker                <-chan time.Time
	lbcheap               lbcheap // heap for game load balancing
	chooseGameIdx         int     // choose game in a round robin way
	isDeploymentReady     bool    // whether or not the deployment is ready
}

func NewDispatcherService(dispid uint16) *DispatcherService {
	cfg := config.GetDispatcher(dispid)
	ds := &DispatcherService{
		dispid:                dispid,
		config:                cfg,
		messageQueue:          make(chan dispatcherMessage, consts.DISPATCHER_SERVICE_PACKET_QUEUE_SIZE),
		games:                 map[uint16]*gameDispatchInfo{},
		gates:                 map[uint16]*dispatcherClientProxy{},
		entityDispatchInfos:   map[common.EntityID]*entityDispatchInfo{},
		kvregRegisterMap:      map[string]string{},
		entitySyncInfosToGame: map[uint16]*netutil.Packet{},
		ticker:                time.Tick(consts.DISPATCHER_SERVICE_TICK_INTERVAL),
		lbcheap:               nil,
		isDeploymentReady:     false,
	}

	ds.recalcBootGames()

	return ds
}

func (this *DispatcherService) messageLoop() {
	for {
		select {
		case msg := <-this.messageQueue:
			dcp := msg.dcp
			msgtype := msg.MsgType
			pkt := msg.Packet
			if msgtype >= proto.MT_REDIRECT_TO_GATEPROXY_MSG_TYPE_START && msgtype <= proto.MT_REDIRECT_TO_GATEPROXY_MSG_TYPE_STOP {
				this.handleDoSomethingOnSpecifiedClient(dcp, pkt)
			} else {
				switch msgtype {
				case proto.MT_SYNC_POSITION_YAW_FROM_CLIENT:
					this.handleSyncPositionYawFromClient(dcp, pkt)
				case proto.MT_SYNC_POSITION_YAW_ON_CLIENTS:
					this.handleSyncPositionYawOnClients(dcp, pkt)
				case proto.MT_CALL_ENTITY_METHOD:
					this.handleCallEntityMethod(dcp, pkt)
				case proto.MT_CALL_ENTITY_METHOD_FROM_CLIENT:
					this.handleCallEntityMethodFromClient(dcp, pkt)
				case proto.MT_QUERY_SPACE_GAMEID_FOR_MIGRATE:
					this.handleQuerySpaceGameIDForMigrate(dcp, pkt)
				case proto.MT_MIGRATE_REQUEST:
					this.handleMigrateRequest(dcp, pkt)
				case proto.MT_REAL_MIGRATE:
					this.handleRealMigrate(dcp, pkt)
				case proto.MT_CALL_FILTERED_CLIENTS:
					this.handleCallFilteredClientProxies(dcp, pkt)
				case proto.MT_NOTIFY_CLIENT_CONNECTED:
					this.handleNotifyClientConnected(dcp, pkt)
				case proto.MT_NOTIFY_CLIENT_DISCONNECTED:
					this.handleNotifyClientDisconnected(dcp, pkt)
				case proto.MT_LOAD_ENTITY_SOMEWHERE:
					this.handleLoadEntitySomewhere(dcp, pkt)
				case proto.MT_NOTIFY_CREATE_ENTITY:
					eid := pkt.ReadEntityID()
					this.handleNotifyCreateEntity(dcp, pkt, eid)
				case proto.MT_NOTIFY_DESTROY_ENTITY:
					eid := pkt.ReadEntityID()
					this.handleNotifyDestroyEntity(dcp, pkt, eid)
				case proto.MT_CREATE_ENTITY_SOMEWHERE:
					this.handleCreateEntitySomewhere(dcp, pkt)
				case proto.MT_GAME_LBC_INFO:
					this.handleGameLBCInfo(dcp, pkt)
				case proto.MT_CALL_NIL_SPACES:
					this.handleCallNilSpaces(dcp, pkt)
				case proto.MT_CANCEL_MIGRATE:
					this.handleCancelMigrate(dcp, pkt)
				case proto.MT_KVREG_REGISTER:
					this.handleKvregRegister(dcp, pkt)
				case proto.MT_SET_GAME_ID:
					// this is a game server
					this.handleSetGameID(dcp, pkt)
				case proto.MT_SET_GATE_ID:
					// this is a gate
					this.handleSetGateID(dcp, pkt)
				case proto.MT_START_FREEZE_GAME:
					// freeze the game
					this.handleStartFreezeGame(dcp, pkt)
				default:
					gwlog.TraceError("unknown msgtype %d from %s", msgtype, dcp)
				}
			}

			pkt.Release()
			break
		case <-this.ticker:
			post.Tick()
			this.sendEntitySyncInfosToGames()
			break
		}
	}
}

func (this *DispatcherService) Terminate() {
	gwlog.Infof("Dispatcher terminated gracefully.")
	os.Exit(0)
}

func (this *DispatcherService) delEntityDispatchInfo(entityID common.EntityID) {
	delete(this.entityDispatchInfos, entityID)
}

func (this *DispatcherService) setEntityDispatcherInfoForWrite(entityID common.EntityID) (info *entityDispatchInfo) {
	info = this.entityDispatchInfos[entityID]

	if info == nil {
		info = NewEntityDispatchInfo(this)
		this.entityDispatchInfos[entityID] = info
	}

	return
}

func (this *DispatcherService) String() string {
	return fmt.Sprintf("DispatcherService<%d>", this.dispid)
}

func (this *DispatcherService) Run() {
	binutil.PrintSupervisorTag(consts.DISPATCHER_STARTED_TAG)
	go gwutils.RepeatUntilPanicless(this.messageLoop)
	netutil.ServeTCPForever(this.config.ListenAddr, this)
}

// ServeTCPConnection handles dispatcher client connections to dispatcher
func (this *DispatcherService) ServeTCPConnection(conn net.Conn) {
	tcpConn := conn.(*net.TCPConn)
	_ = tcpConn.SetReadBuffer(consts.DISPATCHER_CLIENT_PROXY_READ_BUFFER_SIZE)
	_ = tcpConn.SetWriteBuffer(consts.DISPATCHER_CLIENT_PROXY_WRITE_BUFFER_SIZE)

	client := newDispatcherClientProxy(this, conn)
	client.serve()
}

func (this *DispatcherService) handleSetGameID(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	gameid := pkt.ReadUint16()
	isReconnect := pkt.ReadBool()
	isRestore := pkt.ReadBool()
	isBanBootEntity := pkt.ReadBool()
	numEntities := pkt.ReadUint32() // number of entities on the game
	//for i := uint32(0); i < numEntities; i++ {
	//	eid := pkt.ReadEntityID()
	//}

	gwlog.Infof("%s: connection %s set gameid=%d, isReconnect=%v, isRestore=%v, isBanBootEntity=%v, numEntities=%d", this, dcp, gameid, isReconnect, isRestore, isBanBootEntity, numEntities)

	if gameid <= 0 {
		gwlog.Panicf("invalid gameid: %d", gameid)
	}
	if dcp.gameid > 0 || dcp.gateid > 0 {
		gwlog.Panicf("already set gameid=%d, gateid=%d", dcp.gameid, dcp.gateid)
	}
	dcp.gameid = gameid

	if consts.DEBUG_PACKETS {
		gwlog.Debugf("%s.handleSetGameID: dcp=%s, gameid=%d, isReconnect=%v", this, dcp, gameid, isReconnect)
	}

	gdi := this.games[gameid]
	if gdi == nil {
		// new game connected, create dispatch info for the game
		lbcheapentry := &lbcheapentry{gameid, len(this.lbcheap), 0, 0}
		gdi = NewGameDispatcherInfo(gameid, isBanBootEntity, lbcheapentry, this)
		this.games[gameid] = gdi
		heap.Push(&this.lbcheap, lbcheapentry)
		this.lbcheap.validateHeapIndexes()

		if !isBanBootEntity {
			this.bootGames = append(this.bootGames, gameid)
		}
	} else if gdi.clientProxy != nil {
		_ = gdi.clientProxy.Close()
		this.handleGameDisconnected(gdi.clientProxy)
	}

	oldIsBanBootEntity := gdi.isBanBootEntity
	gdi.isBanBootEntity = isBanBootEntity
	gdi.setClientProxy(dcp) // should be nil, unless reconnect
	gdi.unblock()           // unlock game dispatch info if new game is connected
	if oldIsBanBootEntity != isBanBootEntity {
		this.recalcBootGames() // recalc if necessary
	}

	// restore all entities for the game from the packet
	var rejectEntities []common.EntityID
	for i := uint32(0); i < numEntities; i++ {
		eid := pkt.ReadEntityID()
		edi := this.setEntityDispatcherInfoForWrite(eid)
		if edi.gameid == gameid {
			// the current game for the entity is not changed
			edi.unblock()
		} else if edi.gameid == 0 {
			// the entity has no game yet, set to this game
			edi.gameid = gameid
			edi.unblock()
		} else {
			// the entity is on other game ... need to tell the game to destroy his version of entity
			rejectEntities = append(rejectEntities, eid)
		}
	}

	gwlog.Infof("%s: %s set gameid = %d, numEntities = %d, rejectEntites = %d, services = %v", this, dcp, gameid, numEntities, len(rejectEntities), this.kvregRegisterMap)
	// reuse the packet to send SET_GAMEID_ACK with all connected gameids
	connectedGameIDs := this.getConnectedGameIDs()

	_ = dcp.SendSetGameIDAck(this.dispid, this.isDeploymentReady, connectedGameIDs, rejectEntities, this.kvregRegisterMap)
	this.sendNotifyGameConnected(gameid)
	this.checkDeploymentReady()
	return
}

func (this *DispatcherService) getConnectedGameIDs() (gameids []uint16) {
	for _, gdi := range this.games {
		if gdi.clientProxy != nil {
			gameids = append(gameids, gdi.gameid)
		}
	}
	return
}

//
//func (service *DispatcherService) sendSetGameIDAck(pkt *netutil.Packet) {
//}

func (this *DispatcherService) sendNotifyGameConnected(gameid uint16) {
	pkt := proto.MakeNotifyGameConnectedPacket(gameid)
	this.broadcastToGamesExcept(pkt, gameid)
	pkt.Release()
}

func (this *DispatcherService) handleSetGateID(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	gateid := pkt.ReadUint16()
	if gateid <= 0 {
		gwlog.Panicf("invalid gateid: %d", gateid)
	}
	if dcp.gameid > 0 || dcp.gateid > 0 {
		gwlog.Panicf("already set gameid=%d, gateid=%d", dcp.gameid, dcp.gateid)
	}

	dcp.gateid = gateid
	gwlog.Infof("Gate %d is connected: %s", gateid, dcp)

	olddcp := this.gates[gateid]
	if olddcp != nil {
		gwlog.Warnf("Gate %d connection %s is replaced by new connection %s", gateid, olddcp, dcp)
		_ = olddcp.Close()
		this.handleGateDisconnected(olddcp)
	}

	this.gates[gateid] = dcp
	this.checkDeploymentReady()
}

func (this *DispatcherService) checkDeploymentReady() {
	if this.isDeploymentReady {
		// if deployment was ever ready, it is already ready forever
		return
	}

	deployCfg := config.GetDeployment()
	numGates := len(this.gates)
	if numGates < deployCfg.DesiredGates {
		gwlog.Infof("%s check deployment ready: %d/%d gates", this, numGates, deployCfg.DesiredGates)
		return
	}
	numGames := 0
	for _, gdi := range this.games {
		if gdi.isBlocked || gdi.clientProxy != nil {
			numGames += 1
		}
	}
	gwlog.Infof("%s check deployment ready: %d/%d games %d/%d gates", this, numGames, deployCfg.DesiredGames, numGates, deployCfg.DesiredGates)
	if numGames < deployCfg.DesiredGames {
		// games not ready
		return
	}

	// now deployment is ready
	this.isDeploymentReady = true
	// now the deployment is ready for only once
	// broadcast deployment ready to all games
	pkt := proto.MakeNotifyDeploymentReadyPacket()
	this.broadcastToGamesRelease(pkt)
}

func (this *DispatcherService) handleStartFreezeGame(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	// freeze the game, which block all entities of that game
	gwlog.Infof("Handling start freeze game ...")
	gameid := dcp.gameid
	gdi := this.games[gameid]
	if gdi == nil {
		gwlog.Panicf("%s handleStartFreezeGame: game%d not found", this, gameid)
	}

	gdi.block(consts.DISPATCHER_FREEZE_GAME_TIMEOUT)

	// tell the game to start real freeze, re-using the packet
	pkt.ClearPayload()
	pkt.AppendUint16(proto.MT_START_FREEZE_GAME_ACK)
	pkt.AppendUint16(this.dispid)
	_ = dcp.SendPacket(pkt)
}

func (this *DispatcherService) isAllGameClientsConnected() bool {
	for _, gdi := range this.games {
		if !gdi.isConnected() {
			return false
		}
	}
	return true
}

func (this *DispatcherService) connectedGameClientsNum() int {
	num := 0
	for _, gdi := range this.games {
		if gdi.isConnected() {
			num += 1
		}
	}
	return num
}

func (this *DispatcherService) dispatchPacketToGame(gameid uint16, pkt *netutil.Packet) error {
	gdi := this.games[gameid]
	if gdi != nil {
		return gdi.dispatchPacket(pkt)
	} else {
		return errors.Errorf("%s: dispatchPacketToGame: game%d is not found", this, gameid)
	}
}

func (this *DispatcherService) dispatcherClientOfGate(gateid uint16) *dispatcherClientProxy {
	return this.gates[gateid]
}

// Choose a dispatcher client for sending Anywhere packets
func (this *DispatcherService) chooseGame() *gameDispatchInfo {
	if len(this.lbcheap) == 0 {
		return nil
	}

	top := this.lbcheap[0]
	gwlog.Infof("%s: choose game by lbc: gameid=%d", this, top.gameid)
	gdi := this.games[top.gameid]

	// after game is chosen, udpate CPU percent by a bit
	this.lbcheap.chosen(0)
	this.lbcheap.validateHeapIndexes()
	return gdi
}

// Choose a dispatcher client for sending Anywhere packets
func (this *DispatcherService) chooseGameForBootEntity() *gameDispatchInfo {

	if len(this.bootGames) > 0 {
		gameid := this.bootGames[this.chooseGameIdx%len(this.bootGames)]
		this.chooseGameIdx += 1
		return this.games[gameid]
	} else {
		gwlog.Errorf("%s chooseGameForBootEntity: no game", this)
		return nil
	}
}

func (this *DispatcherService) handleDispatcherClientDisconnect(dcp *dispatcherClientProxy) {
	// nothing to do when client disconnected
	defer func() {
		if err := recover(); err != nil {
			gwlog.Errorf("handleDispatcherClientDisconnect paniced: %v", err)
		}
	}()
	gwlog.Warnf("%s disconnected", dcp)
	if dcp.gateid > 0 {
		// gate disconnected, notify all clients disconnected
		this.handleGateDisconnected(dcp)
	} else if dcp.gameid > 0 {
		this.handleGameDisconnected(dcp)
	}
}

func (this *DispatcherService) handleGateDisconnected(dcp *dispatcherClientProxy) {
	gateid := dcp.gateid
	gwlog.Warnf("Gate %d connection %s is down!", gateid, dcp)

	curdcp := this.gates[gateid]
	if curdcp != dcp {
		gwlog.Errorf("Gate %d connection %s is down, but the current connection is %s", gateid, dcp, curdcp)
		return
	}

	// should always goes here
	delete(this.gates, gateid)
	// notify all games of gate down
	pkt := netutil.NewPacket()
	pkt.AppendUint16(proto.MT_NOTIFY_GATE_DISCONNECTED)
	pkt.AppendUint16(gateid)
	this.broadcastToGamesRelease(pkt)
}

func (this *DispatcherService) handleGameDisconnected(dcp *dispatcherClientProxy) {
	gameid := dcp.gameid
	gwlog.Errorf("%s: game %d is down: %s", this, gameid, dcp)
	gdi := this.games[gameid]
	if gdi == nil {
		gwlog.Errorf("%s handleGameDisconnected: game%d is not found", this, gameid)
		return
	}

	if dcp != gdi.clientProxy {
		// this connection is not the current connection
		gwlog.Errorf("%s: game%d connection %s is disconnected, but the current connection is %s", this, gameid, dcp, gdi.clientProxy)
		return
	}

	gdi.clientProxy = nil // connection down, set clientProxy = nil
	if !gdi.isBlocked {
		// game is down, we need to clear all
		this.handleGameDown(gdi)
	} else {
		// game is freezed, wait for restore, setup a timer to cleanup later if restore is not success
	}

}

func (this *DispatcherService) handleGameDown(gdi *gameDispatchInfo) {
	gameid := gdi.gameid
	gwlog.Infof("%s: game%d is down, cleaning up...", this, gameid)
	this.cleanupEntitiesOfGame(gameid)
	gdi.clearPendingPackets()

	// send gamedown packet to all games
	this.broadcastToGamesRelease(proto.MakeNotifyGameDisconnectedPacket(gameid))
}

func (this *DispatcherService) cleanupEntitiesOfGame(gameid uint16) {
	cleanEids := common.EntityIDSet{} // get all clean eids
	for eid, dispatchInfo := range this.entityDispatchInfos {
		if dispatchInfo.gameid == gameid {
			cleanEids.Add(eid)
		}
	}

	for eid := range cleanEids {
		this.cleanupEntityInfo(eid)
	}

	gwlog.Infof("%s: game%d is down, %d entities cleaned", this, gameid, len(cleanEids))
}

// Entity is create on the target game
func (this *DispatcherService) handleNotifyCreateEntity(dcp *dispatcherClientProxy, pkt *netutil.Packet, entityID common.EntityID) {
	if consts.DEBUG_PACKETS {
		gwlog.Debugf("%s.handleNotifyCreateEntity: dcp=%s, entityID=%s", this, dcp, entityID)
	}
	entityDispatchInfo := this.setEntityDispatcherInfoForWrite(entityID)
	entityDispatchInfo.gameid = dcp.gameid
	entityDispatchInfo.unblock()
}

func (this *DispatcherService) handleNotifyDestroyEntity(dcp *dispatcherClientProxy, pkt *netutil.Packet, entityID common.EntityID) {
	if consts.DEBUG_PACKETS {
		gwlog.Debugf("%s.handleNotifyDestroyEntity: dcp=%s, entityID=%s", this, dcp, entityID)
	}
	this.cleanupEntityInfo(entityID)
}

func (this *DispatcherService) cleanupEntityInfo(entityID common.EntityID) {
	this.delEntityDispatchInfo(entityID)
}

func (this *DispatcherService) handleNotifyClientConnected(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	targetGame := this.chooseGameForBootEntity()
	pkt.AppendUint16(dcp.gateid)
	_ = targetGame.dispatchPacket(pkt)
}

func (this *DispatcherService) handleNotifyClientDisconnected(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	ownerEntityID := pkt.ReadEntityID() // owner entity's ID for the client
	edi := this.entityDispatchInfos[ownerEntityID]
	if edi != nil {
		_ = edi.dispatchPacket(pkt)
	} else {
		gwlog.Warnf("%s: client %s is disconnected, but owner entity %s not found", this, dcp, ownerEntityID)
	}
}

func (this *DispatcherService) handleLoadEntitySomewhere(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	//typeName := pkt.ReadVarStr()
	//eid := pkt.ReadEntityID()
	if consts.DEBUG_PACKETS {
		gwlog.Debugf("%s.handleLoadEntitySomewhere: dcp=%s, pkt=%v", this, dcp, pkt.Payload())
	}
	gameid := pkt.ReadUint16() // the target game to create entity or 0 for anywhere
	eid := pkt.ReadEntityID()  // field 1

	entityDispatchInfo := this.setEntityDispatcherInfoForWrite(eid)

	if entityDispatchInfo.gameid == 0 { // entity not loaded, try load now
		var gdi *gameDispatchInfo
		if gameid == 0 {
			gdi = this.chooseGame()
		} else {
			gdi = this.games[gameid]
		}

		if gdi != nil {
			entityDispatchInfo.gameid = gdi.gameid
			entityDispatchInfo.blockRPC(consts.DISPATCHER_LOAD_TIMEOUT)
			_ = gdi.dispatchPacket(pkt)
		} else {
			gwlog.Errorf("%s: handleLoadEntitySomewhere: no game", this)
		}
	} else if gameid != 0 && gameid != entityDispatchInfo.gameid {
		gwlog.Warnf("%s: try to load entity on game%d, but already created on game%d", this, gameid, entityDispatchInfo.gameid)
	}
}

func (this *DispatcherService) handleCreateEntitySomewhere(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	if consts.DEBUG_PACKETS {
		gwlog.Debugf("%s.handleCreateEntitySomewhere: dcp=%s, pkt=%s", this, dcp, pkt.Payload())
	}
	gameid := pkt.ReadUint16()
	entityid := pkt.ReadEntityID()
	var gdi *gameDispatchInfo
	if gameid == 0 {
		// choose a random game
		gdi = this.chooseGame()
	} else {
		// choose the specified game
		gdi = this.games[gameid]
	}

	if gdi != nil {
		entityDispatchInfo := this.setEntityDispatcherInfoForWrite(entityid)
		entityDispatchInfo.gameid = gdi.gameid // setup gameid of entity
		_ = gdi.dispatchPacket(pkt)
	} else {
		gwlog.Errorf("%s handleCreateEntitySomewhere: no game", this)
	}
}

func (this *DispatcherService) handleKvregRegister(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	srvid := pkt.ReadVarStr()
	srvinfo := pkt.ReadVarStr()
	force := pkt.ReadBool()

	curinfo := this.kvregRegisterMap[srvid]

	if force || curinfo == "" {
		this.kvregRegisterMap[srvid] = srvinfo
		this.broadcastToGames(pkt)
		gwlog.Infof("%s: kvreg register %s = %s, force %v, register ok", this, srvid, srvinfo, force)
	} else {
		gwlog.Infof("%s: kvreg register %s = %s, force %v, curinfo=%s, register failed", this, srvid, srvinfo, force, curinfo)
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

func (this *DispatcherService) handleCallEntityMethod(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	entityID := pkt.ReadEntityID()

	if consts.DEBUG_PACKETS {
		gwlog.Debugf("%s.handleCallEntityMethod: dcp=%s, entityID=%s", this, dcp, entityID)
	}

	entityDispatchInfo := this.entityDispatchInfos[entityID]
	if entityDispatchInfo != nil {
		_ = entityDispatchInfo.dispatchPacket(pkt)
	} else {
		gwlog.Warnf("%s: entity %s is called by other entity, but dispatch info is not found", this, entityID)
	}
}

func (this *DispatcherService) handleCallNilSpaces(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	// send the packet to all games
	exceptGameID := pkt.ReadUint16()
	this.broadcastToGamesExcept(pkt, exceptGameID)
}

func (this *DispatcherService) handleSyncPositionYawOnClients(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	gateid := pkt.ReadUint16()
	_ = this.dispatcherClientOfGate(gateid).SendPacket(pkt)
}

func (this *DispatcherService) handleSyncPositionYawFromClient(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	// This sync packet contains position-yaw of multiple entities from a gate. Cache the packet to be send before flush?
	payload := pkt.UnreadPayload()

	for i := 0; i < len(payload); i += proto.SYNC_INFO_SIZE_PER_ENTITY + common.ENTITYID_LENGTH {
		eid := common.EntityID(payload[i : i+common.ENTITYID_LENGTH]) // the first bytes of each entry is the EntityID

		entityDispatchInfo := this.entityDispatchInfos[eid]
		if entityDispatchInfo == nil {
			gwlog.Warnf("%s: entity %s is synced from client, but dispatch info is not found", this, eid)
			continue
		}

		gameid := entityDispatchInfo.gameid

		// put this sync info to the pending queue of target game
		// concat to the end of queue
		pkt := this.entitySyncInfosToGame[gameid]
		if pkt == nil {
			pkt = netutil.NewPacket()
			pkt.AppendUint16(proto.MT_SYNC_POSITION_YAW_FROM_CLIENT)
			this.entitySyncInfosToGame[gameid] = pkt
		}
		pkt.AppendBytes(payload[i : i+proto.SYNC_INFO_SIZE_PER_ENTITY+common.ENTITYID_LENGTH])
	}
}

func (this *DispatcherService) sendEntitySyncInfosToGames() {
	if len(this.entitySyncInfosToGame) == 0 {
		return
	}

	for gameid, pkt := range this.entitySyncInfosToGame {
		// send the entity sync infos to this game
		_ = this.games[gameid].dispatchPacket(pkt)
		pkt.Release()
	}
	this.entitySyncInfosToGame = map[uint16]*netutil.Packet{}
}

func (this *DispatcherService) handleCallEntityMethodFromClient(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	entityID := pkt.ReadEntityID()

	if consts.DEBUG_PACKETS {
		gwlog.Debugf("%s.handleCallEntityMethodFromClient: entityID=%s, payload=%v", this, entityID, pkt.Payload())
	}

	entityDispatchInfo := this.entityDispatchInfos[entityID]
	if entityDispatchInfo != nil {
		_ = entityDispatchInfo.dispatchPacket(pkt)
	} else {
		gwlog.Warnf("%s: entity %s is called by client, but dispatch info is not found", this, entityID)
	}
}

func (this *DispatcherService) handleDoSomethingOnSpecifiedClient(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	gid := pkt.ReadUint16()
	_ = this.dispatcherClientOfGate(gid).SendPacket(pkt)
}

func (this *DispatcherService) handleCallFilteredClientProxies(dcp *dispatcherClientProxy, pkt *netutil.Packet) {
	this.broadcastToGates(pkt)
}

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

func (this *DispatcherService) broadcastToGames(pkt *netutil.Packet) {
	for _, gdi := range this.games {
		_ = gdi.dispatchPacket(pkt)
	}
}
func (this *DispatcherService) broadcastToGamesRelease(pkt *netutil.Packet) {
	this.broadcastToGames(pkt)
	pkt.Release()
}
func (this *DispatcherService) broadcastToGamesExcept(pkt *netutil.Packet, exceptGameID uint16) {
	for gameid, gdi := range this.games {
		if gameid == exceptGameID {
			continue
		}
		_ = gdi.dispatchPacket(pkt)
	}
}

func (this *DispatcherService) broadcastToGates(pkt *netutil.Packet) {
	for gateid, dcp := range this.gates {
		if dcp != nil {
			_ = dcp.SendPacket(pkt)
		} else {
			gwlog.Errorf("Gate %d is not connected to dispatcher when broadcasting", gateid)
		}
	}
}

func (this *DispatcherService) recalcBootGames() {
	var candidates []uint16
	for gameid, gdi := range this.games {
		if !gdi.isBanBootEntity {
			candidates = append(candidates, gameid)
		}
	}
	this.bootGames = candidates
}

func (this *DispatcherService) handleGameLBCInfo(dcp *dispatcherClientProxy, packet *netutil.Packet) {
	// handle game LBC info from game
	var lbcinfo proto.GameLBCInfo
	packet.ReadData(&lbcinfo)
	gwlog.Debugf("Game %d Load Balancing Info: %+v", dcp.gameid, lbcinfo)
	lbcinfo.CPUPercent *= 1 + (rand.Float64() * 0.1) // multiply CPUPercent by a random factor 1.0 ~ 1.1
	gdi := this.games[dcp.gameid]
	gdi.lbcheapentry.update(lbcinfo)
	heap.Fix(&this.lbcheap, gdi.lbcheapentry.heapidx)
	this.lbcheap.validateHeapIndexes()
}
