package dispatcher_impl

import (
	"fmt"
	"net"

	"time"

	"os"

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

func (ds *DispatcherService) AddMsgPacket(dcp *dispatcherClientProxy, msgType proto.MsgType, pkt *netutil.Packet) {
	ds.m_msgQueue <- dispatcherMessage{dcp, proto.Message{msgType, pkt}}
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
			} else if msgtype >= proto.MT_REDIRECT_TO_GS_START && msgtype <= proto.MT_REDIRECT_TO_GS_END {
				ds.handleMsgClient2Gs(dcp, pkt)
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

func (ds *DispatcherService) recalcBootGames() {
	var candidates []uint16
	for gameid, gdi := range ds.m_games {
		if !gdi.isBanBootEntity {
			candidates = append(candidates, gameid)
		}
	}
	ds.m_bootGames = candidates
}
