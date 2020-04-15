package game_impl

import (
	"fmt"
	"time"

	"github.com/dannielwallace/goworld/engine/async"
	"github.com/dannielwallace/goworld/engine/binutil"
	"github.com/dannielwallace/goworld/engine/common"
	"github.com/dannielwallace/goworld/engine/config"
	"github.com/dannielwallace/goworld/engine/consts"
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/gwutils"
	"github.com/dannielwallace/goworld/engine/gwvar"
	"github.com/dannielwallace/goworld/engine/netutil"
	"github.com/dannielwallace/goworld/engine/post"
	"github.com/dannielwallace/goworld/engine/proto"
	"github.com/xiaonanln/go-xnsyncutil/xnsyncutil"
	"github.com/xiaonanln/goTimer"
)

const (
	RsNotRunning = iota
	RsRunning
	RsTerminating
	RsTerminated
)

type GameService struct {
	m_config *config.GameConfig
	m_gameId uint16
	//registeredServices map[string]common.EntityIDSet

	m_packetQueue chan proto.Message
	m_runState    xnsyncutil.AtomicInt

	m_nextSyncInfosTime time.Time
	m_syncInfosInterval time.Duration
	m_GameLoopTicker    <-chan time.Time

	m_onlineGames     common.Uint16Set
	isDeploymentReady bool
}

func NewGameService(gameid uint16) *GameService {
	//cfg := m_config.GetGame(gameid)
	return &GameService{
		m_gameId:         gameid,
		m_packetQueue:    make(chan proto.Message, consts.GAME_SERVICE_PACKET_QUEUE_SIZE),
		m_GameLoopTicker: time.Tick(consts.GAME_SERVICE_TICK_INTERVAL),
		m_onlineGames:    common.Uint16Set{},
		//terminated:         xnsyncutil.NewOneTimeCond(),
		//dumpNotify:         xnsyncutil.NewOneTimeCond(),
		//dumpFinishedNotify: xnsyncutil.NewOneTimeCond(),
		//collectEntitySyncInfosRequest: make(chan struct{}),
		//collectEntitySycnInfosReply:   make(chan interface{}),
	}
}

func (gs *GameService) Run() {
	gs.m_runState.Store(RsRunning)
	binutil.PrintSupervisorTag(consts.GAME_STARTED_TAG)
	gwutils.RepeatUntilPanicless(gs.serveRoutine)
}

func (gs *GameService) GetRunState() int {
	return gs.m_runState.Load()
}

func (gs *GameService) GetOnlineGames() common.Uint16Set {
	return gs.m_onlineGames
}

func (gs *GameService) AddMsgPacket(msgType proto.MsgType, packet *netutil.Packet) {
	gs.m_packetQueue <- proto.Message{ // may block the dispatcher client routine
		MsgType: msgType,
		Packet:  packet,
	}
}

func (gs *GameService) serveRoutine() {
	cfg := config.GetGame(gs.m_gameId)
	gs.m_config = cfg
	gs.m_syncInfosInterval = time.Millisecond * time.Duration(cfg.PositionSyncIntervalMS)
	if gs.m_syncInfosInterval < consts.GAME_SERVICE_TICK_INTERVAL {
		gwlog.Warnf("%s: entity position sync interval is too small: %s, so reset to %s", gs, gs.m_syncInfosInterval, consts.GAME_SERVICE_TICK_INTERVAL)
		gs.m_syncInfosInterval = consts.GAME_SERVICE_TICK_INTERVAL
	}

	gwlog.Infof("Read game %d m_config: \n%s\n", gs.m_gameId, config.DumpPretty(cfg))

	// here begins the main loop of Game
	for {
		isTick := false
		select {
		case item := <-gs.m_packetQueue:
			msgtype, pkt := item.MsgType, item.Packet
			switch msgtype {
			case proto.MT_QUERY_SPACE_GAMEID_FOR_MIGRATE_ACK:
				gs.HandleQuerySpaceGameIDForMigrateAck(pkt)
			case proto.MT_MIGRATE_REQUEST_ACK:
				gs.HandleMigrateRequestAck(pkt)
			case proto.MT_REAL_MIGRATE:
				gs.HandleRealMigrate(pkt)
			case proto.MT_NOTIFY_CLIENT_CONNECTED:
				clientid := pkt.ReadClientID()
				gs.HandleNotifyClientConnected(clientid)
			case proto.MT_NOTIFY_CLIENT_DISCONNECTED:
				clientid := pkt.ReadClientID()
				gs.HandleNotifyClientDisconnected(clientid)
			case proto.MT_NOTIFY_GATE_DISCONNECTED:
				gateid := pkt.ReadUint16()
				gs.HandleGateDisconnected(gateid)
			case proto.MT_NOTIFY_GAME_CONNECTED:
				gs.handleNotifyGameConnected(pkt)
			case proto.MT_NOTIFY_GAME_DISCONNECTED:
				gs.handleNotifyGameDisconnected(pkt)
			case proto.MT_NOTIFY_DEPLOYMENT_READY:
				gs.handleNotifyDeploymentReady(pkt)
			case proto.MT_SET_GAME_ID_ACK:
				gs.handleSetGameIDAck(pkt)
			default:
				gwlog.TraceError("unknown msgtype: %v", msgtype)
			}

			pkt.Release()
		case <-gs.m_GameLoopTicker:
			isTick = true
			runState := gs.m_runState.Load()
			if runState == RsTerminating {
				// game is terminating, run the terminating process
				gs.doTerminate()
			}

			timer.Tick()

			//case <-gs.collectEntitySyncInfosRequest: //
			//	gs.collectEntitySycnInfosReply <- 1
		}

		// after handling packets or firing timers, check the posted functions
		post.Tick()
		if isTick {
			now := time.Now()
			if !gs.m_nextSyncInfosTime.After(now) {
				gs.m_nextSyncInfosTime = now.Add(gs.m_syncInfosInterval)
				//entity.CollectEntitySyncInfos()
			}
		}
	}
}

func (gs *GameService) waitPostsComplete() {
	gwlog.Infof("waiting for posts to complete ...")
	post.Tick() // just tick is Ok, tick will consume all posts
}

func (gs *GameService) doTerminate() {
	// wait for all posts to complete
	gwlog.Infof("Waiting for posts to complete ...")
	gs.waitPostsComplete()
	// wait for all async to clear
	gwlog.Infof("Waiting for async tasks to complete ...")
	for async.WaitClear() { // wait for all async to stop
		gs.waitPostsComplete()
	}

	// TODO, call lua function
	gwlog.Infof("All entities saved & destroyed, game service terminated.")
	gs.m_runState.Store(RsTerminated)

	for {
		time.Sleep(time.Second)
	}
}

func (gs *GameService) String() string {
	return fmt.Sprintf("GameService<%d>", gs.m_gameId)
}

func (gs *GameService) HandleGateDisconnected(gateid uint16) {
	// TODO, on gate disconnect
	//entity.OnGateDisconnected(gateid)
}

func (gs *GameService) handleNotifyGameConnected(pkt *netutil.Packet) {
	gameid := pkt.ReadUint16() // the new connected game
	if gs.m_onlineGames.Contains(gameid) {
		// should not happen
		gwlog.Errorf("%s: handle notify game connected: game%d is connected, but it was already connected", gs, gameid)
		return
	}

	gs.m_onlineGames.Add(gameid)
	gwlog.Infof("%s notify game connected: %d online games currently", gs, len(gs.m_onlineGames))
}

func (gs *GameService) handleNotifyGameDisconnected(pkt *netutil.Packet) {
	gameid := pkt.ReadUint16()

	if !gs.m_onlineGames.Contains(gameid) {
		// should not happen
		gwlog.Errorf("%s: handle notify game disconnected: game%d is disconnected, but it was not connected", gs, gameid)
		return
	}

	gs.m_onlineGames.Remove(gameid)
	gwlog.Infof("%s notify game disconnected: %d online games left", gs, len(gs.m_onlineGames))
}

func (gs *GameService) handleNotifyDeploymentReady(pkt *netutil.Packet) {
	gs.onDeploymentReady()
}

func (gs *GameService) handleSetGameIDAck(pkt *netutil.Packet) {
	_ = pkt.ReadUint16()
	//dispid := pkt.ReadUint16() // dispatcher  that sent the SET_GAME_ID_ACK
	isDeploymentReady := pkt.ReadBool()

	gameNum := int(pkt.ReadUint16())
	gs.m_onlineGames = common.Uint16Set{} // clear online games first
	for i := 0; i < gameNum; i++ {
		gameid := pkt.ReadUint16()
		gs.m_onlineGames.Add(gameid)
	}

	gwlog.Infof("%s: set game ID ack received, deployment ready: %v, %d online games",
		gs, isDeploymentReady, len(gs.m_onlineGames))
	if isDeploymentReady {
		// all games are connected
		gs.onDeploymentReady()
	}
}

func (gs *GameService) onDeploymentReady() {
	if gs.isDeploymentReady {
		// should never happen, because dispatcher never send deployment ready to a game more than once
		return
	}

	gs.isDeploymentReady = true
	gwvar.IsDeploymentReady.Set(true)
	gwlog.Infof("DEPLOYMENT IS READY!")
}

func (gs *GameService) HandleNotifyClientConnected(clientid common.ClientID) {
	// find the owner of the client, and notify new client
	//client := entity.MakeGameClient(clientid, gateid)
	//if consts.DEBUG_PACKETS {
	//	gwlog.Debugf("%s.handleNotifyClientConnected: %s", gs, client)
	//}

	// TODO, call lua function
}

func (gs *GameService) HandleNotifyClientDisconnected(clientId common.ClientID) {
	if consts.DEBUG_CLIENTS {
		gwlog.Debugf("%s.handleNotifyClientDisconnected: %s", gs, clientId)
	}
	// find the owner of the client, and notify lose client
	// TODO, call lua function
}

func (gs *GameService) HandleQuerySpaceGameIDForMigrateAck(pkt *netutil.Packet) {
	//spaceid := pkt.ReadEntityID()
	//entityid := pkt.ReadEntityID()
	//gameid := pkt.ReadUint16()
	//entity.OnQuerySpaceGameIDForMigrateAck(entityid, spaceid, gameid)
}

func (gs *GameService) HandleMigrateRequestAck(pkt *netutil.Packet) {
	//eid := pkt.ReadEntityID()
	//spaceid := pkt.ReadEntityID()
	//spaceLoc := pkt.ReadUint16()

	//if consts.DEBUG_PACKETS {
	//	gwlog.Debugf("Entity %s is migrating to space %s at game %d", eid, spaceid, spaceLoc)
	//}

	//entity.OnMigrateRequestAck(eid, spaceid, spaceLoc)
}

func (gs *GameService) HandleRealMigrate(pkt *netutil.Packet) {
	//eid := pkt.ReadEntityID()
	//_ = pkt.ReadUint16() // targetGame is not userful
	//data := pkt.ReadVarBytes()
	//entity.OnRealMigrate(eid, data)
}

func (gs *GameService) Terminate() {
	gs.m_runState.Store(RsTerminating)
}
