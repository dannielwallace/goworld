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
			msgType, pkt := item.MsgType, item.Packet
			if msgType >= proto.MT_REDIRECT_TO_GS_START && msgType <= proto.MT_REDIRECT_TO_GS_END {
				gs.handleMsgClient2Gs(pkt)
			} else {
				switch msgType {
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
					gwlog.TraceError("unknown msgType: %v", msgType)
				}
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

func (gs *GameService) onDeploymentReady() {
	if gs.isDeploymentReady {
		// should never happen, because dispatcher never send deployment ready to a game more than once
		return
	}

	gs.isDeploymentReady = true
	gwvar.IsDeploymentReady.Set(true)
	gwlog.Infof("DEPLOYMENT IS READY!")
}

func (gs *GameService) Terminate() {
	gs.m_runState.Store(RsTerminating)
}
