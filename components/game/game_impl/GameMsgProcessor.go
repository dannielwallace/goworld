package game_impl

import (
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/netutil"
	"github.com/dannielwallace/goworld/engine/proto"
)

type GameMsgProcessor struct {
	m_gameService *GameService
}

func NewGameMsgProcessor(gameService *GameService) *GameMsgProcessor {
	return &GameMsgProcessor{
		m_gameService: gameService,
	}
}

func (gmp *GameMsgProcessor) HandleDispatcherClientPacket(msgType proto.MsgType, packet *netutil.Packet) {
	// may block the dispatcher client routine
	gmp.m_gameService.AddMsgPacket(msgType, packet)
}

func (delegate *GameMsgProcessor) HandleDispatcherClientDisconnect() {
	gwlog.Errorf("Disconnected from dispatcher, try reconnecting ...")
}
