package game_impl

import (
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/netutil"
	"github.com/dannielwallace/goworld/engine/proto"
)

type GameDispacherClient struct {
	m_gameService *GameService
}

func NewGameMsgProcessor(gameService *GameService) *GameDispacherClient {
	return &GameDispacherClient{
		m_gameService: gameService,
	}
}

func (gmp *GameDispacherClient) HandleDispatcherClientPacket(msgType proto.MsgType, packet *netutil.Packet) {
	// may block the dispatcher client routine
	gmp.m_gameService.AddMsgPacket(msgType, packet)
}

func (delegate *GameDispacherClient) HandleDispatcherClientDisconnect() {
	gwlog.Errorf("Disconnected from dispatcher, try reconnecting ...")
}
