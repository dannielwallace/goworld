package game_impl

import (
	"fmt"
	"github.com/dannielwallace/goworld/engine/common"
	"github.com/dannielwallace/goworld/engine/consts"
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/netutil"
)

func (gs *GameService) handleMsgClient2Gs(pkt *netutil.Packet) {
	// TODO, on gate disconnect
	//entity.OnGateDisconnected(gateid)
	// TODO, check msg len for all msg types
	if pkt.GetPayloadLen() < common.CLIENTID_LENGTH {
		panic(pkt.GetPayloadLen())
	}

	clientID := pkt.PeekClientIDFromEnd()
	payloadLen := pkt.GetPayloadLen()

	fmt.Println("[GameService-handleMsgClient2Gs], client:[%v], msgLen:[%v]", clientID, payloadLen)
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

func (gs *GameService) HandleNotifyClientConnected(clientId common.ClientID) {
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
