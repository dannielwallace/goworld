package proto

import (
	"net"

	"time"

	"github.com/dannielwallace/goworld/engine/common"
	"github.com/dannielwallace/goworld/engine/consts"
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/netutil"
	"github.com/xiaonanln/go-xnsyncutil/xnsyncutil"
)

// GoWorldConnection is the network protocol implementation of GoWorld components (dispatcher, gate, game)
type GoWorldConnection struct {
	packetConn   *netutil.PacketConnection
	closed       xnsyncutil.AtomicBool
	m_bTry2Close xnsyncutil.AtomicBool
	autoFlushing bool
}

// NewGoWorldConnection creates a GoWorldConnection using network connection
func NewGoWorldConnection(conn netutil.Connection) *GoWorldConnection {
	return &GoWorldConnection{
		packetConn: netutil.NewPacketConnection(conn),
	}
}

// SendSetGameID sends MT_SET_GAME_ID message
func (gwc *GoWorldConnection) SendSetGameID(id uint16, isReconnect bool, isRestore bool, isBanBootEntity bool) error {
	packet := gwc.packetConn.NewPacket()
	packet.AppendUint16(MT_SET_GAME_ID)
	packet.AppendUint16(id)
	packet.AppendBool(isReconnect)
	packet.AppendBool(isRestore)
	packet.AppendBool(isBanBootEntity)
	// put all entity IDs to the packet

	return gwc.SendPacketRelease(packet)
}

// SendSetGateID sends MT_SET_GATE_ID message
func (gwc *GoWorldConnection) SendSetGateID(id uint16) error {
	packet := gwc.packetConn.NewPacket()
	packet.AppendUint16(MT_SET_GATE_ID)
	packet.AppendUint16(id)
	return gwc.SendPacketRelease(packet)
}

// SendNotifyClientConnected sends MT_NOTIFY_CLIENT_CONNECTED message
func (gwc *GoWorldConnection) SendNotifyClientConnected(id common.ClientID) error {
	packet := gwc.packetConn.NewPacket()
	packet.AppendUint16(MT_NOTIFY_CLIENT_CONNECTED)
	packet.AppendClientID(id)
	return gwc.SendPacketRelease(packet)
}

// SendNotifyClientDisconnected sends MT_NOTIFY_CLIENT_DISCONNECTED message
func (gwc *GoWorldConnection) SendNotifyClientDisconnected(id common.ClientID) error {
	packet := gwc.packetConn.NewPacket()
	packet.AppendUint16(MT_NOTIFY_CLIENT_DISCONNECTED)
	packet.AppendClientID(id)
	return gwc.SendPacketRelease(packet)
}

//func (gwc *GoWorldConnection) SendSetClientClientID(clientid common.ClientID) error {
//	packet := gwc.packetConn.NewPacket()
//	packet.AppendUint16(MT_SET_CLIENT_CLIENTID)
//	packet.AppendClientID(clientid)
//	return gwc.SendPacketRelease(packet)
//}

func (gwc *GoWorldConnection) SetHeartbeatFromClient() error {
	packet := gwc.packetConn.NewPacket()
	packet.AppendUint16(MT_HEARTBEAT_FROM_CLIENT)
	return gwc.SendPacketRelease(packet)

}

// SendSetClientFilterProp sends MT_GATE_SET_CLIENTPROXY_FILTER_PROP message
func (gwc *GoWorldConnection) SendSetClientFilterProp(gateid uint16, clientid common.ClientID, key, val string) (err error) {
	packet := gwc.packetConn.NewPacket()
	packet.AppendUint16(MT_GATE_SET_CLIENTPROXY_FILTER_PROP)
	packet.AppendUint16(gateid)
	packet.AppendClientID(clientid)
	packet.AppendVarStr(key)
	packet.AppendVarStr(val)
	return gwc.SendPacketRelease(packet)
}

// SendClearClientFilterProp sends MT_GATE_CLEAR_CLIENTPROXY_FILTER_PROPS message
func (gwc *GoWorldConnection) SendClearClientFilterProp(gateid uint16, clientid common.ClientID) (err error) {
	packet := gwc.packetConn.NewPacket()
	packet.AppendUint16(MT_GATE_CLEAR_CLIENTPROXY_FILTER_PROPS)
	packet.AppendUint16(gateid)
	packet.AppendClientID(clientid)
	return gwc.SendPacketRelease(packet)
}

// SendCallFilterClientProxies sends MT_CALL_FILTERED_CLIENTS message
func AllocCallFilterClientProxiesPacket(op FilterClientsOpType, key, val string, method string) *netutil.Packet {
	packet := netutil.NewPacket()
	packet.AppendUint16(MT_CALL_FILTERED_CLIENTS)
	packet.AppendByte(byte(op))
	packet.AppendVarStr(key)
	packet.AppendVarStr(val)
	packet.AppendVarStr(method)
	return packet
}

func AllocGameLBCInfoPacket(lbcCpuUsageInPercent uint16) *netutil.Packet {
	packet := netutil.NewPacket()
	packet.AppendUint16(MT_GAME_LBC_INFO)
	packet.AppendUint16(lbcCpuUsageInPercent)
	return packet
}

/* TODO
// SendQuerySpaceGameIDForMigrate sends MT_QUERY_SPACE_GAMEID_FOR_MIGRATE message
func (gwc *GoWorldConnection) SendQuerySpaceGameIDForMigrate(spaceid common.EntityID, entityid common.EntityID) error {
	packet := gwc.packetConn.NewPacket()
	packet.AppendUint16(MT_QUERY_SPACE_GAMEID_FOR_MIGRATE)
	packet.AppendEntityID(spaceid)
	packet.AppendEntityID(entityid)
	return gwc.SendPacketRelease(packet)
}

// SendMigrateRequest sends MT_MIGRATE_REQUEST message
func (gwc *GoWorldConnection) SendMigrateRequest(entityID common.EntityID, spaceID common.EntityID, spaceGameID uint16) error {
	packet := gwc.packetConn.NewPacket()
	packet.AppendUint16(MT_MIGRATE_REQUEST)
	packet.AppendEntityID(entityID)
	packet.AppendEntityID(spaceID)
	packet.AppendUint16(spaceGameID)
	return gwc.SendPacketRelease(packet)
}

// SendCancelMigrate sends MT_CANCEL_MIGRATE message to dispatcher to unblock the entity
func (gwc *GoWorldConnection) SendCancelMigrate(entityid common.EntityID) error {
	packet := gwc.packetConn.NewPacket()
	packet.AppendUint16(MT_CANCEL_MIGRATE)
	packet.AppendEntityID(entityid)
	return gwc.SendPacketRelease(packet)
}

// SendRealMigrate sends MT_REAL_MIGRATE message
func (gwc *GoWorldConnection) SendRealMigrate(eid common.EntityID, targetGame uint16, data []byte) error {
	packet := gwc.packetConn.NewPacket()
	packet.AppendUint16(MT_REAL_MIGRATE)
	packet.AppendEntityID(eid)
	packet.AppendUint16(targetGame)
	packet.AppendVarBytes(data)
	return gwc.SendPacketRelease(packet)
}*/

func MakeNotifyGameConnectedPacket(gameid uint16) *netutil.Packet {
	pkt := netutil.NewPacket()
	pkt.AppendUint16(MT_NOTIFY_GAME_CONNECTED)
	pkt.AppendUint16(gameid)
	return pkt
}

func MakeNotifyGameDisconnectedPacket(gameid uint16) *netutil.Packet {
	pkt := netutil.NewPacket()
	pkt.AppendUint16(MT_NOTIFY_GAME_DISCONNECTED)
	pkt.AppendUint16(gameid)
	return pkt
}

func MakeNotifyDeploymentReadyPacket() *netutil.Packet {
	pkt := netutil.NewPacket()
	pkt.AppendUint16(MT_NOTIFY_DEPLOYMENT_READY)
	return pkt
}

func (gwc *GoWorldConnection) SendSetGameIDAck(dispid uint16, isDeploymentReady bool, connectedGameIDs []uint16) error {
	pkt := netutil.NewPacket()
	pkt.AppendUint16(MT_SET_GAME_ID_ACK)
	pkt.AppendUint16(dispid)

	pkt.AppendBool(isDeploymentReady)

	pkt.AppendUint16(uint16(len(connectedGameIDs)))
	for _, gameid := range connectedGameIDs {
		pkt.AppendUint16(gameid)
	}
	return gwc.SendPacketRelease(pkt)
}

// SendPacket send a packet to remote
func (gwc *GoWorldConnection) SendPacket(packet *netutil.Packet) error {
	return gwc.packetConn.SendPacket(packet)
}

// SendPacketRelease send a packet to remote and then release the packet
func (gwc *GoWorldConnection) SendPacketRelease(packet *netutil.Packet) error {
	err := gwc.packetConn.SendPacket(packet)
	packet.Release()
	return err
}

// Flush connection writes
func (gwc *GoWorldConnection) Flush(reason string) error {
	return gwc.packetConn.Flush(reason)
}

// SetAutoFlush starts a goroutine to flush connection writes at some specified interval
func (gwc *GoWorldConnection) SetAutoFlush(interval time.Duration) {
	if gwc.autoFlushing {
		gwlog.Panicf("%s.SetAutoFlush: already auto flushing!", gwc)
	}
	gwc.autoFlushing = true
	go func() {
		//defer gwlog.Debugf("%s: auto flush routine quited", gwc)
		for !gwc.IsClosed() {
			time.Sleep(interval)
			err := gwc.Flush("AutoFlush")
			if err != nil {
				break
			}
		}
	}()
}

// Recv receives the next packet and retrive the message type
func (gwc *GoWorldConnection) Recv(msgtype *MsgType) (*netutil.Packet, error) {
	pkt, err := gwc.packetConn.RecvPacket()
	if err != nil {
		return nil, err
	}

	*msgtype = MsgType(pkt.ReadUint16())
	if consts.DEBUG_PACKETS {
		gwlog.Infof("%s: Recv msgtype=%v, payload size=%d", gwc, *msgtype, pkt.GetPayloadLen())
	}
	return pkt, nil
}

// SetRecvDeadline set receive deadline
func (gwc *GoWorldConnection) SetRecvDeadline(deadline time.Time) error {
	return gwc.packetConn.SetRecvDeadline(deadline)
}

// Try to close this connection
func (gwc *GoWorldConnection) Try2Close() {
	if gwc.IsClosed() {
		return
	}
	gwc.m_bTry2Close.Store(true)
}

// Is trying to close the connection
func (gwc *GoWorldConnection) IsTry2Close() bool {
	return gwc.m_bTry2Close.Load()
}

// Close this connection
func (gwc *GoWorldConnection) Close() error {
	gwc.closed.Store(true)
	return gwc.packetConn.Close()
}

// IsClosed returns if the connection is closed
func (gwc *GoWorldConnection) IsClosed() bool {
	return gwc.closed.Load()
}

// RemoteAddr returns the remote address
func (gwc *GoWorldConnection) RemoteAddr() net.Addr {
	return gwc.packetConn.RemoteAddr()
}

// LocalAddr returns the local address
func (gwc *GoWorldConnection) LocalAddr() net.Addr {
	return gwc.packetConn.LocalAddr()
}

func (gwc *GoWorldConnection) String() string {
	return gwc.packetConn.String()
}
