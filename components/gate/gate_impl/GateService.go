package gate_impl

import (
	"fmt"
	"time"

	"net"

	"crypto/tls"

	"path"

	"github.com/dannielwallace/goworld/engine/binutil"
	"github.com/dannielwallace/goworld/engine/common"
	"github.com/dannielwallace/goworld/engine/config"
	"github.com/dannielwallace/goworld/engine/consts"
	"github.com/dannielwallace/goworld/engine/dispatchercluster"
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/gwutils"
	"github.com/dannielwallace/goworld/engine/netutil"
	"github.com/dannielwallace/goworld/engine/opmon"
	"github.com/dannielwallace/goworld/engine/post"
	"github.com/dannielwallace/goworld/engine/proto"
	"github.com/pkg/errors"
	"github.com/xiaonanln/go-xnsyncutil/xnsyncutil"
)

type clientProxyMessage struct {
	cp  *ClientProxy
	msg proto.Message
}

// GateService implements the gate service logic
type GateService struct {
	m_gateId                uint16
	m_listenAddr            string
	m_clientProxies         map[common.ClientID]*ClientProxy
	m_dispatcherPacketQueue chan proto.Message
	m_clientPacketQueue     chan clientProxyMessage

	m_filterTrees       map[string]*_FilterTree
	m_nextFlushSyncTime time.Time

	m_tlsConfig     *tls.Config
	m_clientConnTTL time.Duration

	m_terminating xnsyncutil.AtomicBool
	Terminated    *xnsyncutil.OneTimeCond
}

func NewGateService(gateId uint16) *GateService {
	return &GateService{
		m_gateId:                gateId,
		m_clientProxies:         map[common.ClientID]*ClientProxy{},
		m_dispatcherPacketQueue: make(chan proto.Message, consts.GATE_SERVICE_PACKET_QUEUE_SIZE),
		m_clientPacketQueue:     make(chan clientProxyMessage, consts.GATE_SERVICE_PACKET_QUEUE_SIZE),
		m_filterTrees:           map[string]*_FilterTree{},
		Terminated:              xnsyncutil.NewOneTimeCond(),
	}
}

func (gs *GateService) Run() {
	cfg := config.GetGate(gs.m_gateId)
	gwlog.Infof("Compress connection: %v, encrypt connection: %v", cfg.CompressConnection, cfg.EncryptConnection)

	if cfg.EncryptConnection {
		gs.setupTLSConfig(cfg)
	}

	gs.m_listenAddr = cfg.ListenAddr
	go netutil.ServeTCPForever(gs.m_listenAddr, gs)

	if cfg.ClientConnTTL > 0 {
		gs.m_clientConnTTL = time.Second * time.Duration(cfg.ClientConnTTL)
		gwlog.Infof("%s: ClientConnTTL = %s", gs, gs.m_clientConnTTL)
	}
	binutil.PrintSupervisorTag(consts.GATE_STARTED_TAG)
	gwutils.RepeatUntilPanicless(gs.mainRoutine)
}

func (gs *GateService) setupTLSConfig(cfg *config.GateConfig) {
	cfgdir := config.GetConfigDir()
	rsaCert := path.Join(cfgdir, cfg.RSACertificate)
	rsaKey := path.Join(cfgdir, cfg.RSAKey)
	cert, err := tls.LoadX509KeyPair(rsaCert, rsaKey)
	if err != nil {
		gwlog.Panic(errors.Wrap(err, "load RSA key & certificate failed"))
	}

	gs.m_tlsConfig = &tls.Config{
		//MinVersion:       tls.VersionTLS12,
		//CurvePreferences: []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		Certificates: []tls.Certificate{cert},
		//CipherSuites: []uint16{
		//	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		//	tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		//	tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
		//	tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		//},
		//PreferServerCipherSuites: true,
	}
}

func (gs *GateService) String() string {
	return fmt.Sprintf("GateService<%s>", gs.m_listenAddr)
}

// ServeTCPConnection handle TCP connections from clients
func (gs *GateService) ServeTCPConnection(conn net.Conn) {
	tcpConn := conn.(*net.TCPConn)
	_ = tcpConn.SetWriteBuffer(consts.CLIENT_PROXY_WRITE_BUFFER_SIZE)
	_ = tcpConn.SetReadBuffer(consts.CLIENT_PROXY_READ_BUFFER_SIZE)
	_ = tcpConn.SetNoDelay(consts.CLIENT_PROXY_SET_TCP_NO_DELAY)

	gs.handleClientConnection(conn, false)
}

func (gs *GateService) handleClientConnection(conn net.Conn, isWebSocket bool) {
	// this function might run in multiple threads
	if gs.m_terminating.Load() {
		// server m_terminating, not accepting more connections
		_ = conn.Close()
		return
	}

	cfg := config.GetGate(gs.m_gateId)

	if cfg.EncryptConnection && !isWebSocket {
		tlsConn := tls.Server(conn, gs.m_tlsConfig)
		conn = net.Conn(tlsConn)
	}

	cp := newClientProxy(conn, cfg)
	if consts.DEBUG_CLIENTS {
		gwlog.Debugf("%s.ServeTCPConnection: client %s connected", gs, cp)
	}

	// pass the client proxy to GateService ...
	post.Post(func() {
		gs.onNewClientProxy(cp)
	})
	cp.serve(gs)
}

func (gs *GateService) checkClientHeartbeats() {
	if gs.m_clientConnTTL < time.Second {
		return
	}

	now := time.Now()
	for _, cp := range gs.m_clientProxies { // close all connected clients when m_terminating
		if cp.IsClosed() || cp.IsTry2Close() {
			continue
		}
		shouldExpireTime := cp.m_heartbeatTime.Add(gs.m_clientConnTTL)
		if shouldExpireTime.Before(now) {
			// 10 seconds no heartbeat, close it...
			gwlog.Infof("Connection %s timeout ...", cp)
			cp.Try2Close()
		}
	}
}

func (gs *GateService) onNewClientProxy(cp *ClientProxy) {
	gs.m_clientProxies[cp.m_clientId] = cp
	_ = dispatchercluster.SelectByClientID(cp.m_clientId).SendNotifyClientConnected(cp.m_clientId)
}

func (gs *GateService) onClientProxyClose(cp *ClientProxy) {
	delete(gs.m_clientProxies, cp.m_clientId)

	for key, val := range cp.m_mapFilterProps {
		ft := gs.m_filterTrees[key]
		if ft != nil {
			if consts.DEBUG_FILTER_PROP {
				gwlog.Debugf("DROP CLIENT %s FILTER PROP: %s = %s", cp, key, val)
			}
			ft.Remove(cp, val)
		}
	}

	_ = dispatchercluster.SelectByClientID(cp.m_clientId).SendNotifyClientDisconnected(cp.m_clientId)
	if consts.DEBUG_CLIENTS {
		gwlog.Debugf("%s.onClientProxyClose: client %s disconnected", gs, cp)
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// HANDLE CLIENT MSG BEGIN
// HandleDispatcherClientPacket handles packets received by dispatcher client
func (gs *GateService) handleClientProxyPacket(cp *ClientProxy, msgType proto.MsgType, pkt *netutil.Packet) {
	cp.m_heartbeatTime = time.Now()

	if msgType >= proto.MT_REDIRECT_TO_GS_START && msgType <= proto.MT_REDIRECT_TO_GS_END {
		gs.handleClientMsgDirect2GS(cp, pkt)
	} else {
		gwlog.Errorf("unknown message type from client:[%v], msgType:[%d], try 2 close this connection", cp.m_clientId, msgType)
		cp.Try2Close()
	}
}

func (gs *GateService) handleClientMsgDirect2GS(cp *ClientProxy, pkt *netutil.Packet) {
	pkt.AppendClientID(cp.m_clientId)

	dispId := dispatchercluster.ClientIDToDispatcherID(cp.m_clientId)
	_ = dispatchercluster.SelectByDispatcherID(dispId).SendPacket(pkt)
}

// HANDLE CLIENT MSG END
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// HANDLE DISPATCHER MSG BEGIN
func (gs *GateService) handleDispatcherClientPacket(msgtype proto.MsgType, packet *netutil.Packet) {
	if consts.DEBUG_PACKETS {
		gwlog.Debugf("%s.handleDispatcherClientPacket: msgtype=%v, packet(%d)=%v", gs, msgtype, packet.GetPayloadLen(), packet.Payload())
	}

	if msgtype >= proto.MT_REDIRECT_TO_GATEPROXY_MSG_TYPE_START && msgtype <= proto.MT_REDIRECT_TO_GATEPROXY_MSG_TYPE_STOP {
		_ = packet.ReadUint16() // gid
		clientId := packet.ReadClientID()
		cp := gs.m_clientProxies[clientId]
		if cp == nil {
			gwlog.Errorf("[GateService-handleDispatcherClientPacket] %s: un exist client proxy:[%v] msgType:[%d]", gs, clientId, msgtype)
			return
		}

		if msgtype == proto.MT_GATE_SET_CLIENTPROXY_FILTER_PROP {
			gs.handleSetClientFilterProp(cp, packet)
		} else if msgtype == proto.MT_GATE_CLEAR_CLIENTPROXY_FILTER_PROPS {
			gs.handleClearClientFilterProps(cp, packet)
		} else {
			// message types that should be redirected to client proxy
			_ = cp.SendPacket(packet)
		}
	} else if msgtype == proto.MT_CALL_FILTERED_CLIENTS {
		gs.handleCallFilteredClientProxies(packet)
	} else {
		gwlog.Errorf("%s: unknown msg type: %d", gs, msgtype)
	}
}

func (gs *GateService) handleSetClientFilterProp(clientproxy *ClientProxy, packet *netutil.Packet) {
	gwlog.Debugf("%s.handleSetClientFilterProp: clientproxy=%s", gs, clientproxy)
	key := packet.ReadVarStr()
	val := packet.ReadVarStr()

	ft, ok := gs.m_filterTrees[key]
	if !ok {
		ft = newFilterTree()
		gs.m_filterTrees[key] = ft
	}

	oldVal, ok := clientproxy.m_mapFilterProps[key]
	if ok {
		if consts.DEBUG_FILTER_PROP {
			gwlog.Debugf("REMOVE CLIENT %s FILTER PROP: %s = %s", clientproxy, key, val)
		}
		ft.Remove(clientproxy, oldVal)
	}
	clientproxy.m_mapFilterProps[key] = val
	ft.Insert(clientproxy, val)

	if consts.DEBUG_FILTER_PROP {
		gwlog.Debugf("SET CLIENT %s FILTER PROP: %s = %s", clientproxy, key, val)
	}
}

func (gs *GateService) handleClearClientFilterProps(clientproxy *ClientProxy, packet *netutil.Packet) {
	gwlog.Debugf("%s.handleClearClientFilterProps: clientproxy=%s", gs, clientproxy)

	for key, val := range clientproxy.m_mapFilterProps {
		ft, ok := gs.m_filterTrees[key]
		if !ok {
			continue
		}
		ft.Remove(clientproxy, val)
	}

	if consts.DEBUG_FILTER_PROP {
		gwlog.Debugf("CLEAR CLIENT %s FILTER PROPS", clientproxy)
	}
}

func (gs *GateService) handleCallFilteredClientProxies(packet *netutil.Packet) {
	op := proto.FilterClientsOpType(packet.ReadOneByte())
	key := packet.ReadVarStr()
	val := packet.ReadVarStr()

	if key == "" {
		// empty key meaning calling all clients
		for _, cp := range gs.m_clientProxies {
			_ = cp.SendPacket(packet)
		}
		return
	}

	ft := gs.m_filterTrees[key]
	if ft != nil {
		ft.Visit(op, val, func(cp *ClientProxy) {
			//// visit all clientids and
			_ = cp.SendPacket(packet)
		})
	} else {
		gwlog.Errorf("clients are not filtered by key %s", key)
	}

}

// HANDLE CLIENT MSG END
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (gs *GateService) AddClientPacket(cp *ClientProxy, msgType proto.MsgType, packet *netutil.Packet) {
	gs.m_clientPacketQueue <- clientProxyMessage{cp, proto.Message{msgType, packet}}
}

func (gs *GateService) AddDispatcherPacket(msgType proto.MsgType, packet *netutil.Packet) {
	gs.m_dispatcherPacketQueue <- proto.Message{msgType, packet}
}

func (gs *GateService) mainRoutine() {
	heartbeatCheckTicker := time.NewTicker(time.Second)
	for {
		select {
		case item := <-gs.m_clientPacketQueue:
			op := opmon.StartOperation("GateServiceHandlePacket")
			gs.handleClientProxyPacket(item.cp, item.msg.MsgType, item.msg.Packet)
			op.Finish(time.Millisecond * 100)
			item.msg.Packet.Release()
		case item := <-gs.m_dispatcherPacketQueue:
			op := opmon.StartOperation("GateServiceHandlePacket")
			gs.handleDispatcherClientPacket(item.MsgType, item.Packet)
			op.Finish(time.Millisecond * 100)
			item.Packet.Release()
			break
		case <-heartbeatCheckTicker.C:
			gs.checkClientHeartbeats()
			break
		}
		post.Tick()
	}
}

func (gs *GateService) Terminate() {
	gs.m_terminating.Store(true)

	for _, cp := range gs.m_clientProxies { // close all connected clients when m_terminating
		_ = cp.Close()
	}

	gs.Terminated.Signal()
}
