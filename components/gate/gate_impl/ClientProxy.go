package gate_impl

import (
	"fmt"
	"github.com/xiaonanln/netconnutil"
	"net"
	"time"

	"github.com/dannielwallace/goworld/engine/common"
	"github.com/dannielwallace/goworld/engine/config"
	"github.com/dannielwallace/goworld/engine/consts"
	"github.com/dannielwallace/goworld/engine/gwioutil"
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/netutil"
	"github.com/dannielwallace/goworld/engine/post"
	"github.com/dannielwallace/goworld/engine/proto"
)

// ClientProxy is a game client connections managed by gate
type ClientProxy struct {
	*proto.GoWorldConnection
	m_clientId       common.ClientID
	m_mapFilterProps map[string]string
	m_heartbeatTime  time.Time
}

func newClientProxy(_conn net.Conn, cfg *config.GateConfig) *ClientProxy {
	_conn = netconnutil.NewNoTempErrorConn(_conn)
	var conn netutil.Connection = netutil.NetConn{_conn}
	if cfg.CompressConnection {
		conn = netconnutil.NewSnappyConn(conn)
	}
	conn = netconnutil.NewBufferedConn(conn, consts.BUFFERED_READ_BUFFSIZE, consts.BUFFERED_WRITE_BUFFSIZE)
	gwc := proto.NewGoWorldConnection(conn)
	return &ClientProxy{
		GoWorldConnection: gwc,
		m_clientId:        common.GenClientID(), // each client has its unique m_clientId
		m_mapFilterProps:  map[string]string{},
	}
}

func (cp *ClientProxy) String() string {
	return fmt.Sprintf("ClientProxy<%s@%s>", cp.m_clientId, cp.RemoteAddr())
}

func (cp *ClientProxy) serve(gs *GateService) {
	defer func() {
		_ = cp.Close()
		// tell the gate service that this client is down
		post.Post(func() {
			gs.onClientProxyClose(cp)
		})
		if err := recover(); err != nil && !netutil.IsConnectionError(err.(error)) {
			gwlog.TraceError("%s error: %s", cp, err.(error))
		} else {
			gwlog.Debugf("%s disconnected", cp)
		}
	}()

	cp.SetAutoFlush(consts.CLIENT_PROXY_WRITE_FLUSH_INTERVAL)
	//cp.SendSetClientClientID(cp.cp) // set the cp on the client side

	for {
		if cp.IsTry2Close() {
			cp.Flush("try 2 close")
			break
		}

		var msgtype proto.MsgType
		pkt, err := cp.Recv(&msgtype)
		if pkt != nil {
			gs.AddClientPacket(cp, msgtype, pkt)
		} else if err != nil && !gwioutil.IsTimeoutError(err) {
			if netutil.IsConnectionError(err) {
				break
			} else {
				panic(err)
			}
		}
	}
}
