package dispatcher_impl

import (
	"github.com/xiaonanln/netconnutil"
	"net"

	"fmt"

	"github.com/dannielwallace/goworld/engine/consts"
	"github.com/dannielwallace/goworld/engine/gwioutil"
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/netutil"
	"github.com/dannielwallace/goworld/engine/post"
	"github.com/dannielwallace/goworld/engine/proto"
)

type dispatcherClientProxy struct {
	*proto.GoWorldConnection
	m_dispacherSrv *DispatcherService
	gameid         uint16
	gateid         uint16
}

func newDispatcherClientProxy(owner *DispatcherService, conn net.Conn) *dispatcherClientProxy {
	conn = netconnutil.NewNoTempErrorConn(conn)
	gwc := proto.NewGoWorldConnection(netconnutil.NewBufferedConn(conn, consts.BUFFERED_READ_BUFFSIZE, consts.BUFFERED_WRITE_BUFFSIZE))

	dcp := &dispatcherClientProxy{
		GoWorldConnection: gwc,
		m_dispacherSrv:    owner,
	}
	dcp.SetAutoFlush(consts.DISPATCHER_CLIENT_PROXY_WRITE_FLUSH_INTERVAL)
	return dcp
}

func (dcp *dispatcherClientProxy) serve() {
	// Serve the dispatcher client from server / gate
	defer func() {
		dcp.Close()
		post.Post(func() {
			dcp.m_dispacherSrv.handleDispatcherClientDisconnect(dcp)
		})
		err := recover()
		if err != nil && !netutil.IsConnectionError(err) {
			gwlog.TraceError("Client %s paniced with error: %v", dcp, err)
		}
	}()

	gwlog.Infof("New dispatcher client: %s", dcp)
	for {
		var msgType proto.MsgType
		pkt, err := dcp.Recv(&msgType)

		if err != nil {
			if gwioutil.IsTimeoutError(err) {
				continue
			} else if netutil.IsConnectionError(err) {
				break
			}

			gwlog.Panic(err)
		}

		//
		//if consts.DEBUG_PACKETS {
		//	gwlog.Debugf("%s.RecvPacket: msgtype=%v, payload=%v", dcp, msgtype, pkt.Payload())
		//}

		// pass the packet to the dispatcher service
		dcp.m_dispacherSrv.AddMsgPacket(dcp, msgType, pkt)
	}
}

func (dcp *dispatcherClientProxy) String() string {
	if dcp.gameid > 0 {
		return fmt.Sprintf("dispatcherClientProxy<game%d|%s>", dcp.gameid, dcp.RemoteAddr())
	} else if dcp.gateid > 0 {
		return fmt.Sprintf("dispatcherClientProxy<gate%d|%s>", dcp.gateid, dcp.RemoteAddr())
	} else {
		return fmt.Sprintf("dispatcherClientProxy<%s>", dcp.RemoteAddr())
	}
}
