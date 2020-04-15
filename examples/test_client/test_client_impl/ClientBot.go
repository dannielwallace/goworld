package test_client_impl

import (
	"github.com/xiaonanln/netconnutil"
	"net"
	"sync"

	"fmt"

	"math/rand"

	"time"

	"crypto/tls"

	"github.com/dannielwallace/goworld/engine/config"
	"github.com/dannielwallace/goworld/engine/consts"
	"github.com/dannielwallace/goworld/engine/gwioutil"
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/netutil"
	"github.com/dannielwallace/goworld/engine/post"
	"github.com/dannielwallace/goworld/engine/proto"
	"github.com/xtaci/kcp-go"
	"golang.org/x/net/websocket"
)

var (
	tlsConfig = &tls.Config{
		InsecureSkipVerify: true,
	}
)

// ClientBot is  a client bot representing a game client
type ClientBot struct {
	sync.Mutex

	id int

	waiter             *sync.WaitGroup
	waitAllConnected   *sync.WaitGroup
	conn               *proto.GoWorldConnection
	logined            bool
	startedDoingThings bool
	syncPosTime        time.Time
	useKCP             bool
	useWebSocket       bool
	noEntitySync       bool
	packetQueue        chan proto.Message
	m_bQuite           bool
	m_bStrictMode      bool
}

func NewClientBot(id int, useWebSocket bool, useKCP bool, noEntitySync bool,
	waiter *sync.WaitGroup, waitAllConnected *sync.WaitGroup,
	quite bool, strictMode bool) *ClientBot {
	return &ClientBot{
		id:               id,
		waiter:           waiter,
		waitAllConnected: waitAllConnected,
		useKCP:           useKCP,
		useWebSocket:     useWebSocket,
		noEntitySync:     noEntitySync,
		packetQueue:      make(chan proto.Message),
		m_bQuite:         quite,
		m_bStrictMode:    strictMode,
	}
}

func (bot *ClientBot) String() string {
	return fmt.Sprintf("ClientBot<%d>", bot.id)
}

func (bot *ClientBot) Run(serverHost string) {
	defer bot.waiter.Done()

	gwlog.Infof("%s is running ...", bot)

	desiredGates := config.GetDeployment().DesiredGates
	// choose a random gateid
	gateid := uint16(rand.Intn(desiredGates) + 1)
	gwlog.Debugf("%s is connecting to gate %d", bot, gateid)
	cfg := config.GetGate(gateid)

	var netconn net.Conn
	var err error
	for { // retry for ever
		netconn, err = bot.connectServer(serverHost, cfg)
		if err != nil {
			Errorf("%s: connect failed: %s", bot, err)
			time.Sleep(time.Second * time.Duration(1+rand.Intn(10)))
			continue
		}
		// connected , ok
		break
	}

	gwlog.Infof("connected: %s", netconn.RemoteAddr())
	if cfg.EncryptConnection && !bot.useWebSocket {
		netconn = tls.Client(netconn, tlsConfig)
	}
	var conn netutil.Connection = netutil.NetConn{netconnutil.NewNoTempErrorConn(netconn)}
	if cfg.CompressConnection {
		conn = netconnutil.NewSnappyConn(conn)
	}
	conn = netconnutil.NewBufferedConn(conn, consts.BUFFERED_READ_BUFFSIZE, consts.BUFFERED_WRITE_BUFFSIZE)
	bot.conn = proto.NewGoWorldConnection(conn)
	defer bot.conn.Close()

	if bot.useKCP {
		gwlog.Infof("Notify KCP connected ...")
		bot.conn.SetHeartbeatFromClient()
	}

	go bot.recvLoop()
	bot.waitAllConnected.Done()

	bot.waitAllConnected.Wait()
	bot.loop()
}

func (bot *ClientBot) connectServer(serverHost string, cfg *config.GateConfig) (net.Conn, error) {
	if bot.useWebSocket {
		return bot.connectServerByWebsocket(serverHost, cfg)
	} else if bot.useKCP {
		return bot.connectServerByKCP(serverHost, cfg)
	}
	// just use tcp
	_, listenPort, err := net.SplitHostPort(cfg.ListenAddr)
	if err != nil {
		gwlog.Fatalf("can not parse host:port: %s", cfg.ListenAddr)
	}

	conn, err := netutil.ConnectTCP(net.JoinHostPort(serverHost, listenPort))
	if err == nil {
		conn.(*net.TCPConn).SetWriteBuffer(64 * 1024)
		conn.(*net.TCPConn).SetReadBuffer(64 * 1024)
	}
	return conn, err
}

func (bot *ClientBot) connectServerByKCP(serverHost string, cfg *config.GateConfig) (net.Conn, error) {
	// just use tcp
	_, listenPort, err := net.SplitHostPort(cfg.ListenAddr)
	if err != nil {
		gwlog.Fatalf("can not parse host:port: %s", cfg.ListenAddr)
	}

	serverAddr := net.JoinHostPort(serverHost, listenPort)
	conn, err := kcp.DialWithOptions(serverAddr, nil, 10, 3)
	if err != nil {
		return nil, err
	}
	conn.SetReadBuffer(64 * 1024)
	conn.SetWriteBuffer(64 * 1024)
	conn.SetNoDelay(consts.KCP_NO_DELAY, consts.KCP_INTERNAL_UPDATE_TIMER_INTERVAL, consts.KCP_ENABLE_FAST_RESEND, consts.KCP_DISABLE_CONGESTION_CONTROL)
	conn.SetStreamMode(consts.KCP_SET_STREAM_MODE)
	conn.SetWriteDelay(consts.KCP_SET_WRITE_DELAY)
	conn.SetACKNoDelay(consts.KCP_SET_ACK_NO_DELAY)
	return conn, err
}

func (bot *ClientBot) connectServerByWebsocket(serverHost string, cfg *config.GateConfig) (net.Conn, error) {
	originProto := "http"
	wsProto := "ws"
	if cfg.EncryptConnection {
		originProto = "https"
		wsProto = "wss"
	}
	_, httpPort, err := net.SplitHostPort(cfg.HTTPAddr)
	if err != nil {
		gwlog.Fatalf("can not parse host:port: %s", cfg.HTTPAddr)
	}

	origin := fmt.Sprintf("%s://%s:%s/", originProto, serverHost, httpPort)
	wsaddr := fmt.Sprintf("%s://%s:%s/ws", wsProto, serverHost, httpPort)

	if cfg.EncryptConnection {
		dialCfg, err := websocket.NewConfig(wsaddr, origin)
		if err != nil {
			return nil, err
		}
		dialCfg.TlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
		return websocket.DialConfig(dialCfg)
	} else {
		return websocket.Dial(wsaddr, "", origin)
	}
}

func (bot *ClientBot) recvLoop() {
	var msgtype proto.MsgType

	for {
		pkt, err := bot.conn.Recv(&msgtype)
		if pkt != nil {
			//fmt.Fprintf(os.Stderr, "P")
			bot.packetQueue <- proto.Message{msgtype, pkt}
		} else if err != nil && !gwioutil.IsTimeoutError(err) {
			// bad error
			Errorf("%s: client recv packet failed: %v", bot, err)
			break
		}
	}
}

func (bot *ClientBot) loop() {
	ticker := time.Tick(time.Millisecond * 100)
	for {
		select {
		case item := <-bot.packetQueue:
			//fmt.Fprintf(os.Stderr, "p")
			bot.handlePacket(item.MsgType, item.Packet)
			item.Packet.Release()
			break
		case <-ticker:
			//fmt.Fprintf(os.Stderr, "|")
			if !bot.noEntitySync {
				/*
					if bot.player != nil && bot.player.TypeName == "Avatar" {
						now := time.Now()
						if now.Sub(bot.syncPosTime) > time.Millisecond*100 {
							player := bot.player
							const moveRange = 0.01
							if rand.Float32() < 0.5 { // let the posibility of avatar moving to be 50%
								player.pos.X += entity.Coord(-moveRange + moveRange*2*rand.Float32())
								player.pos.Z += entity.Coord(-moveRange + moveRange*rand.Float32())
								//gwlog.Infof("move to %f, %f", player.pos.X, player.pos.Z)
								player.yaw = entity.Yaw(rand.Float32() * 3.14)
								bot.conn.SendSyncPositionYawFromClient(player.ID, float32(player.pos.X), float32(player.pos.Y), float32(player.pos.Z), float32(player.yaw))
							}

							bot.syncPosTime = now
						}
					}*/

			}
			bot.conn.Flush("ClientBot")
			post.Tick()
			break
		}
	}
}

func (bot *ClientBot) handlePacket(msgtype proto.MsgType, packet *netutil.Packet) {
	defer func() {
		err := recover()
		if err != nil {
			gwlog.TraceError("handle packet failed: %v", err)
		}
	}()

	bot.Lock()
	defer bot.Unlock()

	//gwlog.Infof("client handle packet: msgtype=%v, payload=%v", msgtype, packet.Payload())

	if msgtype >= proto.MT_REDIRECT_TO_GATEPROXY_MSG_TYPE_START && msgtype <= proto.MT_REDIRECT_TO_GATEPROXY_MSG_TYPE_STOP {
		_ = packet.ReadUint16()
		_ = packet.ReadClientID() // TODO: strip these two fields ? seems a little difficult, maybe later.
	}

	/*
		if msgtype == proto.MT_NOTIFY_MAP_ATTR_CHANGE_ON_CLIENT {
			entityID := packet.ReadEntityID()
			var path []interface{}
			packet.ReadData(&path)
			key := packet.ReadVarStr()
			var val interface{}
			packet.ReadData(&val)
			if !bot.m_bQuite {
				gwlog.Debugf("Entity %s Attribute %v: set %s=%v", entityID, path, key, val)
			}
			bot.applyMapAttrChange(entityID, path, key, val)
		} else {
			gwlog.Panicf("unknown msgtype: %v", msgtype)
		}*/
}

func (bot *ClientBot) username() string {
	return fmt.Sprintf("test%d", bot.id)
}

func (bot *ClientBot) password() string {
	return "123456"
}

func Errorf(fmt string, args ...interface{}) {
	if true /*main.strictMode*/ {
		gwlog.Fatalf(fmt, args...)
	} else {
		gwlog.Errorf(fmt, args...)
	}

}
