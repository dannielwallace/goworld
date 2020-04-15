package proto

import (
	"github.com/dannielwallace/goworld/engine/common"
	"github.com/dannielwallace/goworld/engine/netutil"
)

type MsgType common.MsgType

type Message struct {
	MsgType MsgType
	Packet  *netutil.Packet
}

// 0 ~ 999, client 2 gs directly, not care the content
const (
	MT_REDIRECT_TO_GS_START = 1 + iota
	MT_REDIRECT_TO_GS_END   = 499
)

const (
	// MT_INVALID is the invalid message type
	MT_INVALID = 500 + iota
	// MT_SET_GAME_ID is a message type for game
	MT_SET_GAME_ID
	// MT_SET_GATE_ID is a message type for gate
	MT_SET_GATE_ID
	// MT_NOTIFY_CLIENT_CONNECTED is a message type for clients
	MT_NOTIFY_CLIENT_CONNECTED
	// MT_NOTIFY_CLIENT_DISCONNECTED is a message type for clients
	MT_NOTIFY_CLIENT_DISCONNECTED
	// MT_NOTIFY_ALL_GAMES_CONNECTED is a message type to notify all games connected
	MT_NOTIFY_ALL_GAMES_CONNECTED // NOT USED ANYMORE
	// MT_NOTIFY_GATE_DISCONNECTED is a message type to notify gate disconnected
	MT_NOTIFY_GATE_DISCONNECTED

	// Message types for migrating
	// MT_MIGRATE_REQUEST is a message type for entity migrations
	MT_MIGRATE_REQUEST
	// MT_REAL_MIGRATE is a message type for entity migrations
	MT_REAL_MIGRATE
	// MT_QUERY_SPACE_GAMEID_FOR_MIGRATE is a message type for entity migrations
	MT_QUERY_SPACE_GAMEID_FOR_MIGRATE
	MT_CANCEL_MIGRATE

	// MT_SET_GAME_ID_ACK is sent by dispatcher to game to ACK MT_SET_GAME_ID message
	MT_SET_GAME_ID_ACK
	// MT_NOTIFY_GAME_CONNECTED is sent by dispatcher to game to notify new game connected
	MT_NOTIFY_GAME_CONNECTED
	MT_NOTIFY_GAME_DISCONNECTED
	MT_NOTIFY_DEPLOYMENT_READY
	// MT_GAME_LBC_INFO contains game load balacing info
	MT_GAME_LBC_INFO

	MT_MISC_END = 999
)

// Alias message types
const (
	// MT_MIGRATE_REQUEST_ACK is a message type for entity migrations
	MT_MIGRATE_REQUEST_ACK                = MT_MIGRATE_REQUEST
	MT_QUERY_SPACE_GAMEID_FOR_MIGRATE_ACK = MT_QUERY_SPACE_GAMEID_FOR_MIGRATE
)

const (
	// MT_GATE_SERVICE_MSG_TYPE_START is the first message types that should be handled by GateService
	MT_GATE_SERVICE_MSG_TYPE_START = 1000 + iota
	// MT_REDIRECT_TO_GATEPROXY_MSG_TYPE_START is the first message type that should be redirected to client proxy
	MT_REDIRECT_TO_GATEPROXY_MSG_TYPE_START
	// MT_GATE_SET_CLIENTPROXY_FILTER_PROP message type
	MT_GATE_SET_CLIENTPROXY_FILTER_PROP
	// MT_GATE_CLEAR_CLIENTPROXY_FILTER_PROPS message type
	MT_GATE_CLEAR_CLIENTPROXY_FILTER_PROPS
	// MT_REDIRECT_TO_GATEPROXY_MSG_TYPE_STOP message type
	MT_REDIRECT_TO_GATEPROXY_MSG_TYPE_STOP = 1499
)

const (
	// MT_CALL_FILTERED_CLIENTS message type: messages to be processed by GateService from Dispatcher, but not redirected to clients
	MT_CALL_FILTERED_CLIENTS = 1501 + iota
	// MT_GATE_SERVICE_MSG_TYPE_STOP message type
	MT_GATE_SERVICE_MSG_TYPE_STOP = 1999
)

// Messages types that is sent directly between Gate & Client
const (
	// MT_SET_CLIENT_CLIENTID message is sent to client to set its clientid
	MT_SET_CLIENT_CLIENTID = 2001 + iota
	MT_UDP_SYNC_CONN_NOTIFY_CLIENTID
	MT_UDP_SYNC_CONN_NOTIFY_CLIENTID_ACK
	// MT_HEARTBEAT_FROM_CLIENT is sent by client to notify the gate server that the client is alive
	MT_HEARTBEAT_FROM_CLIENT
)

// Operators for calling filtered clients
type FilterClientsOpType byte

const (
	FILTER_CLIENTS_OP_EQ FilterClientsOpType = iota
	FILTER_CLIENTS_OP_NE
	FILTER_CLIENTS_OP_GT
	FILTER_CLIENTS_OP_LT
	FILTER_CLIENTS_OP_GTE
	FILTER_CLIENTS_OP_LTE
)
