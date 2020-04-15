package common

import (
	"github.com/dannielwallace/goworld/engine/uuid"
)

// MsgType is the type of message types
type MsgType uint16

// ClientID type
type ClientID string

// GenClientID generates a new Client ID
func GenClientID() ClientID {
	return ClientID(uuid.GenUUID())
}

// IsNil returns if ClientID is nil
func (id ClientID) IsNil() bool {
	return id == ""
}

// CLIENTID_LENGTH is the length of Client IDs
const CLIENTID_LENGTH = uuid.UUID_LENGTH
