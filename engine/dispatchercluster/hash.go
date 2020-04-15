package dispatchercluster

import (
	"github.com/dannielwallace/goworld/engine/common"
)

func hashClientID(id common.ClientID) int {
	// hash EntityID to dispatcher shard index: use least 2 bytes
	b1 := id[14]
	b2 := id[15]
	return int(b1)*256 + int(b2)
}

func hashGateID(gateid uint16) int {
	return int(gateid - 1)
}

func hashString(s string) int {
	return int(common.HashString(s))
}

func hashSrvID(sn string) int {
	return hashString(sn)
}
