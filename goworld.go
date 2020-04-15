package goworld

import (
	"time"

	"github.com/dannielwallace/goworld/components/game"
	"github.com/dannielwallace/goworld/engine/common"
	"github.com/dannielwallace/goworld/engine/crontab"
	"github.com/dannielwallace/goworld/engine/kvdb"
	"github.com/dannielwallace/goworld/engine/post"
	"github.com/xiaonanln/goTimer"
)

// Run runs the server endless loop
//
// This is the main routine for the server and all entity logic,
// and this function never quit
func Run() {
	game.Run()
}

// GetGameID gets the local server ID
//
// server ID is a uint16 number starts from 1, which should be different for each servers
// server ID is also in the game config section name of goworld.ini
func GetGameID() uint16 {
	return game.GetGameID()
}

// GetKVDB gets value of key from KVDB
func GetKVDB(key string, callback kvdb.KVDBGetCallback) {
	kvdb.Get(key, callback)
}

// PutKVDB puts key-value to KVDB
func PutKVDB(key string, val string, callback kvdb.KVDBPutCallback) {
	kvdb.Put(key, val, callback)
}

// GetOrPut gets value of key from KVDB, if val not exists or is "", put key-value to KVDB.
func GetOrPutKVDB(key string, val string, callback kvdb.KVDBGetOrPutCallback) {
	kvdb.GetOrPut(key, val, callback)
}

// GetOnlineGames returns all online game IDs
func GetOnlineGames() common.Uint16Set {
	return game.GetOnlineGames()
}

// AddTimer adds a timer to be executed after specified duration
func AddCallback(d time.Duration, callback func()) {
	timer.AddCallback(d, callback)
}

// AddTimer adds a repeat timer to be executed every specified duration
func AddTimer(d time.Duration, callback func()) {
	timer.AddTimer(d, callback)
}

// Post posts a callback to be executed
// It is almost same as AddCallback(0, callback)
func Post(callback post.PostCallback) {
	post.Post(callback)
}

// RegisterCrontab a callack which will be executed when time condition is satisfied
//
// param minute: time condition satisfied on the specified minute, or every -minute if minute is negative
// param hour: time condition satisfied on the specified hour, or every -hour when hour is negative
// param day: time condition satisfied on the specified day, or every -day when day is negative
// param month: time condition satisfied on the specified month, or every -month when month is negative
// param dayofweek: time condition satisfied on the specified week day, or every -dayofweek when dayofweek is negative
// param cb: callback function to be executed when time is satisfied
func RegisterCrontab(minute, hour, day, month, dayofweek int, cb func()) {
	crontab.Register(minute, hour, day, month, dayofweek, cb)
}
