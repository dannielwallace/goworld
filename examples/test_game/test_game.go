package main

import (
	"github.com/dannielwallace/goworld/examples/test_game/test_game_impl"
	"time"

	"github.com/dannielwallace/goworld"
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/ext/pubsub"
	"github.com/xiaonanln/goTimer"
)

var (
	_SERVICE_NAMES = []string{
		"OnlineService",
		"SpaceService",
		"MailService",
		pubsub.ServiceName,
	}
)

func init() {

}

func main() {
	goworld.RegisterSpace(&test_game_impl.MySpace{
		GameReadyCallback: checkServerStarted,
	}) // Register the space type

	// Register each entity types
	goworld.RegisterEntity("Account", &test_game_impl.Account{})
	goworld.RegisterEntity("AOITester", &test_game_impl.AOITester{})
	goworld.RegisterService("OnlineService", &test_game_impl.OnlineService{}, 3)
	goworld.RegisterService("SpaceService", &test_game_impl.SpaceService{}, 3)
	// todo: implement sharding for MailService. Currently, MailService only allows 1 shard
	goworld.RegisterService("MailService", &test_game_impl.MailService{}, 1)

	pubsub.RegisterService(3)

	// Register Monster type and define attributes
	goworld.RegisterEntity("Monster", &test_game_impl.Monster{})
	// Register Avatar type and define attributes
	goworld.RegisterEntity("Avatar", &test_game_impl.Avatar{})

	// Run the game server
	goworld.Run()
}

func checkServerStarted() {
	ok := isAllServicesReady()
	gwlog.Infof("checkServerStarted: %v", ok)
	if ok {
		onAllServicesReady()
	} else {
		timer.AddCallback(time.Millisecond*1000, checkServerStarted)
	}
}

func isAllServicesReady() bool {
	for _, serviceName := range _SERVICE_NAMES {
		if !goworld.CheckServiceEntitiesReady(serviceName) {
			gwlog.Infof("%s entities are not ready ...", serviceName)
			return false
		}
	}
	return true
}

func onAllServicesReady() {
	gwlog.Infof("ALL SERVICES ARE READY!!!")
	goworld.CallNilSpaces("TestCallNilSpaces", 1, "abc", true, 2.3)
}
