package test_game_impl

import (
	"github.com/dannielwallace/goworld"
	"github.com/dannielwallace/goworld/engine/entity"
)

// AOITester type
type AOITester struct {
	goworld.Entity // Entity type should always inherit entity.Entity
}

func (m *AOITester) DescribeEntityType(desc *entity.EntityTypeDesc) {
	desc.SetUseAOI(true, 100)
	desc.DefineAttr("name", "AllClients")
}
