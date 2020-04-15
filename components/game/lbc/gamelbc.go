package gamelbc

import (
	"os"

	"context"

	"time"

	"github.com/dannielwallace/goworld/engine/dispatchercluster"
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/gwutils"
	"github.com/shirou/gopsutil/process"
)

func Initialize(ctx context.Context, collectInterval time.Duration) {
	pid := os.Getpid()
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		gwlog.Fatalf("can not find game process: pid = %v", pid)
	}
	gwlog.Infof("gamelbc: found game process: %s", p)

	go gwutils.RepeatUntilPanicless(func() {
		for {
			time.Sleep(collectInterval)
			pcnt, err := p.CPUPercentWithContext(ctx)
			if err != nil {
				gwlog.Panicf("gamelbc: get process cpu percent failed: %s", err)
			}

			uPCnt := uint16(pcnt)
			gwlog.Debugf("gamelbc: cpu percent is %.3f%%", pcnt)
			dispatchercluster.SendGameLBCInfo(uPCnt)
		}
	})
}
