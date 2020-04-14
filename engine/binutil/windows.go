// +build windows

package binutil

import "github.com/dannielwallace/goworld/engine/gwlog"

type nopRelease int

func (_ nopRelease) Release() {

}

func Daemonize() nopRelease {
	// Windows can not daemonize
	gwlog.Warnf("can not run in daemon mode in windows, -d ignored")
	return nopRelease(0)
}
