package main

import (
	"github.com/dannielwallace/goworld/components/dispatcher/dispatcher_impl"
	"os"
	"syscall"

	"flag"

	_ "net/http/pprof"

	"os/signal"

	"runtime/debug"

	"github.com/dannielwallace/goworld/engine/binutil"
	"github.com/dannielwallace/goworld/engine/config"
	"github.com/dannielwallace/goworld/engine/consts"
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/post"
)

var (
	dispidArg         int
	dispid            uint16
	configFile        = ""
	logLevel          string
	runInDaemonMode   bool
	sigChan           = make(chan os.Signal, 1)
	dispatcherService *dispatcher_impl.DispatcherService
)

func parseArgs() {
	flag.IntVar(&dispidArg, "dispid", 0, "set dispatcher ID")
	flag.StringVar(&configFile, "c", "", "set config file path")
	flag.StringVar(&logLevel, "log", "", "set log level, will override log level in config")
	flag.BoolVar(&runInDaemonMode, "d", false, "run in daemon mode")
	flag.Parse()
	dispid = uint16(dispidArg)
}

func setupGCPercent() {
	debug.SetGCPercent(consts.DISPATCHER_GC_PERCENT)
}

func main() {
	parseArgs()
	if runInDaemonMode {
		daemoncontext := binutil.Daemonize()
		defer daemoncontext.Release()
	}

	setupGCPercent()

	if configFile != "" {
		config.SetConfigFile(configFile)
	}

	validDispIds := config.GetDispatcherIDs()
	if dispid < validDispIds[0] || dispid > validDispIds[len(validDispIds)-1] {
		gwlog.Fatalf("dispatcher ID must be one of %v, but is %v, use -dispid to specify", config.GetDispatcherIDs(), dispid)
	}

	dispatcherConfig := config.GetDispatcher(dispid)

	if logLevel == "" {
		logLevel = dispatcherConfig.LogLevel
	}
	binutil.SetupGWLog("dispatcherService", logLevel, dispatcherConfig.LogFile, dispatcherConfig.LogStderr)
	binutil.SetupHTTPServer(dispatcherConfig.HTTPAddr, nil)

	dispatcherService = dispatcher_impl.NewDispatcherService(dispid)
	setupSignals() // call setupSignals to avoid data race on `dispatcherService`
	dispatcherService.Run()
}

func setupSignals() {
	signal.Ignore(syscall.Signal(10), syscall.Signal(12), syscall.SIGPIPE, syscall.SIGHUP)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for {
			sig := <-sigChan

			if sig == syscall.SIGINT || sig == syscall.SIGTERM {
				// interrupting, quit dispatcher
				post.Post(func() {
					dispatcherService.Terminate()
				})
			} else {
				gwlog.Infof("unexcepted signal: %s", sig)
			}
		}
	}()
}
