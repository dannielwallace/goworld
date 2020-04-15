package main

import (
	"flag"
	"github.com/dannielwallace/goworld/components/gate/gate_impl"

	"math/rand"
	"time"

	"os"

	_ "net/http/pprof"

	"runtime"

	"os/signal"

	"syscall"

	"fmt"

	"github.com/dannielwallace/goworld/engine/binutil"
	"github.com/dannielwallace/goworld/engine/config"
	"github.com/dannielwallace/goworld/engine/dispatchercluster"
	"github.com/dannielwallace/goworld/engine/dispatchercluster/dispatcherclient"
	"github.com/dannielwallace/goworld/engine/gwlog"
	"github.com/dannielwallace/goworld/engine/post"
)

var (
	args struct {
		gateid          uint16
		configFile      string
		logLevel        string
		runInDaemonMode bool
		//listenAddr      string
	}
	gateService *gate_impl.GateService
	signalChan  = make(chan os.Signal, 1)
)

func parseArgs() {
	var gateIdArg int
	flag.IntVar(&gateIdArg, "gid", 0, "set gateid")
	flag.StringVar(&args.configFile, "c", "", "set config file path")
	flag.StringVar(&args.logLevel, "log", "", "set log level, will override log level in config")
	flag.BoolVar(&args.runInDaemonMode, "d", false, "run in daemon mode")
	//flag.StringVar(&args.listenAddr, "listen-addr", "", "set listen address for gate, overriding listen_addr in config file")
	flag.Parse()
	args.gateid = uint16(gateIdArg)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	parseArgs()

	if args.runInDaemonMode {
		daemonCtx := binutil.Daemonize()
		defer daemonCtx.Release()
	}

	if args.configFile != "" {
		config.SetConfigFile(args.configFile)
	}

	if args.gateid <= 0 {
		gwlog.Errorf("gateid %d is not valid, should be positive", args.gateid)
		os.Exit(1)
	}

	gateConfig := config.GetGate(args.gateid)
	if gateConfig.GoMaxProcs > 0 {
		gwlog.Infof("SET GOMAXPROCS = %d", gateConfig.GoMaxProcs)
		runtime.GOMAXPROCS(gateConfig.GoMaxProcs)
	}
	logLevel := args.logLevel
	if logLevel == "" {
		logLevel = gateConfig.LogLevel
	}
	binutil.SetupGWLog(fmt.Sprintf("gate%d", args.gateid), logLevel, gateConfig.LogFile, gateConfig.LogStderr)
	binutil.SetupHTTPServer(gateConfig.HTTPAddr, nil)

	gateService = gate_impl.NewGateService(args.gateid)
	dispatcherCliInst := gate_impl.NewDispatcherClientDelegate(gateService, signalChan)
	dispatchercluster.Initialize(args.gateid, dispatcherclient.GateDispatcherClientType, false, false, dispatcherCliInst)
	setupSignals()
	gateService.Run() // run gate service in another goroutine
}

func setupSignals() {
	gwlog.Infof("Setup signals ...")
	signal.Ignore(syscall.Signal(10), syscall.Signal(12), syscall.SIGPIPE, syscall.SIGHUP)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			sig := <-signalChan
			if sig == syscall.SIGINT || sig == syscall.SIGTERM {
				// terminating gate ...
				gwlog.Infof("Terminating gate service ...")
				post.Post(func() {
					gateService.Terminate()
				})

				gateService.Terminated.Wait()
				gwlog.Infof("Gate %d terminated gracefully.", args.gateid)
				os.Exit(0)
			} else {
				gwlog.Errorf("unexpected signal: %s", sig)
			}
		}
	}()
}
