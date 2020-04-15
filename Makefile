.PHONY: dispatcher game test_client gate
.PHONY: runtestserver killtestserver test covertest install-deps

all: install dispatcher game test_client gate

install:
	go install ./cmd/...

dispatcher:
	cd components/dispatcher && go build

gate:
	cd components/gate && go build

test_game:
	cd components/game && go build

test_client:
	cd examples/test_client && go build

runtestserver: dispatcher gate game
	components/dispatcher/dispatcher &
	components/game/game -gid=1 -log info &
	components/game/game -gid=2 -log info &
	components/gate/gate -gid 1 -log debug &
	components/gate/gate -gid 2 -log debug &

killtestserver:
	- killall gate
	- sleep 3
	- killall game
	- sleep 5
	- killall dispatcher

test:
	go test -v `go list ./... | grep -v "/vendor/"`

covertest:
	go test -v -covermode=count `go list ./... | grep -v "/vendor/"`

install-deps:
	dep ensure
