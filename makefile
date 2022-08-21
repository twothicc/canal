
all: vendors format lint compile

fieldAlignment:
	fieldalignment -fix github.com/twothicc/canal/domain/entity/syncmanager

format:
	gofmt -s -w $$(find . -type f -name '*.go'| grep -v "/vendor/")

lint:
	golangci-lint run

vendors:
	go mod vendor

compile:
	cd app; go build -o ../build/canal

clearLog:
	> server.log

# Connect to jaeger via http://localhost:16686
startJaegerUI:
	docker run -d --name jaeger -e COLLECTOR_ZIPKIN_HTTP_PORT=9411 -p 5775:5775/udp -p 6831:6831/udp -p 6832:6832/udp -p 5778:5778 -p 16686:16686 -p 14268:14268 -p 9411:9411 jaegertracing/all-in-one:1.9

start:
	./build/canal