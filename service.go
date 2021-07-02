package main

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/runtime/protoimpl"
	"log"
	"net"
	"strings"
	"time"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type tokenAuth struct {
	Acl   map[string]interface{}
	Admin *Admin
	Biz   *Biz
}

type statStream struct {
	stream         Admin_StatisticsServer
	statByMethod   map[string]uint64
	statByConsumer map[string]uint64
}

type Admin struct {
	streamsLog  []Admin_LoggingServer
	streamsStat []statStream
}

func NewAdmin() *Admin {
	return &Admin{}
}

func (adm *Admin) Logging(nothing *Nothing, outStream Admin_LoggingServer) error {
	go func(adm *Admin, outStream Admin_LoggingServer) {
		adm.streamsLog = append(adm.streamsLog, outStream)
	}(adm, outStream)

	for {

	}

	return nil
}

func (adm *Admin) Statistics(inTime *StatInterval, outStream Admin_StatisticsServer) error {
	func(adm *Admin, outStream Admin_StatisticsServer) {
		adm.streamsStat = append(adm.streamsStat, statStream{stream: outStream})
	}(adm, outStream)

	go func(inTime *StatInterval, outStream Admin_StatisticsServer) {
		for {
			time.Sleep(time.Duration(inTime.IntervalSeconds) * time.Second)

			for i, stream := range adm.streamsStat {
				if stream.stream == outStream {
					out := &Stat{
						state:         protoimpl.MessageState{},
						sizeCache:     0,
						unknownFields: nil,
						Timestamp:     0,
						ByMethod:      stream.statByMethod,
						ByConsumer:    stream.statByConsumer,
					}

					stream.stream.Send(out)

					adm.streamsStat[i].statByMethod = nil
					adm.streamsStat[i].statByConsumer = nil
				}
			}
		}
	}(inTime, outStream)

	for {

	}

	return nil
}

type Biz struct {
}

func NewBiz() *Biz {
	return &Biz{}
}

func (biz Biz) Check(ctx context.Context, nthg *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (biz Biz) Add(ctx context.Context, nthg *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (biz Biz) Test(ctx context.Context, nthg *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func getConsumerFromMd(ctx context.Context) string {
	var consumer string

	md, _ := metadata.FromIncomingContext(ctx)
	consumers := md["consumer"]

	if len(consumers) > 0 {
		consumer = consumers[0]
	}

	return consumer
}

func createEvent(ctx context.Context, fullMethod string) Event {
	host, _ := peer.FromContext(ctx)
	consumer := getConsumerFromMd(ctx)

	return Event{
		state:         protoimpl.MessageState{},
		sizeCache:     0,
		unknownFields: nil,
		Timestamp:     0,
		Consumer:      consumer,
		Method:        fullMethod,
		Host:          host.Addr.String(),
	}
}

func addStat(oldStat map[string]uint64, keyValue string) map[string]uint64 {
	var newStat map[string]uint64

	if oldStat != nil {
		newStat = oldStat
	} else {
		newStat = map[string]uint64{}
	}

	cnt, ok := oldStat[keyValue]
	if !ok {
		newStat[keyValue] = 1
	} else {
		newStat[keyValue] = cnt + 1
	}

	return newStat
}

func (token tokenAuth) checkAuthFromContext(ctx context.Context, fullMethod string) error {
	md, _ := metadata.FromIncomingContext(ctx)

	consumer, ok := md["consumer"]
	if !ok || len(consumer) < 1 {
		return status.Error(codes.Unauthenticated, "ACL fail. Consumer is nil")
	}

	aclAllowedMethods, ok := token.Acl[consumer[0]]
	if !ok {
		return status.Error(codes.Unauthenticated, "ACL fail. Consumer not found in ACL")
	}

	var aclOk bool

aclLoop:
	for _, aclMethod := range aclAllowedMethods.([]interface{}) {
		aclMethodLevels := strings.Split(aclMethod.(string), "/")

		for i, infoMethodLevel := range strings.Split(fullMethod, "/") {
			if infoMethodLevel == aclMethodLevels[i] || aclMethodLevels[i] == "*" {
				aclOk = true
			} else {
				aclOk = false

				continue aclLoop
			}
		}

		if aclOk {
			break
		}
	}

	if !aclOk {
		return status.Error(codes.Unauthenticated, "ACL fail. Consumer not allowed in ACL")
	}

	return nil
}

func (token tokenAuth) authStreamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler) error {

	ev := createEvent(ss.Context(), info.FullMethod)

	for _, streams := range token.Admin.streamsLog {
		streams.Send(&ev)
	}

	consumer := getConsumerFromMd(ss.Context())

	for i, statStreams := range token.Admin.streamsStat {
		token.Admin.streamsStat[i].statByMethod = addStat(statStreams.statByMethod, info.FullMethod)
		token.Admin.streamsStat[i].statByConsumer = addStat(statStreams.statByConsumer, consumer)
	}

	err := token.checkAuthFromContext(ss.Context(), info.FullMethod)
	if err != nil {
		return err
	}

	err = handler(srv, ss)

	return err
}

func (token tokenAuth) authInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	ev := createEvent(ctx, info.FullMethod)

	for _, streams := range token.Admin.streamsLog {
		streams.Send(&ev)
	}

	consumer := getConsumerFromMd(ctx)

	for i, statStreams := range token.Admin.streamsStat {
		token.Admin.streamsStat[i].statByMethod = addStat(statStreams.statByMethod, info.FullMethod)
		token.Admin.streamsStat[i].statByConsumer = addStat(statStreams.statByConsumer, consumer)
	}

	err := token.checkAuthFromContext(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}

	reply, err := handler(ctx, req)

	return reply, err
}

func StartMyMicroservice(ctx context.Context, listenAddr string, ACLData string) error {
	var acl map[string]interface{}

	err := json.Unmarshal([]byte(ACLData), &acl)
	if err != nil {
		log.Println("cant unmarshal acl json", err)
		return err
	}

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Println("cant listen port", err)
		return err
	}

	newAdmin := NewAdmin()
	newBiz := NewBiz()

	tAuth := tokenAuth{acl,
		newAdmin,
		newBiz}

	server := grpc.NewServer(
		grpc.UnaryInterceptor(tAuth.authInterceptor),
		grpc.StreamInterceptor(tAuth.authStreamInterceptor),
	)

	RegisterAdminServer(server, newAdmin)
	RegisterBizServer(server, newBiz)

	fmt.Println("starting server at ", listenAddr)
	go server.Serve(lis)

	go func(done <-chan struct{}, server *grpc.Server) {
		select {
		case <-done:
			fmt.Println("stop server at ", listenAddr)
			server.Stop()
		}
	}(ctx.Done(), server)

	return nil
}
