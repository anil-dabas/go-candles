package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"go-candles/internal/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	pairsStr := flag.String("pairs", "BTCUSDT,ETHUSDT", "Comma-separated pairs to subscribe")
	flag.Parse()

	pairs := strings.Split(*pairsStr, ",")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	log.Info().Msg("Attempting to connect to gRPC server at localhost:50051")
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Fatal().Err(err).Msg("gRPC connect failed")
	}
	defer conn.Close()
	log.Info().Msg("Successfully connected to gRPC server")

	client := proto.NewCandleServiceClient(conn)

	stream, err := client.Subscribe(ctx, &proto.SubscribeRequest{Pairs: pairs})
	if err != nil {
		log.Fatal().Err(err).Msg("Subscribe failed")
	}
	log.Info().Str("pairs", strings.Join(pairs, ",")).Msg("Subscribed to candle stream")

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			log.Info().Msg("Stream closed by server")
			break
		}
		if err != nil {
			log.Error().Err(err).Msg("Recv error")
			break
		}
		fmt.Printf("Candle [%s @ %d]: O=%.2f H=%.2f L=%.2f C=%.2f V=%.2f\n",
			resp.Pair, resp.Timestamp, resp.Open, resp.High, resp.Low, resp.Close, resp.Volume)
	}
}
