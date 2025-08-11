package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go-candles/internal/common"
	"go-candles/internal/config"
	"go-candles/internal/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

var kacp = keepalive.ClientParameters{
	Time:                10 * time.Second, // send pings every 10 seconds
	Timeout:             time.Second,      // wait 1 second for ping ack
	PermitWithoutStream: true,             // send pings even without active streams
}

func main() {
	// Configure logger
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	configPath := flag.String("config", common.DefaultConfigPath, "Path to config file")
	pairsStr := flag.String("pairs", "", "Comma-separated pairs to subscribe (overrides config)")
	retryInterval := flag.Int("retry", 5, "Retry interval in seconds for reconnection")
	maxRetries := flag.Int("max-retries", 10, "Maximum number of retry attempts (0 for unlimited)")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatal().
			Err(err).
			Str("error_code", common.ErrCodeConfigLoadFailed.String()).
			Str("error_message", common.ErrMsgConfigLoadFailed.String()).
			Msg("Failed to load config")
	}

	var pairs []string
	if *pairsStr != "" {
		pairs = strings.Split(*pairsStr, ",")
	} else {
		pairs = cfg.Pairs
	}

	serverAddr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	retryCount := 0

	for {
		if *maxRetries > 0 && retryCount >= *maxRetries {
			log.Error().Msg("Maximum retry attempts reached. Exiting...")
			break
		}

		if retryCount > 0 {
			log.Info().
				Int("retry_count", retryCount).
				Int("max_retries", *maxRetries).
				Int("retry_interval_sec", *retryInterval).
				Msg("Attempting to reconnect...")
			time.Sleep(time.Duration(*retryInterval) * time.Second)
		}

		ctx := context.Background()
		conn, err := grpc.DialContext(
			ctx,
			serverAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithKeepaliveParams(kacp),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(common.MaxGRPCMessageSize),
				grpc.MaxCallSendMsgSize(common.MaxGRPCMessageSize),
			),
		)
		if err != nil {
			log.Error().
				Err(err).
				Str("error_code", common.ErrCodeGRPCConnectionFailed.String()).
				Str("error_message", common.ErrMsgGRPCConnectionFailed.String()).
				Str("address", serverAddr).
				Msg("gRPC connect failed")
			retryCount++
			continue
		}

		log.Info().Str("address", serverAddr).Msg("Successfully connected to gRPC server")

		client := proto.NewCandleServiceClient(conn)
		if err := streamCandles(ctx, client, pairs); err != nil {
			log.Error().
				Err(err).
				Str("error_code", common.ErrCodeStreamClosed.String()).
				Str("error_message", common.ErrMsgStreamClosed.String()).
				Msg("Streaming error occurred")
		}

		if err := conn.Close(); err != nil {
			log.Error().
				Err(err).
				Str("error_code", common.ErrCodeGRPCConnectionCloseFailed.String()).
				Str("error_message", common.ErrMsgGRPCConnectionCloseFailed.String()).
				Msg("Failed to close gRPC connection")
		}
		retryCount++
	}
}

func streamCandles(ctx context.Context, client proto.CandleServiceClient, pairs []string) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := client.Subscribe(streamCtx, &proto.SubscribeRequest{Pairs: pairs})
	if err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	log.Info().Strs("pairs", pairs).Msg("Subscribed to candle stream")

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			log.Info().Msg("Stream closed by server (EOF)")
			return nil
		}
		if err != nil {
			if isContextError(err) {
				log.Debug().Err(err).Msg("Context canceled, stopping stream")
				return nil
			}
			return fmt.Errorf("receive error: %w", err)
		}

		printCandle(resp)
	}
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func printCandle(candle *proto.CandleResponse) {
	fmt.Printf("Candle [%s @ %d]: O=%.2f H=%.2f L=%.2f C=%.2f V=%.2f\n",
		candle.Pair, candle.Timestamp, candle.Open, candle.High, candle.Low, candle.Close, candle.Volume)
}
