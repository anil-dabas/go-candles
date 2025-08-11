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
	"go-candles/internal/util"
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
	logger := util.NewLogger()

	configPath := flag.String("config", common.DefaultConfigPath, "Path to config file")
	pairsStr := flag.String("pairs", "", "Comma-separated pairs to subscribe (overrides config)")
	retryInterval := flag.Int("retry", 5, "Retry interval in seconds for reconnection")
	maxRetries := flag.Int("max-retries", 10, "Maximum number of retry attempts (0 for unlimited)")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.Error(err, common.ErrCodeConfigLoadFailed, common.ErrMsgConfigLoadFailed, "Failed to load config")
		os.Exit(1)
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
			logger.Error(nil, common.ErrCodeStreamClosed, common.ErrMsgStreamClosed, "Maximum retry attempts reached. Exiting...")
			break
		}

		if retryCount > 0 {
			logger.Info("Attempting to reconnect...", "retry_count", retryCount, "max_retries", *maxRetries, "retry_interval_sec", *retryInterval)
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
			logger.Error(err, common.ErrCodeGRPCConnectionFailed, common.ErrMsgGRPCConnectionFailed, "gRPC connect failed", "address", serverAddr)
			retryCount++
			continue
		}

		logger.Info("Successfully connected to gRPC server", "address", serverAddr)

		client := proto.NewCandleServiceClient(conn)
		if err := streamCandles(ctx, client, pairs); err != nil {
			logger.Error(err, common.ErrCodeStreamClosed, common.ErrMsgStreamClosed, "Streaming error occurred")
		}

		if err := conn.Close(); err != nil {
			logger.Error(err, common.ErrCodeGRPCConnectionCloseFailed, common.ErrMsgGRPCConnectionCloseFailed, "Failed to close gRPC connection")
		}
		retryCount++
	}
}

func streamCandles(ctx context.Context, client proto.CandleServiceClient, pairs []string) error {
	logger := util.NewLogger()

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := client.Subscribe(streamCtx, &proto.SubscribeRequest{Pairs: pairs})
	if err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	logger.Info("Subscribed to candle stream", "pairs", pairs)

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			logger.Info("Stream closed by server (EOF)")
			return nil
		}
		if err != nil {
			if isContextError(err) {
				logger.Debug("Context canceled, stopping stream", "error", err)
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
