package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go-candles/internal/common"
	"go-candles/internal/config"
	"go-candles/internal/proto"
	"go-candles/internal/service"
	"google.golang.org/grpc"
)

func main() {
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()

	configPath := flag.String("config", common.DefaultConfigPath, "Path to config file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatal().
			Err(err).
			Str("error_code", common.ErrCodeConfigLoadFailed.String()).
			Str("error_message", common.ErrMsgConfigLoadFailed.String()).
			Msg("Failed to load config")
	}

	s := service.NewService(cfg)

	serverAddr := fmt.Sprintf(":%d", cfg.Server.Port)
	lis, err := net.Listen("tcp", serverAddr)
	if err != nil {
		log.Fatal().
			Err(err).
			Str("error_code", common.ErrCodeGRPCServeFailed.String()).
			Str("error_message", common.ErrMsgGRPCServeFailed.String()).
			Str("address", serverAddr).
			Msg("Failed to listen")
	}

	grpcServer := grpc.NewServer()
	proto.RegisterCandleServiceServer(grpcServer, s)

	go func() {
		log.Info().Str("address", serverAddr).Msg("Starting gRPC server")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal().
				Err(err).
				Str("error_code", common.ErrCodeGRPCServeFailed.String()).
				Str("error_message", common.ErrMsgGRPCServeFailed.String()).
				Msg("gRPC serve failed")
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Info().Msg("Shutting down server...")
	s.Shutdown()
	grpcServer.GracefulStop()
	log.Info().Msg("Server stopped gracefully")
}
