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
	"go-candles/internal/util"
	"google.golang.org/grpc"
)

func main() {
	configPath := flag.String("config", common.DefaultConfigPath, "Path to config file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load config")
	}

	// Set global log level from config
	switch cfg.LogLevel {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		log.Fatal().Str("log_level", cfg.LogLevel).Msg("Invalid log level in config, use: debug, info, warn, error")
	}

	// Configure logger
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
	logger := util.NewLogger()

	s := service.NewService(cfg)

	serverAddr := fmt.Sprintf(":%d", cfg.Server.Port)
	lis, err := net.Listen("tcp", serverAddr)
	if err != nil {
		logger.Error(err, common.ErrCodeGRPCServeFailed, common.ErrMsgGRPCServeFailed, "Failed to listen", "address", serverAddr)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterCandleServiceServer(grpcServer, s)

	go func() {
		logger.Info("Starting gRPC server", "address", serverAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error(err, common.ErrCodeGRPCServeFailed, common.ErrMsgGRPCServeFailed, "gRPC serve failed")
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down server...")
	s.Shutdown()
	grpcServer.GracefulStop()
	logger.Info("Server stopped gracefully")
}
