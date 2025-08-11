package util

import (
	"strconv"
	"strings"
)

func ParseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

func PairToBinance(pair string) string {
	return strings.ToLower(strings.ReplaceAll(pair, "-", ""))
}

func PairFromBinance(symbol string) string {
	return strings.ToUpper(symbol)
}

func PairToOKX(pair string) string {
	return strings.ReplaceAll(pair, "USDT", "-USDT")
}

func PairFromOKX(instId string) string {
	return strings.ReplaceAll(instId, "-USDT", "USDT")
}

func PairToCoinbase(pair string) string {
	if strings.HasSuffix(pair, "USDT") {
		return strings.ReplaceAll(pair, "USDT", "-USDT")
	}
	if strings.HasSuffix(pair, "USDC") {
		return strings.ReplaceAll(pair, "USDC", "-USDC")
	}
	return pair
}

func PairFromCoinbase(productID string) string {
	if strings.HasSuffix(productID, "-USDT") {
		return strings.ReplaceAll(productID, "-USDT", "USDT")
	}
	if strings.HasSuffix(productID, "-USDC") {
		return strings.ReplaceAll(productID, "-USDC", "USDC")
	}
	return productID
}
