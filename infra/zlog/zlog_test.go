package zlog

import (
	"errors"
	"go.uber.org/zap"
	"testing"
)

func TestError(t *testing.T) {
	Error("TestError", zap.Error(errors.New("xsas")))
}
