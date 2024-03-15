package xerror

import (
	"fmt"
)

// XError 通用错误格式 todo 考虑是否有必要改成小写
type XError struct {
	Code     int    `json:"code"`    // 错误码
	Message  string `json:"message"` // 错误消息
	RawError error  `json:"-"`       // 原始错误
}

// 重写 Error 方法
func (e *XError) Error() string {
	return fmt.Sprintf("code [%d] message [%s]", e.Code, e.Message)
}

func (e *XError) WithRawError(rawError error) *XError {
	e.RawError = rawError
	return e
}

func NewXError(code int, message string) *XError {
	return &XError{
		Code:    code,
		Message: message,
	}
}
