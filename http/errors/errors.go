package errors

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/pkg/errors"
)

// 错误码
const (
	// 通用错误
	ErrCustom           = 10000 // 自定义错误，透传给前端直接显示， 比如验证码未过期，剩余时间xx秒等
	ErrRuntime          = 10001
	ErrParamRequired    = 10002
	ErrParamInvalid     = 10003
	ErrInvoke           = 10004
	ErrSession          = 10005
	ErrUnauthenticated  = 10006
	ErrUnauthorized     = 10007
	ErrContentSensitive = 10008
)

var errorMap = map[int]errorDefinition{
	ErrCustom:           {code: ErrCustom, message: "自定义错误", status: http.StatusOK},
	ErrRuntime:          {code: ErrRuntime, message: "运行错误", status: http.StatusInternalServerError},
	ErrParamRequired:    {code: ErrParamRequired, message: "缺少参数", status: http.StatusBadRequest},
	ErrParamInvalid:     {code: ErrParamInvalid, message: "参数格式错误", status: http.StatusBadRequest},
	ErrContentSensitive: {code: ErrContentSensitive, message: "内容包含敏感词", status: http.StatusBadRequest},
	ErrInvoke:           {code: ErrInvoke, message: "调用服务失败", status: http.StatusInternalServerError},
	ErrSession:          {code: ErrSession, message: "用户会话错误", status: http.StatusInternalServerError},
	ErrUnauthenticated:  {code: ErrUnauthenticated, message: "用户未认证", status: http.StatusForbidden},
	ErrUnauthorized:     {code: ErrUnauthorized, message: "用户未授权", status: http.StatusUnauthorized},
}

// 业务自定义错误
type XError struct {
	Code    int    `json:"code"`    // 错误码
	Message string `json:"message"` // 错误消息
	Param   string `json:"param"`   // 当发生参数错误时返回具体的参数名，如 id。
	Status  int    `json:"-"`       // HTTP状态码
	Raw     error  `json:"-"`       // 原始错误
}

func (e *XError) Error() string {
	errorInfos := make([]string, 0)
	errorInfos = append(errorInfos, fmt.Sprintf("code [%d]", e.Code))
	if len(e.Param) > 0 {
		errorInfos = append(errorInfos, fmt.Sprintf("param [%s]", e.Param))
	}
	errorInfos = append(errorInfos, e.Message)
	if e.Raw != nil {
		errorInfos = append(errorInfos, fmt.Sprintf("%+v", e.Raw))
	}
	return strings.Join(errorInfos, " ")
}

func (e *XError) WithParam(param string) *XError {
	e.Param = param
	return e
}

func (e *XError) WithRaw(err error) *XError {
	e.Raw = errors.WithStack(err)
	return e
}

// 错误定义
type errorDefinition struct {
	code    int    // 错误码
	message string // 错误描述
	status  int    // HTTP返回状态码
}

// 构造错误
func New(code int) *XError {
	var definition errorDefinition
	if d, ok := errorMap[code]; ok {
		definition = d
	} else {
		definition = errorMap[ErrRuntime]
	}
	return &XError{
		Message: definition.message,
		Code:    definition.code,
		Status:  definition.status,
	}
}
