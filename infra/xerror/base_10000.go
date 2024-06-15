package xerror

// 业务错误码模块
const (
	ErrCustom        = 10000 // 自定义错误
	ErrRuntime       = 10010 // 运行错误
	ErrParamRequired = 10020 // 缺少参数
	ErrParamInvalid  = 10030 // 参数格式错误
	ErrParamData     = 10040 // 参数错误
	ErrForbidden     = 10050 // 没有权限
	ErrShow2User     = 10100 // 透传错误，提示语会直接展示在界面上给用户看

	ErrAddFail    = 11000 // 创建失败
	ErrUpdateFail = 11001 // 更新失败
	ErrDeleteFail = 11002 // 删除失败
	ErrFindFail   = 11003 // 获取失败
)

var baseErrorMap = ErrorDefinition{
	ErrRuntime:       "运行错误",
	ErrParamRequired: "缺少参数",
	ErrParamInvalid:  "参数格式错误",
	ErrParamData:     "参数错误",
	ErrForbidden:     " 没有权限",

	ErrAddFail:    "创建失败",
	ErrUpdateFail: "更新失败",
	ErrDeleteFail: "删除失败",
	ErrFindFail:   "获取失败",
}

type ErrorDefinition map[int]string

var allErrorMap = make(ErrorDefinition)

func init() {
	for k, v := range baseErrorMap {
		allErrorMap[k] = v
	}
}

// NewXErrorByCode 通过错误码构造错误
func NewXErrorByCode(code int) *XError {
	return &XError{
		Code:    code,
		Message: allErrorMap[code],
	}
}

// NewCustomXError 构造自定义错误
func NewCustomXError(message string) *XError {
	return &XError{
		Code:    ErrCustom,
		Message: message,
	}
}

// NewShow2UserXError 构造展示给用户的错误
func NewShow2UserXError(message string) *XError {
	return &XError{
		Code:    ErrShow2User,
		Message: message,
	}
}
