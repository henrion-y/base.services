package middlewares

import (
	"net/http"
	"strings"

	"github.com/henrion-y/base.services/infra/jwt"

	"github.com/gin-gonic/gin"
)

type JWTAuthMiddleware struct {
	authService jwt.AuthService
}

func NewJWTAuthMiddleware(authService jwt.AuthService) (*JWTAuthMiddleware, error) {
	return &JWTAuthMiddleware{authService: authService}, nil
}

// SetClaims JWTAuthMiddleware 中间件，检查token
func (m *JWTAuthMiddleware) SetClaims() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		authHeader := ctx.Request.Header.Get("Authorization")
		if authHeader == "" {
			// 未携带token，视为游客
			ctx.Next()
			return
		}

		// 按空格拆分
		parts := strings.SplitN(authHeader, " ", 2)
		if !(len(parts) == 2 && parts[0] == "Bearer") {
			// 请求头中auth格式有误，没解析出token，视为游客
			ctx.Next()
			return
		}

		// 解析token包含的信息
		claims, err := m.authService.ParseToken(parts[1])
		if err != nil {
			// 没解析出token，视为游客
			ctx.Next()
			return
		}
		// 将当前请求的claims信息保存到请求的上下文c上
		ctx.Set("claims", claims)
		ctx.Next() // 后续的处理函数可以用过ctx.Get("claims")来获取当前请求的用户信息
	}
}

// SetClaimsAbortTourist JWTAuthMiddleware 中间件，检查token 拦截游客
func (m *JWTAuthMiddleware) SetClaimsAbortTourist() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		authHeader := ctx.Request.Header.Get("Authorization")
		if authHeader == "" {
			// 拦截游客
			ctx.JSON(http.StatusOK, gin.H{
				"code":    -1,
				"message": "无权限访问，请求未携带token",
			})
			ctx.Abort()
			return
		}

		// 按空格拆分
		parts := strings.SplitN(authHeader, " ", 2)
		if !(len(parts) == 2 && parts[0] == "Bearer") {
			// 拦截游客
			ctx.JSON(http.StatusOK, gin.H{
				"code":    -1,
				"message": "无权限访问，请求未携带token",
			})
			ctx.Abort()
			return
		}

		// 解析token包含的信息
		claims, err := m.authService.ParseToken(parts[1])
		if err != nil {
			// 拦截游客
			ctx.JSON(http.StatusOK, gin.H{
				"code":    -1,
				"message": "无权限访问，请求未携带token",
			})
			ctx.Abort()
			return
		}
		// 将当前请求的claims信息保存到请求的上下文c上
		ctx.Set("claims", claims)
		ctx.Next() // 后续的处理函数可以用过ctx.Get("claims")来获取当前请求的用户信息
	}
}
