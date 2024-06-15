package controllers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/henrion-y/base.services/infra/xerror"
)

func ResponseError(c *gin.Context, err error) {
	if xerr, ok := err.(*xerror.XError); ok {
		// fmt.Errorf("%+v", xerr)
		c.AbortWithStatusJSON(http.StatusOK, xerr)
	} else {
		ResponseError(c, xerror.NewXErrorByCode(xerror.ErrRuntime))
	}
}

type RespData struct {
	Code int32       `json:"code"`
	Data interface{} `json:"data,omitempty"`
}

func ResponseData(c *gin.Context, data interface{}) {
	b, _ := json.Marshal(data)
	fmt.Printf("%s %s response: %s", strings.ToUpper(c.Request.Method), c.Request.URL.Path, string(b))
	c.JSON(http.StatusOK, RespData{Code: 0, Data: data})
}

func ResponseSuccess(c *gin.Context) {
	fmt.Printf("%s %s response success", strings.ToUpper(c.Request.Method), c.Request.URL.Path)
	c.JSON(http.StatusOK, RespData{Code: 0})
}
