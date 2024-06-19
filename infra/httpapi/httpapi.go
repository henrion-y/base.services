package httpapi

import (
	"bytes"
	"errors"
	"github.com/henrion-y/base.services/infra/zlog"
	json "github.com/json-iterator/go"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

// GetJsonIoReader 获取json格式请求体
func GetJsonIoReader(payload interface{}) (io.Reader, error) {
	var reqBody io.Reader
	if payload == nil {
		reqBody = bytes.NewBuffer([]byte(`{}`))
	} else {
		payloadByte, err := json.Marshal(payload)
		if err != nil {
			zlog.Error("GetJsonIoReader.Marshal",
				zap.Any("payload", payload),
				zap.Error(err))
			return nil, err
		}
		reqBody = bytes.NewBuffer(payloadByte)
	}
	return reqBody, nil
}

// GetHttpResponse 获取Response
func GetHttpResponse(method string, urlStr string, reqBody io.Reader, headers map[string]string, timeout time.Duration) (*http.Response, error) {
	// 创建自定义的 HTTP 客户端
	client := &http.Client{
		Timeout: timeout,
	}

	// 创建 request
	request, err := http.NewRequest(method, urlStr, reqBody)
	if err != nil {
		var reqBodyByte []byte
		if request.Body != nil {
			reqBodyByte, _ = ioutil.ReadAll(request.Body)
		}
		zlog.Error("GetHttpResponse.NewRequest",
			zap.String("method", method),
			zap.String("urlStr", urlStr),
			zap.String("reqBody", string(reqBodyByte)),
			zap.Any("headers", headers),
			zap.Any("timeout", timeout),
			zap.Error(err))
		return nil, err
	}

	// 设置请求头部
	for k, v := range headers {
		request.Header.Set(k, v)
	}

	// 发送请求
	response, err := client.Do(request)
	if err != nil {
		var reqBodyByte []byte
		if request.Body != nil {
			reqBodyByte, _ = ioutil.ReadAll(request.Body)
		}
		zlog.Error("GetHttpResponse.Do",
			zap.String("method", method),
			zap.String("urlStr", urlStr),
			zap.String("reqBody", string(reqBodyByte)),
			zap.Any("headers", headers),
			zap.Any("timeout", timeout),
			zap.Error(err))
		return nil, err
	}
	return response, nil
}

// SendRequest 发送http请求
func SendRequest(method string, urlStr string, reqBody io.Reader, headers map[string]string, timeout time.Duration) ([]byte, int, error) {
	// 创建自定义的 HTTP 客户端
	client := &http.Client{
		Timeout: timeout,
	}

	// 创建 request
	request, err := http.NewRequest(method, urlStr, reqBody)
	if err != nil {
		var reqBodyByte []byte
		if request.Body != nil {
			reqBodyByte, _ = ioutil.ReadAll(request.Body)
		}
		zlog.Error("SendRequest.NewRequest",
			zap.String("method", method),
			zap.String("urlStr", urlStr),
			zap.String("reqBody", string(reqBodyByte)),
			zap.Any("headers", headers),
			zap.Any("timeout", timeout),
			zap.Error(err))
		return nil, 0, err
	}

	// 设置请求头部
	for k, v := range headers {
		request.Header.Set(k, v)
	}

	// 发送请求
	response, err := client.Do(request)
	if err != nil {
		var reqBodyByte []byte
		if request.Body != nil {
			reqBodyByte, _ = ioutil.ReadAll(request.Body)
		}
		zlog.Error("SendRequest.Do",
			zap.String("method", method),
			zap.String("urlStr", urlStr),
			zap.String("reqBody", string(reqBodyByte)),
			zap.Any("headers", headers),
			zap.Any("timeout", timeout),
			zap.Error(err))
		return nil, 0, err
	}

	defer response.Body.Close()

	// 读取响应内容
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		var reqBodyByte []byte
		if request.Body != nil {
			reqBodyByte, _ = ioutil.ReadAll(request.Body)
		}
		zlog.Error("SendRequest.ReadAll",
			zap.String("method", method),
			zap.String("urlStr", urlStr),
			zap.String("reqBody", string(reqBodyByte)),
			zap.Any("headers", headers),
			zap.Any("timeout", timeout),
			zap.Error(err))
		return nil, response.StatusCode, err
	}

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		var reqBodyByte []byte
		if request.Body != nil {
			reqBodyByte, _ = ioutil.ReadAll(request.Body)
		}
		zlog.Error("SendRequest.StatusCode!=ok",
			zap.String("method", method),
			zap.String("urlStr", urlStr),
			zap.String("reqBody", string(reqBodyByte)),
			zap.Any("headers", headers),
			zap.Any("timeout", timeout),
			zap.Any("respBody", string(body)),
			zap.Int("statusCode", response.StatusCode))
		return body, response.StatusCode, err
	}

	return body, response.StatusCode, nil
}

// SendRequestAndUnmarshal 发送请求并对返回值进行结构化,注意如果序列化的时候字段不匹配不会报错！！
func SendRequestAndUnmarshal(method string, urlStr string, reqBody io.Reader, headers map[string]string, timeout time.Duration, result interface{}) error {
	body, statusCode, err := SendRequest(method, urlStr, reqBody, headers, timeout)
	if err != nil {
		return err
	}

	if statusCode != http.StatusOK && statusCode != http.StatusCreated {
		return errors.New("statusCode != http.StatusOK")
	}

	err = json.Unmarshal(body, result)
	if err != nil {
		zlog.Error("SendRequestAndUnmarshal.Unmarshal",
			zap.String("method", method),
			zap.String("urlStr", urlStr),
			zap.Any("headers", headers),
			zap.Any("result", result),
			zap.Any("timeout", timeout),
			zap.Error(err))
		return err
	}

	return nil
}

// SendGet 发送get请求
func SendGet(urlStr string, headers map[string]string, timeout time.Duration) ([]byte, int, error) {
	return SendRequest(http.MethodGet, urlStr, nil, headers, timeout)
}

// SendGetAndUnmarshal 发送请求并对返回值进行结构化,注意如果序列化的时候字段不匹配不会报错！！
func SendGetAndUnmarshal(urlStr string, headers map[string]string, timeout time.Duration, result interface{}) error {
	return SendRequestAndUnmarshal(http.MethodGet, urlStr, nil, headers, timeout, result)
}

func SendPostByJsonBody(urlStr string, payload interface{}, headers map[string]string, timeout time.Duration) ([]byte, int, error) {
	if headers == nil {
		headers = map[string]string{
			"Content-Type": "application/json",
		}
	} else {
		headers["Content-Type"] = "application/json"
	}
	reqBody, err := GetJsonIoReader(payload)
	if err != nil {
		return nil, 0, err
	}

	return SendRequest(http.MethodPost, urlStr, reqBody, headers, timeout)
}

// SendPostByJsonBodyAndUnmarshal 发送请求并对返回值进行结构化,注意如果序列化的时候字段不匹配不会报错！！
func SendPostByJsonBodyAndUnmarshal(urlStr string, payload interface{}, headers map[string]string, timeout time.Duration, result interface{}) error {
	if headers == nil {
		headers = map[string]string{
			"Content-Type": "application/json",
		}
	} else {
		headers["Content-Type"] = "application/json"
	}
	reqBody, err := GetJsonIoReader(payload)
	if err != nil {
		return err
	}

	return SendRequestAndUnmarshal(http.MethodPost, urlStr, reqBody, headers, timeout, result)
}
