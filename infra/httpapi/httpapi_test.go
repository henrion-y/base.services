package httpapi

import (
	"net/http"
	"testing"
	"time"
)

func TestSendGet(t *testing.T) {
	urlStr := "https://kunpeng.csdn.net/ad/json/integrate/list?positions=932"

	data, statusCode, err := SendGet(urlStr, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(statusCode)
	t.Log(string(data))
}

func TestSendGetAndUnmarshal(t *testing.T) {
	type Data struct {
		Code    int         `json:"code"`
		Message string      `json:"message"`
		Data    interface{} `json:"data"`
	}

	urlStr := "https://kunpeng.csdn.net/ad/json/integrate/list?positions=932"
	data := Data{}
	err := SendGetAndUnmarshal(urlStr, nil, 0, &data)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(data.Code)
}

func TestSendPost(t *testing.T) {
	urlStr := ""

	headers := map[string]string{}

	data, statusCode, err := SendPostByJsonBody(urlStr, nil, headers, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(statusCode)
	t.Log(string(data))

}

func TestSendPostAndUnmarshal(t *testing.T) {
	type Data struct {
		Success   bool   `json:"success"`
		Error     string `json:"error"`
		ErrorCode int    `json:"errorCode"`
		Data      struct {
			Describe string `json:"describe"`
			FileType string `json:"file_type"`
			Link     string `json:"link"`
			Title    string `json:"title"`
		} `json:"data"`
	}

	data := Data{}

	urlStr := ""

	headers := map[string]string{}

	payload := map[string]string{
		"lan": "en",
	}

	err := SendPostByJsonBodyAndUnmarshal(urlStr, payload, headers, 0, &data)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(data)
}

func TestSendRequest(t *testing.T) {
	urlStr := "g"

	_, statusCode, err := SendRequest(http.MethodHead, urlStr, nil, nil, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(statusCode)

}

func TestSendRequestAndUnmarshal(t *testing.T) {
	type Data struct {
		Code    int         `json:"code"`
		Message string      `json:"message"`
		Data    interface{} `json:"data"`
	}

	urlStr := "https://kunpeng.csdn.net/ad/json/integrate/list?positions=932"
	data := Data{}
	err := SendRequestAndUnmarshal(http.MethodGet, urlStr, nil, nil, 0, &data)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(data.Code)
}

func TestSendGetTimeOut(t *testing.T) {
	urlStr := "http://127.0.0.1:5000/hello"

	timeout1 := 3 * time.Second
	data, statusCode, err := SendGet(urlStr, nil, timeout1)

	if err != nil {
		t.Log("打印错误 ：  ", err.Error())
		return
	}
	t.Log(data, statusCode)

}
