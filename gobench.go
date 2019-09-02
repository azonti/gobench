package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fastrand"
)

var (
	requests            int64
	period              int64
	clients             int
	url                 string
	urlsFilePath        string
	keepAlive           bool
	postDataFilePath    string
	writeTimeout        int
	readTimeout         int
	authHeader          string
	authHeadersFilePath string
	sendAuthHeader0Prob float64
	noCheckCert         bool
)

type Configuration struct {
	urls                []string
	method              string
	postData            []byte
	requests            int64
	period              int64
	keepAlive           bool
	authHeaders         []string
	sendAuthHeader0Prob float64
	noCheckCert         bool
	myClient            fasthttp.Client
}

type Result struct {
	requests        int64
	withAuthHeader0 int64
	success         int64
	networkFailed   int64
	information     int64
	redirection     int64
	clientError     int64
	serverError     int64
}

var readThroughput int64
var writeThroughput int64

type MyConn struct {
	net.Conn
}

func (this *MyConn) Read(b []byte) (n int, err error) {
	len, err := this.Conn.Read(b)

	if err == nil {
		atomic.AddInt64(&readThroughput, int64(len))
	}

	return len, err
}

func (this *MyConn) Write(b []byte) (n int, err error) {
	len, err := this.Conn.Write(b)

	if err == nil {
		atomic.AddInt64(&writeThroughput, int64(len))
	}

	return len, err
}

func init() {
	flag.Int64Var(&requests, "r", -1, "Number of requests per client")
	flag.IntVar(&clients, "c", 100, "Number of concurrent clients")
	flag.StringVar(&url, "u", "", "URL")
	flag.StringVar(&urlsFilePath, "f", "", "URL's file path (line seperated)")
	flag.BoolVar(&keepAlive, "k", true, "Do HTTP keep-alive")
	flag.StringVar(&postDataFilePath, "d", "", "HTTP POST data file path")
	flag.Int64Var(&period, "t", -1, "Period of time (in seconds)")
	flag.IntVar(&writeTimeout, "tw", 5000, "Write timeout (in milliseconds)")
	flag.IntVar(&readTimeout, "tr", 5000, "Read timeout (in milliseconds)")
	flag.StringVar(&authHeader, "auth", "", "Authorization header")
	flag.StringVar(&authHeadersFilePath, "auth_f", "", "Authorization header file path")
	flag.Float64Var(&sendAuthHeader0Prob, "send_auth0_prob", 0.2, "Probability of sending 0th Authorization header")
	flag.BoolVar(&noCheckCert, "nc", false, "Do not validate server's certificate")
}

func printResults(results map[int]*Result, startTime time.Time) {
	var requests int64
	var withAuthHeader0 int64
	var success int64
	var networkFailed int64
	var information int64
	var redirection int64
	var clientError int64
	var serverError int64

	for _, result := range results {
		requests += result.requests
		withAuthHeader0 += result.withAuthHeader0
		success += result.success
		networkFailed += result.networkFailed
		information += result.information
		redirection += result.redirection
		clientError += result.clientError
		serverError += result.serverError
	}

	elapsed := time.Since(startTime).Seconds()

	if elapsed == 0 {
		elapsed = 1
	}

	fmt.Println()
	fmt.Printf("Requests:                       %10d hits\n", requests)
	fmt.Printf("With 0th Authorization Header:  %10d hits\n", withAuthHeader0)
	fmt.Printf("Successful requests (2xx):      %10d hits\n", success)
	fmt.Printf("Network failed:                 %10d hits\n", networkFailed)
	fmt.Printf("Informational responses (1xx):  %10d hits\n", information)
	fmt.Printf("Redirections (3xx):             %10d hits\n", redirection)
	fmt.Printf("Client Errors (4xx):            %10d hits\n", clientError)
	fmt.Printf("Server Errors (5xx):            %10d hits\n", serverError)
	fmt.Printf("Successful requests rate:       %10.2f hits/sec\n", float64(success)/elapsed)
	fmt.Printf("Read throughput:                %10.2f bytes/sec\n", float64(readThroughput)/elapsed)
	fmt.Printf("Write throughput:               %10.2f bytes/sec\n", float64(writeThroughput)/elapsed)
	fmt.Printf("Test time:                      %10.2f sec\n", elapsed)
}

func readLines(path string, lines *[]string) (err error) {

	var file *os.File
	var part []byte
	var prefix bool

	if file, err = os.Open(path); err != nil {
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	buffer := bytes.NewBuffer(make([]byte, 0))
	for {
		if part, prefix, err = reader.ReadLine(); err != nil {
			break
		}
		buffer.Write(part)
		if !prefix {
			*lines = append(*lines, buffer.String())
			buffer.Reset()
		}
	}
	if err == io.EOF {
		err = nil
	}
	return
}

func NewConfiguration() *Configuration {

	if urlsFilePath == "" && url == "" {
		flag.Usage()
		os.Exit(1)
	}

	if requests == -1 && period == -1 {
		fmt.Println("Requests or period must be provided")
		flag.Usage()
		os.Exit(1)
	}

	if requests != -1 && period != -1 {
		fmt.Println("Only one should be provided: [requests|period]")
		flag.Usage()
		os.Exit(1)
	}

	configuration := &Configuration{
		urls:                make([]string, 0),
		method:              "GET",
		postData:            nil,
		keepAlive:           keepAlive,
		requests:            int64((1 << 63) - 1),
		authHeaders:         make([]string, 0),
		sendAuthHeader0Prob: sendAuthHeader0Prob,
		noCheckCert:         noCheckCert,
	}

	if period != -1 {
		configuration.period = period

		timeout := make(chan bool, 1)
		go func() {
			<-time.After(time.Duration(period) * time.Second)
			timeout <- true
		}()

		go func() {
			<-timeout
			pid := os.Getpid()
			proc, _ := os.FindProcess(pid)
			err := proc.Signal(os.Interrupt)
			if err != nil {
				log.Println(err)
				return
			}
		}()
	}

	if requests != -1 {
		configuration.requests = requests
	}

	if url != "" {
		configuration.urls = append(configuration.urls, url)
	}

	if urlsFilePath != "" {
		if err := readLines(urlsFilePath, &configuration.urls); err != nil {
			log.Fatalf("Error in ioutil.ReadFile for file: %s Error: %v", urlsFilePath, err)
		}
	}

	if postDataFilePath != "" {
		configuration.method = "POST"

		data, err := ioutil.ReadFile(postDataFilePath)

		if err != nil {
			log.Fatalf("Error in ioutil.ReadFile for file path: %s Error: %v", postDataFilePath, err)
		}

		configuration.postData = data
	}

	if authHeader != "" {
		configuration.authHeaders = append(configuration.authHeaders, authHeader)
	}

	if authHeadersFilePath != "" {
		if err := readLines(authHeadersFilePath, &configuration.authHeaders); err != nil {
			log.Fatalf("Error in ioutil.ReadFile for file: %s Error: %v", authHeadersFilePath, err)
		}
	}

	configuration.myClient.ReadTimeout = time.Duration(readTimeout) * time.Millisecond
	configuration.myClient.WriteTimeout = time.Duration(writeTimeout) * time.Millisecond
	configuration.myClient.MaxConnsPerHost = clients
	configuration.myClient.TLSConfig = &tls.Config{InsecureSkipVerify: noCheckCert}

	configuration.myClient.Dial = MyDialer()

	return configuration
}

func MyDialer() func(address string) (conn net.Conn, err error) {
	return func(address string) (net.Conn, error) {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			return nil, err
		}

		myConn := &MyConn{Conn: conn}

		return myConn, nil
	}
}

func client(clientNo int, configuration *Configuration, result *Result, done *sync.WaitGroup) {
	for result.requests < configuration.requests {
		for _, tmpUrl := range configuration.urls {

			req := fasthttp.AcquireRequest()

			req.SetRequestURI(tmpUrl)
			req.Header.SetMethodBytes([]byte(configuration.method))

			if configuration.keepAlive == true {
				req.Header.Set("Connection", "keep-alive")
			} else {
				req.Header.Set("Connection", "close")
			}

			switch true {
			case len(configuration.authHeaders) > 1:
				var i int
				if fastrand.Uint32() < uint32(math.MaxUint32*configuration.sendAuthHeader0Prob) {
					i = 0
					result.withAuthHeader0++
				} else {
					l := len(configuration.authHeaders) / configuration.myClient.MaxConnsPerHost
					i = clientNo*l + int(result.requests%int64(l))
				}
				req.Header.Set("Authorization", configuration.authHeaders[i])
			case len(configuration.authHeaders) > 0:
				req.Header.Set("Authorization", configuration.authHeaders[0])
			}

			req.SetBody(configuration.postData)

			resp := fasthttp.AcquireResponse()
			err := configuration.myClient.Do(req, resp)
			statusCode := resp.StatusCode()
			result.requests++
			fasthttp.ReleaseRequest(req)
			fasthttp.ReleaseResponse(resp)

			if err != nil {
				result.networkFailed++
				continue
			}

			switch true {
			case statusCode < 200:
				result.information++
			case statusCode < 300:
				result.success++
			case statusCode < 400:
				result.redirection++
			case statusCode < 500:
				result.clientError++
			default:
				result.serverError++
			}
		}
	}

	done.Done()
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	startTime := time.Now()
	var done sync.WaitGroup
	results := make(map[int]*Result)

	signalChannel := make(chan os.Signal, 2)
	signal.Notify(signalChannel, os.Interrupt)
	go func() {
		_ = <-signalChannel
		printResults(results, startTime)
		os.Exit(0)
	}()

	flag.Parse()

	configuration := NewConfiguration()

	goMaxProcs := os.Getenv("GOMAXPROCS")

	if goMaxProcs == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	fmt.Printf("Dispatching %d clients\n", clients)

	done.Add(clients)
	for i := 0; i < clients; i++ {
		result := &Result{}
		results[i] = result
		go client(i, configuration, result, &done)

	}
	fmt.Println("Waiting for results...")
	done.Wait()
	printResults(results, startTime)
}
