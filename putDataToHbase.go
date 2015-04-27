package main

import (
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/widuu/goini"
	"hbase"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const goNum = 50
const cacheNum = 200

var listenGetNum uint32 //用以记录系统已经收到了多少条数据
var chs []chan *InfoStruct

const conf_path = "./putDataToHbase.ini"

var putTable string
var hbasePORT string
var hbaseHOST string

var receivePort int
var maxFailNum int   //如果提交失败，重新提交尝试的次数
var channelCache int //每个channel可以缓存多少的数据

var clientPool chan *hbase.THBaseServiceClient
const clientPoolSize = 1000

type InfoStruct struct {
	info string
	name string
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	loadParameter()
	loadClient()
	loadGoroutine()
	go listenPort()
	showReceiveNum()

	return
}

/******************************************************
函数名称:loadGoroutine
函数功能:初始化用以传递数据的channel
******************************************************/
func loadGoroutine() {
	chs = make([]chan *InfoStruct, goNum)

	for i := 0; i < goNum; i++ {
		chs[i] = make(chan *InfoStruct, channelCache)
		go receiveAndCache(chs[i])
	}

	return
}

/******************************************************
函数名称:receiveAndCache
函数功能:接受从channel中传递的数据，缓存之；当缓存到一定数目时
		 或者在间隔一定时间没有收到数据时将已接受的数据提交到数据库
******************************************************/
func receiveAndCache(ch chan *InfoStruct) {
	var receiveNum uint32
	var cache [cacheNum](*InfoStruct)

	for {
		select {
		case cache[receiveNum] = <-ch:
			receiveNum++
			if receiveNum >= cacheNum {
				go putToDB(cache, receiveNum)
				receiveNum = 0
			}

		case <-time.After(10 * time.Second):
			if receiveNum > 0 {
				go putToDB(cache, receiveNum)
				receiveNum = 0
			}
		}
	}
	return
}

func putToDB(cache [cacheNum](*InfoStruct), putNum uint32) {
	if putNum <= 0 {
		return
	}

	//如果提交失败，则延迟一段时间再重新提交
	for i := 0; i < maxFailNum+1; i++ {
		err := putToHBase(cache, putNum)
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second)
		if i < maxFailNum {
			log.Println("[warning]:commit error,Re commit data after 2*time.Second.")
		} else {
			log.Println("[err]:", maxFailNum+1, " attempts still not committed to the database, give up.")
		}
	}

	return
}

func putToHBase(cache [cacheNum](*InfoStruct), putNum uint32) (err error) {
	var client *hbase.THBaseServiceClient
	var i uint32
	var putData []*hbase.TPut

	client = <- clientPool	
	for i = 0; i < putNum; i++ {
		putData = append(putData, &hbase.TPut{
			Row: []byte((*cache[i]).name),
			ColumnValues: []*hbase.TColumnValue{
				&hbase.TColumnValue{
					Family:    []byte("name"),
					Qualifier: []byte("info"),
					Value:     []byte((*cache[i]).info)}}})
	}
	err = client.PutMultiple([]byte(putTable), putData)
	if err != nil {
		log.Println("PutMultiple err:", err, " so lost data!")
		clientPool <- client
		return err
	}

	clientPool <- client
	return
}

func listenPort() {
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{
		IP:   net.IPv4(0, 0, 0, 0),
		Port: receivePort,
	})
	if err != nil {
		log.Println("监听失败:", err)
		return
	}
	defer socket.Close()

	for {
		var data [2048]byte
		readnum, _, err := socket.ReadFromUDP(data[0:])
		if err != nil {
			log.Println("读取数据失败", err)
			continue
		}

		listenGetNum++
		go dealReceiveData(data[:readnum], listenGetNum)
	}

	return
}

func dealReceiveData(data []byte, i uint32) {
	result, err := analyzeData(data)
	if err != nil {
		log.Println("err analyzeData,", err)
		return
	}

	i = i % goNum
	chs[i] <- &result

	return
}

//analyze
func analyzeData(data []byte) (result InfoStruct, err error) {
	tmp := string(data)
	spit := strings.Index(tmp, ":")
	if spit < 0 {
		err = errors.New("HeartBeat err")
		return
	}
	result.name = tmp[:spit]
	result.info = tmp[spit+1:]

	return
}

/******************************************************
函数名称:showReceiveNum
函数功能:显示已经接受到了多少条数据
******************************************************/
func showReceiveNum() {
	for {
		log.Println("[info]:now get:", listenGetNum)
		time.Sleep(5 * time.Second)
	}

	return
}

func myRand(n int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(n)
}

func loadClient() {
	log.Println("Loading hbase client...............")
	clientPool = make(chan *hbase.THBaseServiceClient, clientPoolSize)
	for i := 0; i < clientPoolSize; i++ {
		clientTemp, err := getHbaseClient()
		if err != nil {
			log.Println("get client error")
			os.Exit(1)
		}
		clientPool <- clientTemp
	}

	log.Println("Loading hbase client over..........")
	return
}

func getHbaseClient() (client *hbase.THBaseServiceClient, err error) {
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transport, err := thrift.NewTSocket(net.JoinHostPort(hbaseHOST, hbasePORT))
	if err != nil {
		return nil, err
	}
	client = hbase.NewTHBaseServiceClientFactory(transport, protocolFactory)
	if err = transport.Open(); err != nil {
		return nil, err
	}
	return
}

/******************************************************
函数名称:loadParameter
函数功能:初始化系统参数
******************************************************/
func loadParameter() {
	/*
		putTable = "hbase_test"
		hbaseHOST = "127.0.0.1"
		hbasePORT = "9090"
		receivePort = 33333
		channelCache = 5
		maxFailNum = 10
	*/
	log.Println("System Startup.......................")
	log.Println("Loading Parameter.....................")

	conf := goini.SetConfig(conf_path)
	putTable = conf.GetValue("DB", "PutTable")
	hbasePORT = conf.GetValue("DB", "HbasePORT")
	hbaseHOST = conf.GetValue("DB", "HbaseHOST")

	receivePort_string := conf.GetValue("system", "ReceivePort")
	receivePort_tmp, _ := strconv.ParseInt(receivePort_string, 10, 64)
	receivePort = int(receivePort_tmp)
	channelCache_string := conf.GetValue("system", "ChannelCache")
	channelCache_tmp, _ := strconv.ParseInt(channelCache_string, 10, 64)
	channelCache = int(channelCache_tmp)
	maxFailNum_string := conf.GetValue("system", "MaxFailNum")
	maxFailNum_tmp, _ := strconv.ParseInt(maxFailNum_string, 10, 64)
	maxFailNum = int(maxFailNum_tmp)

	log.Println("数据库提交表名为：", putTable)
	log.Println("Hbase提交地址为：", hbaseHOST, ":", hbasePORT)
	log.Println("接受数据端口为:", receivePort)
	log.Println("每个channel最大缓存数目:", channelCache)
	log.Println("提交失败时重新提交次数为:", maxFailNum)
	log.Println("Loading Parameter end ..............")
	return
}
