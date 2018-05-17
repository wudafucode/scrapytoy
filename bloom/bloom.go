package bloom

import (

	"crypto/sha256"
	"fmt"
	"log"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"github.com/spaolacci/murmur3"
)

type BloomFilter interface {
	Put([]byte)
	PutString(string)

	Has([]byte) bool
	HasString(string) bool

	Close()
}

type Message interface {

}

type RedisBloomFilter struct {
	cli redis.Conn
	Queue   chan Message
	quit    chan struct{}
	n   uint
	k   uint
}

type outDataMsg struct {
	data      []byte       
}
type existDataMsg struct {
	data         []byte  
    retChan      chan<- bool     
}
func HashData(data []byte, seed uint) uint {
	sha_data := sha256.Sum256(data)
	data = sha_data[:]
	m := murmur3.New64WithSeed(uint32(seed))
	m.Write(data)
	return uint(m.Sum64())
}


func NewRedisBloomFilter(ip string, n, k uint) *RedisBloomFilter {

	 cli, err := redis.Dial("tcp", ip)
     if err!= nil{
		return nil
     }
	filter := &RedisBloomFilter{
		cli:          cli,
		Queue:  make(chan Message, 100), 
		quit:   make(chan struct{}),
		n:            n,
		k:            k,
	}
	
    go filter.queueHandler()
	return filter
}
func (filter *RedisBloomFilter) queueHandler() {
	fmt.Printf("queue handler\r\n")
	for{

		select{
				case msg:= <-filter.Queue:
					switch m:=msg.(type){
						case outDataMsg:
							  
							  filter.put(m.data)
						case existDataMsg:
							  
							  ret:=filter.has(m.data)
							  m.retChan<-ret
					    case interface{}:
					    	fmt.Printf("error\n\r")
					    	return 

					}
				case <-filter.quit:
			        return 	
		   }

	}
}
func (filter *RedisBloomFilter) put(data []byte) {
	
	for i := uint(0); i < filter.k; i++ {
		index := HashData(data, i) % filter.n
		_, err := filter.cli.Do("SETBIT", filter.redisKey(), index,1)
		if err != nil {
			log.Fatalf("%+v", errors.Wrap(err, "LSET"))
		}
	}
}

func (filter *RedisBloomFilter) PutString(data string) {
	filter.Queue<-outDataMsg{data:[]byte(data)}
}

func (filter *RedisBloomFilter) has(data []byte) bool {
	for i := uint(0); i < filter.k; i++ {
		index := HashData(data, i) % filter.n
		value, err := redis.Int(filter.cli.Do("GETBIT", filter.redisKey(), index))
		if err != nil {
			log.Fatalf("%+v", errors.Wrap(err, "LINDEX"))
		}
		if value != 1 {
			return false
		}
	}

	return true
}

func (filter *RedisBloomFilter) HasString(data string) bool {
     exist:=make(chan bool)
     filter.Queue<-existDataMsg{data:[]byte(data),retChan:exist}
     ret:=<-exist
     return ret
}

// Close 只将cli设置为nil, 关闭redis连接的操作放在调用处
func (filter *RedisBloomFilter) Close() {
	filter.cli = nil
}

// redisKey 根据filter的n和k来生成一个独立的redis key
func (filter *RedisBloomFilter) redisKey() string {
	return fmt.Sprintf("bloomfilter:")
}
