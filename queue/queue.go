package queue


import(
	//"fmt"
	"github.com/gomodule/redigo/redis"
	"time"
	"log"
)


type RedisQueue struct {
	cli              redis.Conn
	queuekey         string
	urllist         chan string
    urlCacheCount    chan struct{}
    urlCache         chan string
}
func NewRedisQueue(ip string,queuekey string) *RedisQueue {
	 cli, err := redis.Dial("tcp", ip)
     if err!= nil{
		return nil
     }
	 q := &RedisQueue{
	 	cli           :cli,
	 	queuekey      :queuekey,
	 	urllist       :make(chan string, 100),
	 	urlCacheCount :make(chan struct{},10),
	 	urlCache      :make(chan string,10),
	 }
	 for i:=0;i<10;i++{
	 	 q.urlCacheCount<-struct{}{}
	 }
     go q.queueHandler()
     return q
}
func (q *RedisQueue) queueHandler() {
	for{
		select{
				case _=<-q.urlCacheCount:
					 url, err := redis.String(q.cli.Do("RPOP", q.queuekey))
					 if err != nil{
					 	q.urlCacheCount<-struct{}{}
					 
					 	time.Sleep(time.Millisecond*100)
					 	continue
					 }
					 q.urlCache<-url
					 //fmt.Printf("redis queue get url:%s\r\n",url)

				case url:=<-q.urllist:

					 _, err := q.cli.Do("RPUSH", q.queuekey,url)
					 if err != nil{
					 	log.Printf("oush url fail\r\n")
					 }


			}
	}
}
func (q *RedisQueue) Put(url string) {
	q.urllist<-url

}
func (q *RedisQueue) Get() string {
     url:=<-q.urlCache
     q.urlCacheCount<-struct{}{}
     return url

}