package main

import (
	"fmt"

	"github.com/gocolly/colly"
	"strings"
	"./queue"
	"./bloom"
	"log"
	"flag"
	"github.com/PuerkitoBio/goquery"
	"github.com/gomodule/redigo/redis"
	"io"
    "io/ioutil"
    "net/http"
    "os"
    "bytes"
)

var(
	serverip string
)
func init() {
      flag.StringVar(&serverip, "server", "localhost:6379", "masternode")
}
func main() {
	
    flag.Parse()
	log.SetFlags(log.Ldate | log.Ltime | log.LUTC | log.Lshortfile)
    
    urllist:=make([]string,100)
    //piclist:=make(chan string,100)
    countchan:=make(chan struct{},10)
	// Instantiate default collector
	c := colly.NewCollector(
		
		colly.AllowedDomains("gallerix.asia"),

	)

    q:= queue.NewRedisQueue(serverip,"galleryqueue")
    if q == nil{
		log.Fatal("redis queue err")
		return 
    }
    filter := bloom.NewRedisBloomFilter(serverip, 1<<30, 5)
    if filter == nil{
    	log.Fatal("redis bloomlfilter err")
		return 
    }

	// On every a element which has href attribute call callback
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		// Print link
		//fmt.Printf("Link found: %q -> %s\n", e.Text, link)
		// Visit link found on page
		// Only those links are visited which are in AllowedDomains
		//fmt.Printf("Link %s\n", e.Request.AbsoluteURL(link))

		urllist=append(urllist,e.Request.AbsoluteURL(link))
		
		//c.Visit(e.Request.AbsoluteURL(link))
	})
    c.OnHTML("img[src][title]", func(e *colly.HTMLElement) {
	
        img_url := e.Attr("src")
        urllist:=strings.Split(img_url,".")
        if(urllist[len(urllist)-1]=="gif"){
        	return 
        }
        //if strings.split
        countchan<-struct{}{}

	    img_name:=e.Attr("title")
	    img_url = "https:"+img_url
	    log.Printf("dowload url:%s,name:%s\r\n",img_url,img_name)
        
	    resp, err := http.Get(img_url)
	    if err != nil{
	    	log.Printf("url:%s",img_url)
	    	log.Println(err)  
	        return 
	    }
	    
		body, _ := ioutil.ReadAll(resp.Body)
		out, _ := os.Create(img_name)
		io.Copy(out, bytes.NewReader(body))
		
		
	})
    count:=0 
	// Before making a request print "Visiting ..."
	c.OnRequest(func(r *colly.Request) {
	
	    count = count+1
		fmt.Printf("total:%d,Visiting:%s;\r\n", count,r.URL.String())

	})

    //go downloadImage(piclist)
    go redisImageCount(countchan,serverip)
	// Start scraping on https://hackerspaces.org
	c.Visit("https://gallerix.asia/")
	
    var visiturl string 
    for{
    	visiturl = ""
        for _,url:=range urllist{
        	if filter.HasString(url) == false{
        		if visiturl == ""{
        			visiturl = url
        			continue
        		}
            	q.Put(url)
            	filter.PutString(url)
            }
        }
      
        urllist=urllist[:0]
        if visiturl == ""{
        	visiturl = q.Get()
        }
       
        c.Visit(visiturl)

    }

}
func redisImageCount(countchan chan struct{},ip string){
	cli, err := redis.Dial("tcp", ip)
    if err!= nil{
		return 
    }
    for{
        select {
            case <-countchan:
            	  _, err :=cli.Do("INCR", "count")
            	  if err != nil{
            	  	fmt.Printf("err")
            	  }

            }
    }
}
func downloadImage(piclist chan string){
	//piclist:=make(chan string,100)
	for{
		for url:=range piclist{
			doc, err := goquery.NewDocument(url)  
		    if err != nil {  
		        log.Println(err)  
		        continue
		    } 
            doc.Find("img[src][title]").Each(func(i int, s *goquery.Selection) {
		    img_url, _ := s.Attr("src")
		    img_name,_:=s.Attr("title")
		    img_url = "https:"+img_url
		    log.Printf("dowload url:%s,name:%s\r\n",img_url,img_name)

		    resp, err := http.Get(img_url)
		    if err != nil{
		    	log.Printf("url:%s",img_url)
		    	log.Println(err)  
		        return 
		    }
			body, _ := ioutil.ReadAll(resp.Body)
			out, _ := os.Create(img_name)
			io.Copy(out, bytes.NewReader(body))
   			})


		}
	}
}