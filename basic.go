package main

import (
	//"fmt"

	"github.com/gocolly/colly"
	"strings"
	"./queue"
	"./bloom"
	"log"
	"flag"
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

	// Instantiate default collector
	c := colly.NewCollector(
		// Visit only domains: hackerspaces.org, wiki.hackerspaces.org
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

		//fmt.Println("lnk", e.Request.AbsoluteURL(link))
		c.Visit(e.Request.AbsoluteURL(link))
	})
    //count:=0 atmoic add
	// Before making a request print "Visiting ..."
	c.OnRequest(func(r *colly.Request) {
		url := r.URL.String()
		ret:=strings.Split(url,"/")
	    if len(ret)>2 && ret[len(ret)-2] == "pic"{
            if filter.HasString(url) == false{
            	q.Put(url)
            	filter.PutString(url)
	            //fmt.Println("success")
            }else{
            	log.Printf("duplicate:%s",url)
            	//fmt.Println("duplicate")	
            }
	    	
	    }
		//fmt.Println("Visiting", r.URL.String())
	})

	// Start scraping on https://hackerspaces.org
	c.Visit("https://gallerix.asia/")
}
