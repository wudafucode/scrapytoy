package main

import (
	"fmt"

	"github.com/gocolly/colly"
	"strings"
	"./queue"
	"github.com/gomodule/redigo/redis"
	"log"
)

func main() {
	// Instantiate default collector
	c := colly.NewCollector(
		// Visit only domains: hackerspaces.org, wiki.hackerspaces.org
		colly.AllowedDomains("gallerix.asia"),

	)
	//c := colly.NewCollector()
    cli, err := redis.Dial("tcp", ":6379")
    if err!= nil{
		log.Fatal(err)
		return 
    }
    q:= queue.NewRedisQueue(cli,"queue1")
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

	// Before making a request print "Visiting ..."
	c.OnRequest(func(r *colly.Request) {
		url := r.URL.String()
		ret:=strings.Split(url,"/")
	    if len(ret)>2 && ret[len(ret)-2] == "pic"{
	    	q.Put(url)
	        fmt.Println("success")
	    }
		fmt.Println("Visiting", r.URL.String())
	})

	// Start scraping on https://hackerspaces.org
	c.Visit("https://gallerix.asia/")
}
