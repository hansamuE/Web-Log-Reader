package main

import (
	"fmt"
	"github.com/satyrius/gonx"
	"flag"
	"io"
	"strings"
	"os"
	"time"
	"strconv"
	"sort"
)

type Request struct {
	time string
	userId string
	videoId string
}

type sortedMap struct {
	m map[string]int
	s []string
}

func (sm *sortedMap) Len() int {
	return len(sm.m)
}

func (sm *sortedMap) Less(i, j int) bool {
	return sm.m[sm.s[i]] < sm.m[sm.s[j]]
}

func (sm *sortedMap) Swap(i, j int) {
	sm.s[i], sm.s[j] = sm.s[j], sm.s[i]
}

func sortedKeys(m map[string]int, order string) []string {
	sm := new(sortedMap)
	sm.m = m
	sm.s = make([]string, len(m))
	i := 0
	for key, _ := range m {
		sm.s[i] = key
		i++
	}
	if order == "ASC" {
		sort.Sort(sm)
	} else if order == "DESC" {
		sort.Sort(sort.Reverse(sm))
	}
	return sm.s
}

var format string
var oriFormat string
var logFile string
var youTubeFile string
var outputFile string

func init()  {
	flag.StringVar(&oriFormat, "oriFormat", "$id\t$logtime\t$visit_time\t$title\t$url\t$transition\t$domain", "Original log format")
	flag.StringVar(&format, "format", "$request_time\t$user_id\t$video_id", "Log format")
	flag.StringVar(&logFile, "logFile", "/home/hansamuE/Documents/VCLab/history/history.txt", "Log file name to read.")
	flag.StringVar(&youTubeFile, "youTubeFile", "/home/hansamuE/Documents/VCLab/history/youtubeParsed.txt", "Parsed YouTube log file name.")
	flag.StringVar(&outputFile, "outputFile", "/home/hansamuE/Documents/VCLab/history/history-input.txt", "Log file name to output.")
}

func main() {
	flag.Parse()

	if _, err := os.Stat(youTubeFile); os.IsNotExist(err) {
		file, _ := os.Open(logFile)
		defer file.Close()
		reader := gonx.NewReader(file, oriFormat)
		writer, _ := os.Create(youTubeFile)
		defer writer.Close()
		requestTime := make(map[string]int)

		for {
			rec, err := reader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				panic(err)
			}

			url, _ := rec.Field("url")
			if strings.Contains(url, "youtube.com/watch?v=") {
				videoId := strings.Split(url, "v=")[1]
				if strings.Index(videoId, "&") != -1 {
					videoId = strings.Split(videoId, "&")[0]
				}
				if strings.Index(videoId, "#") != -1 {
					videoId = strings.Split(videoId, "#")[0]
				}

				visitTime, _ := rec.Field("visit_time")
				visitTimeParsed, _ := time.Parse("2006-01-02 15:04:05.000", visitTime)
				visitTimeUnix := int(visitTimeParsed.Unix())	//ignore year 2038 problem
				userId, _ := rec.Field("id")
				request := strconv.Itoa(visitTimeUnix) + "\t" + userId + "\t" + videoId + "\n"
				requestTime[request] = visitTimeUnix
			}
		}

		requestSorted := sortedKeys(requestTime, "ASC")
		for _, request := range requestSorted {
			writer.WriteString(request)
		}
	}

	file, _ := os.Open(youTubeFile)
	defer file.Close()
	reader := gonx.NewReader(file, format)

	requestSlice := make([]Request, 100)
	lastRequestTime := make(map[string]int)
	requestTimeThreshold := 300
	videoCount := make(map[string]int)

	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		requestTime, _ := rec.Field("request_time")
		requestTimeInt, _ := strconv.Atoi(requestTime)
		userId, _ := rec.Field("user_id")
		videoId, _ := rec.Field("video_id")

		if requestTimeInt - lastRequestTime[userId + "." + videoId] < requestTimeThreshold {
			continue
		}
		lastRequestTime[userId + "." + videoId] = requestTimeInt
		requestSlice = append(requestSlice, Request{requestTime, userId, videoId})
		videoCount[videoId]++
	}

	videoSorted := sortedKeys(videoCount, "DESC")
	isPopular := make(map[string]bool)
	numberPopular := 10
	for i := 0; i < numberPopular; i++ {
		isPopular[videoSorted[i]] = true
	}
	for videoCount[videoSorted[numberPopular]] == videoCount[videoSorted[numberPopular - 1]] {
		isPopular[videoSorted[numberPopular]] = true
		numberPopular++
	}

	videoCount = make(map[string]int)
	userCount := make(map[string]int)
	requestCount := 0

	file, _ = os.Open(youTubeFile)
	defer file.Close()
	reader = gonx.NewReader(file, format)
	writer, _ := os.Create(outputFile)
	defer writer.Close()

	for _, request := range requestSlice {
		videoId := request.videoId
		if !isPopular[videoId] {
			continue
		}

		requestTime := request.time
		userId := request.userId
		log := requestTime + "\t" + userId + "\t" + videoId + "\n"
		fmt.Print(log)
		writer.WriteString(log)
		videoCount[videoId]++
		userCount[userId]++
		requestCount++
	}
	for videoId, count := range videoCount {
		fmt.Printf("%s: %d\n", videoId, count)
	}
	fmt.Printf("User: %d\nVideo: %d\nRequest: %d", len(userCount), len(videoCount), requestCount)
}
