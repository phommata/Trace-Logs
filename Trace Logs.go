package main

import (
	"fmt"
	"log"
	"strings"
	"time"
)

/*
Trace logs
We have a stream of logs representing operation started/finished in following format:
"<timestamp> Operation <id> [started|finished]"
A program which reads a stream of logs and prints operation durations as they finish. The program also
prints the average operation duration and highest 3 durations every 10 seconds.

Example input:
2020-06-09T04:50:10.870Z Operation c118eb41 started
2020-06-09T04:50:12.870Z Operation c118eb41 finished
2020-06-09T04:50:13.000Z Operation def123a0 started
2020-06-09T04:50:13.400Z Operation def123a0 finished

Example output:
c118eb41 finished in 2000ms
def123a0 finished in 400ms

// 10 seconds later
Average: 1200ms
Top: 2000ms, 400ms

Hashmap of operations
Id => [start, finish]

Sorted list of durations after start & finish are calc
2000ms, 400ms

mapOps string
logArray = explode(space, string) logArray[0] 2020...
logArray[1] “operation” logArray[2] “c11...”
logArray[3] “started”

startTimestamp = now
Total = 0
Count = 0
topThreeDurations = [] // dynamically keeps all durations sorted,
Duration = 0

Check hashmap for oper id started is set
Hashmap = [
    C11.. => [2020..., null]
]

Check hashmap for oper id finished is set
Hashmap = [
    C11.. => [2020..., 200]
]

If both ts exist for oper id, calc duration
    Duration = subtract_datetime(
		Hashmap[c11..][1],
		Hashmap[c11..][0]
)

delete(hashmap[c11..])
//sorted(sortedDurations); // nlogn
//Insertion sort into sortedDurations and keep top 3 // nlogn

Loop thru topThreeDurations // n
    Check if duration is small than currTopThreeDurations
    Overwrite currTopThreeDurations

If duration
    Total += duration
    Count++

Operation 311.. Finished in ...ms
 */
const (
	started               = "started"
	finished              = "finished"
	topDurations          = 3
	printOutEveryXSeconds = 2
)

type Times struct {
	Started  time.Time
	Finished time.Time
}

func main() {
	startTimestamp := time.Now()
	inputs := []string{
		"2020-06-09T04:50:10.870Z Operation c118eb41 started",
		"2020-06-09T04:50:12.870Z Operation c118eb41 finished",
		"2020-06-09T04:50:13.000Z Operation def123a0 started",
		"2020-06-09T04:50:13.400Z Operation def123a0 finished",
	}
	hashMap := make(map[string]Times)
	var durations []int64 // slice keeps durations sorted
	durationsChan := make(chan []int64) // slice keeps durations sorted
	var total float64
	var count int64
	var average float64
	averageChan := make(chan float64)

	// background ticker
	// https://levelup.gitconnected.com/timer-and-ticker-using-golang-933a6d00a832
	go bgTask(startTimestamp, durationsChan, averageChan)
	/*logArray = explode(space, string)
	logArray[0] 2020...
	logArray[1] “operation”
	logArray[2] “c11...”
	logArray[3] “started”

	Check hashmap for oper id started is set
	Hashmap [C11..] = [2020..., null]

	Check hashmap for oper id finished is set
	Hashmap[C11..] = [2020..., 200]

	If both ts exist for oper id, calc duration
	    Duration = subtract_datetime(
	    Hashmap[c11..][1],
	    Hashmap[c11..][0] )

	unset(hashmap[c11..])

	//Insertion sort into sortedDurations and keep top 3 // nlogn
	// heap sort nlogn
	Loop thru topThreeDurations // n
	    Check if duration is small than currTopThreeDurations
	    Overwrite currTopThreeDurations
	*/
	for _, input := range(inputs) {
		s := strings.Split(input, " ")
		layout := "2006-01-02T15:04:05.000Z"
		timeParse, err := time.Parse(layout, s[0])

		if err != nil {
			log.Fatal(err)
		}

		_, found := hashMap[s[2]]

		if !found {
			hashMap[s[2]] = Times{}
		}

		if s[3] == started {
			hashMap[s[2]] = Times{Started: timeParse, Finished:hashMap[s[2]].Finished}
		} else if s[3] == finished {
			hashMap[s[2]] = Times{Started: hashMap[s[2]].Started, Finished: timeParse}
		}

		if !hashMap[s[2]].Started.IsZero() && !hashMap[s[2]].Finished.IsZero() {
			duration := hashMap[s[2]].Finished.Sub(hashMap[s[2]].Started).Microseconds()
			total += float64(duration)
			count++
			average = total / float64(count)
			averageChan <- average

			delete(hashMap, s[2])

			if len(durations) == 0 {
				durations = append(durations, duration)
			} else {
				for i, v := range(durations) {
					rI := len(durations) - i - 1

					if durations[rI] >= v {
						temp := durations[:rI + 1]
						durations = append(temp, durations[rI + 1:]...)
						durations = append(durations, duration)
					}
				}
			}

			fmt.Println("count: ", count)
			fmt.Println("durations: ", durations)
			durationsChan <- durations
		}
	}

	// here we use an empty select{} in order to keep
	// our main function alive indefinitely as it would
	// complete before our backgroundTask has a chance
	// to execute if we didn't.
	select {}
}

func bgTask(startTimestamp time.Time, durationsChan chan []int64, averageChan chan float64)  {
	ticker := time.NewTicker(1 * time.Second)
	var durations []int64
	var average float64

	for _ = range ticker.C {
		fmt.Println()
		select {
		case durations = <- durationsChan:
			fmt.Println("durations received: ", durations)
		case average = <- averageChan:
			fmt.Println("average received: ", int(average))
		default:
		}

		// Every 10 seconds in a coroutine lightweight thread by timer
		if int(time.Now().Sub(startTimestamp).Seconds()) % printOutEveryXSeconds == 0 {
			fmt.Println("average: ", int(average))
			fmt.Print("top durations: ")
			for i := 0; i < topDurations; i++ {
				if i > len(durations) - 1 ||  durations[i] ==  0 {
					break
				}
				fmt.Print(durations[i], " ")
			}
		}
	}
}