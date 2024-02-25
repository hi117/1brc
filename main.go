package main

import (
	"bytes"
	"fmt"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"time"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type Station struct {
	Min, Max, Count, Sum, Average float64
	Name                          []byte
}

type Line struct {
	Name  []byte
	Value float64
}

var Stations = make([]Station, 10000)

func open() (mmap.MMap, int64, error) {
	f, err := os.OpenFile("./measurements.txt", os.O_RDONLY, 0644)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}

	mmap, _ := mmap.Map(f, mmap.RDONLY, 0)

	return mmap, stat.Size(), nil
}

func readLine(rawMap mmap.MMap, offset int64) (Line, int64, error) {
	endOffset := offset
	var err error
	var ret Line
	var search byte = ';'
	var floatBegin int64
	for {
		if rawMap[endOffset] == search {
			if search == ';' {
				// Extract the name
				ret.Name = rawMap[offset : endOffset-1]
				search = '\n'
				floatBegin = endOffset + 1
			} else {
				// Parse float
				ret.Value, err = strconv.ParseFloat(string(rawMap[floatBegin:endOffset]), 64)
				if err != nil {
					return ret, endOffset, errors.WithStack(err)
				}
				break
			}
		}
		endOffset += 1
	}
	return ret, endOffset, nil
}

func insertLine(line Line) {
	for i := range Stations {
		if Stations[i].Name != nil && bytes.Equal(Stations[i].Name, line.Name) {
			Stations[i].Count += 1
			if Stations[i].Max < line.Value {
				Stations[i].Max = line.Value
			}
			if Stations[i].Min > line.Value {
				Stations[i].Min = line.Value
			}
			Stations[i].Sum += line.Value
			return
		} else if Stations[i].Name == nil {
			Stations[i].Name = line.Name
			Stations[i].Count = 1
			Stations[i].Max = line.Value
			Stations[i].Min = line.Value
			Stations[i].Sum = line.Value
			return
		}
	}
}

func main() {
	f, err := os.Create("prof.out")
	if err != nil {
		log.Fatal().Err(err).Msg("could not create CPU profile")
	}
	defer f.Close() // error handling omitted for example
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal().Err(err).Msg("could not start CPU profile")
	}
	defer pprof.StopCPUProfile()

	rawMap, size, err := open()
	if err != nil {
		log.Fatal().Err(err).Stack().Msg("Could not open file")
	}
	defer rawMap.Unmap()

	var offset int64 = 0
	var count int64 = 0
	var start = time.Now()
	var trueStart = start
	var first bool
	for offset < size {
		var line Line
		line, offset, err = readLine(rawMap, offset)
		if err != nil {
			log.Fatal().Err(err).Stack().Msg("Could not read line")
		}
		insertLine(line)

		if count%1000000 == 0 {
			log.Info().Int64("count", count).Dur("time", time.Since(start)).Msg("Reading file")
			start = time.Now()
			if first {
				break
			}
			first = true
		}
		count += 1
	}
	log.Info().Dur("time", time.Since(trueStart)).Msg("Finished reading the data")

	for i, v := range Stations {
		if v.Name != nil {
			log.Debug().Int("index", i).Interface("value", v).Msg("Station Value")
		}
	}

	trueStart = time.Now()
	// Find the end
	end := 0
	for i := range Stations {
		if Stations[i].Name == nil {
			end = i
			break
		}
	}
	Stations = Stations[:end+1]

	// Now sort the new slice
	sort.Slice(Stations, func(i, j int) bool {
		if Stations[i].Name == nil {
			return false
		}
		return bytes.Compare(Stations[i].Name, Stations[j].Name) < 0
	})
	log.Info().Dur("time", time.Since(trueStart)).Msg("Finished sorting the data")

	trueStart = time.Now()
	for i := range Stations {
		if Stations[i].Name == nil {
			break
		}
		fmt.Printf("%s=%0.1f/%0.1f/%0.1f, ", string(Stations[i].Name), Stations[i].Min, Stations[i].Sum/Stations[i].Count, Stations[i].Max)
	}
	log.Info().Dur("time", time.Since(trueStart)).Msg("Finished printing the data")

	for i, v := range Stations {
		if v.Name != nil {
			log.Debug().Int("index", i).Interface("value", v).Msg("Station Value")
		}
	}
}
