package main

import (
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type Station struct {
	Min, Max, Count, Sum, Average int64
}

type Line struct {
	Name  []byte
	Value int64
}

var Stations []map[string]Station

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

func readLine(rawMap mmap.MMap, offset int64) (Line, int64) {
	endOffset := offset
	var ret Line
	for {
		if rawMap[endOffset] == ';' {
			// Extract the name
			ret.Name = rawMap[offset:endOffset]
			// Parse the value ints start at 0x30
			negative := rawMap[endOffset+1] == '-'
			var is2Digit bool
			if negative {
				is2Digit = rawMap[endOffset+4] == '.'
			} else {
				is2Digit = rawMap[endOffset+3] == '.'
			}
			if is2Digit && negative {
				ret.Value = (int64(rawMap[endOffset+2]-'0')*100 + int64(rawMap[endOffset+3]-'0')*10 + int64(rawMap[endOffset+5]-'0')) * -1
				endOffset += 7
			} else if is2Digit && !negative {
				ret.Value = int64(rawMap[endOffset+1]-'0')*100 + int64(rawMap[endOffset+2]-'0')*10 + int64(rawMap[endOffset+4]-'0')
				endOffset += 6
			} else if negative {
				ret.Value = (int64(rawMap[endOffset+2]-'0')*10 + int64(rawMap[endOffset+4]-'0')) * -1
				endOffset += 6
			} else {
				ret.Value = int64(rawMap[endOffset+1]-'0')*10 + int64(rawMap[endOffset+3]-'0')
				endOffset += 5
			}
			return ret, endOffset
		}
		endOffset += 1
	}
}

func insertLine(cpu int64, line Line) {
	name := string(line.Name)
	_, ok := Stations[cpu][name]
	if !ok {
		Stations[cpu][name] = Station{
			Count: 1,
			Max:   line.Value,
			Min:   line.Value,
			Sum:   line.Value,
		}
	} else {
		station := Stations[cpu][name]
		station.Count += 1
		if station.Max < line.Value {
			station.Max = line.Value
		}
		if station.Min > line.Value {
			station.Min = line.Value
		}
		station.Sum += line.Value
		Stations[cpu][name] = station
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

	cpus := int64(runtime.NumCPU())
	chunkSize := size / cpus
	Stations = make([]map[string]Station, cpus)
	for i := range cpus {
		Stations[i] = make(map[string]Station)
	}

	var wg sync.WaitGroup
	start := time.Now()
	for i := range cpus {
		wg.Add(1)
		go func(i int64) {
			start := i * chunkSize
			end := (i + 1) * chunkSize

			// Seek start and end until we align with a new data piece
			for {
				if rawMap[end] == '\n' {
					break
				}
				end += 1
			}
			if start != 0 {
				for {
					if rawMap[start] == '\n' {
						start += 1
						break
					}
					start += 1
				}
			}

			for start < end {
				var line Line
				line, start = readLine(rawMap, start)
				insertLine(i, line)
			}
			wg.Done()
		}(int64(i))
	}

	wg.Wait()
	log.Info().Dur("time", time.Since(start)).Msg("Finished reading the data")
	start = time.Now()

	baseStations := Stations[0]
	for i := range cpus - 1 {
		i += 1
		cpuStations := Stations[i]
		for name, station := range cpuStations {
			baseStation, ok := baseStations[name]
			if !ok {
				baseStations[name] = station
			} else {
				baseStation.Count += station.Count
				baseStation.Sum += station.Sum
				if baseStation.Min > station.Min {
					baseStation.Min = station.Min
				}
				if baseStation.Max < station.Max {
					baseStation.Max = station.Max
				}
				baseStations[name] = baseStation
			}
		}
	}
	log.Info().Dur("time", time.Since(start)).Msg("Finished merging the data")
}
