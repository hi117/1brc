package main

import (
	"bytes"
	"fmt"
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
	Name                          []byte
}

type StationsEntry struct {
	Stations []Station
	Count    int64
}

const StationsEntrySize = 16384 // 2^14
const StationsEntryLog = 14

func (s *StationsEntry) insertLine(line Line) {
	index := int64(StationsEntrySize / 2)

	var cmp int64
	for curDepth := StationsEntryLog; curDepth >= 0; curDepth-- {
		cmp = compare(line.Name, s.Stations[index].Name)
		// We found the entry
		if cmp == 0 {
			s.Stations[index].Sum += line.Value
			s.Stations[index].Count += 1
			if s.Stations[index].Min > line.Value {
				s.Stations[index].Min = line.Value
			}
			if s.Stations[index].Max < line.Value {
				s.Stations[index].Max = line.Value
			}
			return
		}
		// go down left tree
		if cmp == -1 {
			if curDepth != 1 {
				index -= StationsEntrySize >> (StationsEntryLog - (curDepth - 2))
			} else {
				index -= 1
			}
		}
		// go down right tree
		if cmp == 1 {
			if curDepth > 1 {
				index += StationsEntrySize >> (StationsEntryLog - (curDepth - 2))
			} else {
				index += 1
			}
		}
	}
	if s.Count != 0 {
		for i := s.Count; i != index; i-- {
			s.Stations[i] = s.Stations[i-1]
		}
	}
	s.Stations[index] = Station{
		Name:  line.Name,
		Sum:   line.Value,
		Count: 1,
		Min:   line.Value,
		Max:   line.Value,
	}
	s.Count += 1
}

func (s *StationsEntry) insertStation(station Station) {
	index := int64(StationsEntrySize / 2)

	var cmp int64
	for curDepth := StationsEntryLog; curDepth >= 0; curDepth-- {
		cmp = compare(station.Name, s.Stations[index].Name)
		// We found the entry
		if cmp == 0 {
			s.Stations[index].Sum += station.Sum
			s.Stations[index].Count += station.Count
			if s.Stations[index].Min > station.Min {
				s.Stations[index].Min = station.Min
			}
			if s.Stations[index].Max < station.Max {
				s.Stations[index].Max = station.Max
			}
			return
		}
		// go down left tree
		if cmp == -1 {
			if curDepth != 1 {
				index -= StationsEntrySize >> (StationsEntryLog - (curDepth - 2))
			} else {
				index -= 1
			}
		}
		// go down right tree
		if cmp == 1 {
			if curDepth > 1 {
				index += StationsEntrySize >> (StationsEntryLog - (curDepth - 2))
			} else {
				index += 1
			}
		}
	}
	if s.Count != 0 {
		for i := s.Count; i != index; i-- {
			s.Stations[i] = s.Stations[i-1]
		}
	}
	s.Stations[index] = Station{
		Name:  station.Name,
		Sum:   station.Sum,
		Count: station.Count,
		Min:   station.Min,
		Max:   station.Max,
	}
	s.Count += 1
}

func compare(a, b []byte) int64 {
	// Same as compare but puts nulls on the right (-1)
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1
	}
	if b == nil {
		return -1
	}
	return int64(bytes.Compare(a, b))
}

var Stations []StationsEntry

type Line struct {
	Name  []byte
	Value int64
}

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

	cpus := int64(runtime.NumCPU()) * 2
	//cpus := int64(1)
	chunkSize := size / cpus
	Stations = make([]StationsEntry, cpus)
	for i := range cpus {
		Stations[i].Stations = make([]Station, StationsEntrySize)
	}

	var wg sync.WaitGroup
	start := time.Now()
	for i := range cpus {
		wg.Add(1)
		go func(i int64) {
			start := i * chunkSize
			end := (i + 1) * chunkSize
			if end >= size {
				end = size - 1
			}

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
				Stations[i].insertLine(line)
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
		for j := range Stations[i].Count {
			baseStations.insertStation(Stations[i].Stations[j])
		}
	}
	log.Info().Dur("time", time.Since(start)).Msg("Finished merging the data")

	start = time.Now()
	os.Stdout.Write([]byte{'{'})
	for i := range baseStations.Count {
		os.Stdout.Write(baseStations.Stations[i].Name)
		fmt.Printf("=%0.1f/%0.1f/%0.1f",
			float64(baseStations.Stations[i].Min)/10,
			(float64(baseStations.Stations[i].Sum)/10)/(float64(baseStations.Stations[i].Count)/10),
			float64(baseStations.Stations[i].Max)/10)
		if i+1 != baseStations.Count {
			os.Stdout.Write([]byte{',', ' '})
		}
	}
	os.Stdout.Write([]byte{'}'})
	log.Info().Dur("time", time.Since(start)).Msg("Finished writing the data")
}
