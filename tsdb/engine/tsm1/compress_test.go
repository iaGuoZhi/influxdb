package tsm1_test

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
)


func TestCompress_FloatBlock_SlopeFloats(t *testing.T) {
	rand.Seed(23)
	var firstTimestamp int64 = 1444238178437870000
	var iterations = 1000
	var size = 1000
	values := make([]tsm1.Value, size)
	var totalSize = int(0)
	for iteration:= 0; iteration < iterations; iteration++ {
		for i := 0; i < size; i++ {
			var value float64 = 300 * float64(i * (iteration + 1)) + 20 + float64(rand.Int() % 10) * 0.1
			values[i] = tsm1.NewValue(firstTimestamp, value)
			firstTimestamp += 1
		}
		b, err := tsm1.Values(values).Encode(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		totalSize += binary.Size(b)
	}
	fmt.Printf("Total bits: %v\n", totalSize)
}


func TestCompress_FloatBlock_SlopeFloatsRandomNoise(t *testing.T) {
	rand.Seed(23)
	var firstTimestamp int64 = 1444238178437870000
	var iterations = 1000
	var size = 1000
	values := make([]tsm1.Value, size)
	var totalSize = int(0)
	for iteration:= 0; iteration < iterations; iteration++ {
		for i := 0; i < size; i++ {
			var value float64 = 300 * float64(i * (iteration + 1)) + 20 + float64(rand.Int() % 10) * rand.Float64()
			values[i] = tsm1.NewValue(firstTimestamp, value)
			firstTimestamp += 1
		}
		b, err := tsm1.Values(values).Encode(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		totalSize += binary.Size(b)
	}
	fmt.Printf("Total bits: %v\n", totalSize)
}

func TestCompress_FloatBlock_Temperature_Floats(t *testing.T) {
	var firstTimestamp int64 = 1444238178437870000
	temperatures := [...]float64{64.2, 49.4, 48.8, 46.4, 47.9, 48.7, 48.9, 49.1, 49.0, 51.9, 51.7, 51.3, 47.0, 46.9, 47.5, 45.9, 44.5, 50.7, 54.0, 52.6, 54.2, 51.0, 53.5, 54.2, 54.2, 52.6, 55.5, 53.8, 54.3, 57.4, 56.9, 50.4, 50.1, 54.1, 49.1, 48.8, 50.7, 51.6, 52.6, 56.3, 59.0, 59.4, 55.5, 57.0, 60.8, 61.8, 57.7, 56.1, 53.4, 51.4, 52.6, 52.5, 57.5, 55.1, 54.3, 63.0, 60.0, 48.3, 55.3, 52.2, 56.6, 54.7, 51.9, 54.5, 58.5, 53.4, 51.8, 53.3, 65.6, 68.7, 58.4, 55.1, 52.8, 53.9, 54.8, 55.0, 52.8, 56.1, 56.5, 56.7, 51.4, 51.6, 53.3, 56.4, 54.7, 54.5, 53.4, 56.6, 53.2, 46.6, 47.4, 52.0, 62.2, 64.2, 59.5, 59.0, 54.9, 54.2, 57.8, 60.0, 61.1, 56.2, 56.1, 54.6, 54.5, 52.0, 56.6, 60.4, 62.7, 61.0, 56.5, 56.0, 53.1, 51.1, 57.2, 56.3, 56.5, 60.8, 60.4, 61.5}
	values := make([]tsm1.Value, len(temperatures))
	for i := 0; i < len(temperatures); i++ {
		values[i] = tsm1.NewValue(firstTimestamp, temperatures[i])
		firstTimestamp += 1
	}

	b, err := tsm1.Values(values).Encode(nil)
	fmt.Printf("Total bits: %v, %b\n", binary.Size(b)*8, b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

}


func TestCompress_FloatBlock_Temperature_Floats_All(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../city_temperature-fixed.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalBlocks := 0
	totalTime := time.Duration(0)
	decodingTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		row4, err := strconv.Atoi(row[4])
		row5, err := strconv.Atoi(row[5])
		t, err := time.Parse(layout, fmt.Sprintf("%02d/%02d/%s 00:00:00", row4, row5, row[6]))
		if err != nil {
			fmt.Println(err)
		} else {
			if value, err := strconv.ParseFloat(row[7], 64); err == nil {
				values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
				//fmt.Printf("%d: %v\n", t.UnixNano(), value)
			}
			currentRow += 1
			if currentRow == size {
				totalBlocks += 1
				currentRow = 0
				start := time.Now()
				b, err := tsm1.Values(values).Encode(nil)
		                if err != nil {
	                        fmt.Printf("unexpected error: %v\n", err)
        		        }

				//fmt.Println(len(b))
				totalSize += len(b)

				elapsed := time.Since(start)
				totalTime += elapsed

				// Read values out of decoder.
			    got := make([]float64, 0, len(values))
				start2 := time.Now()
				var dec tsm1.FloatDecoder
				if err := dec.SetBytes(b); err != nil {
						fmt.Printf("%s\n", err)
				}
				for dec.Next() {
						got = append(got, dec.Values())
				}
				elapsed2 := time.Since(start2)
				decodingTime += elapsed2

			}
		}
	}

	fmt.Printf("Total size: %v, Total Blocks: %v, Execution took %d, Decoding time %d\n", totalSize, totalBlocks, totalTime.Nanoseconds(), decodingTime.Nanoseconds())

}


func TestCompress_Stocks_Germany(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../Stocks_Germany_TKAG_XETRA_NoExpiry.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			currentRow = 0
			start := time.Now()
			if b, err := tsm1.Values(values).Encode(nil); err == nil {
				//fmt.Println(len(b))
				totalSize += len(b)
			}
			elapsed := time.Since(start)
			totalTime += elapsed
		}
	}
	fmt.Printf("Total size: %v, Execution took %s\n", totalSize, totalTime)
}

func TestCompress_Stocks_UK(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../Stocks_United_Kingdom_BLND.LSE_NoExpiry.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			currentRow = 0
			start := time.Now()
			if b, err := tsm1.Values(values).Encode(nil); err == nil {
				//fmt.Println(len(b))
				totalSize += len(b)
			}
			elapsed := time.Since(start)
			totalTime += elapsed
		}
	}

	fmt.Printf("Total size: %v, Execution took %s\n", totalSize, totalTime)

}

func TestCompress_Stocks_USA(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../Stocks_USA_BAX_NYSE_NoExpiry.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			currentRow = 0
			start := time.Now()
			if b, err := tsm1.Values(values).Encode(nil); err == nil {
				//fmt.Println(len(b))
				totalSize += len(b)
			}
			elapsed := time.Since(start)
			totalTime += elapsed
		}
	}

	fmt.Printf("Total size: %v, Execution took %s\n", totalSize, totalTime)

}


func TestCompress_Stocks_Germany_All(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../Stocks-Germany.txt.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalBlocks := 0
	totalTime := time.Duration(0)
	decodingTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			totalBlocks += 1
			currentRow = 0
			start := time.Now()
			b, err := tsm1.Values(values).Encode(nil)
			//fmt.Println(len(b))
			totalSize += len(b)
			if err != nil {
				fmt.Printf("unexpected error: %v\n", err)
			}
			elapsed := time.Since(start)
			totalTime += elapsed


			// Read values out of decoder.
			got := make([]float64, 0, len(values))
			start2 := time.Now()
			var dec tsm1.FloatDecoder
			if err := dec.SetBytes(b); err != nil {
				fmt.Printf("%s\n", err)
			}
			for dec.Next() {
				got = append(got, dec.Values())
			}
			elapsed2 := time.Since(start2)
			decodingTime += elapsed2
		}
	}

	fmt.Printf("Total size: %v, Total Blocks: %v, Execution took %d, Decoding time %d\n", totalSize, totalBlocks, totalTime.Nanoseconds(), decodingTime.Nanoseconds())

}


func TestCompress_Stocks_UK_All(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../Stocks-UK.txt.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalBlocks := 0
	totalTime := time.Duration(0)
	decodingTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			totalBlocks += 1
			currentRow = 0
			start := time.Now()
			b, err := tsm1.Values(values).Encode(nil)
			//fmt.Println(len(b))
			totalSize += len(b)
			if err != nil {
				fmt.Printf("unexpected error: %v\n", err)
			}
			elapsed := time.Since(start)
			totalTime += elapsed


			// Read values out of decoder.
			got := make([]float64, 0, len(values))
			start2 := time.Now()
			var dec tsm1.FloatDecoder
			if err := dec.SetBytes(b); err != nil {
				fmt.Printf("%s\n", err)
			}
			for dec.Next() {
				got = append(got, dec.Values())
			}
			elapsed2 := time.Since(start2)
			decodingTime += elapsed2
		}
	}

	fmt.Printf("Total size: %v, Total Blocks: %v, Execution took %d, Decoding time %d\n", totalSize, totalBlocks, totalTime.Nanoseconds(), decodingTime.Nanoseconds())

}


func TestCompress_Stocks_USA_All(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../Stocks-USA.txt.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalBlocks := 0
	totalTime := time.Duration(0)
	decodingTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			totalBlocks += 1
			currentRow = 0
			start := time.Now()
			b, err := tsm1.Values(values).Encode(nil)
			//fmt.Println(len(b))
			totalSize += len(b)
			if err != nil {
				fmt.Printf("unexpected error: %v\n", err)
			}
			elapsed := time.Since(start)
			totalTime += elapsed


			// Read values out of decoder.
			got := make([]float64, 0, len(values))
			start2 := time.Now()
			var dec tsm1.FloatDecoder
			if err := dec.SetBytes(b); err != nil {
				fmt.Printf("%s\n", err)
			}
			for dec.Next() {
				got = append(got, dec.Values())
			}
			elapsed2 := time.Since(start2)
			decodingTime += elapsed2
		}
	}

	fmt.Printf("Total size: %v, Total Blocks: %v, Execution took %d, Decoding time %d\n", totalSize, totalBlocks, totalTime.Nanoseconds(), decodingTime.Nanoseconds())

}

func TestCompress_Temp_Air_FNLT(t *testing.T) {
	size := 1000
	layout := "2006-01-02T15:04:05Z"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../NEON_temp-air-buoy-FNLT.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, row[0])
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[1], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			currentRow = 0
			start := time.Now()
			if b, err := tsm1.Values(values).Encode(nil); err == nil {
				//fmt.Println(len(b))
				totalSize += len(b)
			}
			elapsed := time.Since(start)
			totalTime += elapsed
		}
	}

	fmt.Printf("Total size: %v, Execution took %s\n", totalSize, totalTime)

}

func TestCompress_Wind_ABBY(t *testing.T) {
	size := 1000
	layout := "2006-01-02T15:04:05Z"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../ABBY.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, row[0])
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[1], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			currentRow = 0
			start := time.Now()
			if b, err := tsm1.Values(values).Encode(nil); err == nil {
				//fmt.Println(len(b))
				totalSize += len(b)
			}
			elapsed := time.Since(start)
			totalTime += elapsed
		}
	}

	fmt.Printf("Total size: %v, Execution took %s\n", totalSize, totalTime)

}

func TestCompress_Rel_Humidity_DewTemp(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../NEON_rel-humidity-buoy-dewTempMean.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalBlocks := 0
	totalTime := time.Duration(0)
	decodingTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			totalBlocks += 1
			currentRow = 0
			start := time.Now()
			b, err := tsm1.Values(values).Encode(nil)
			//fmt.Println(len(b))
			totalSize += len(b)
			if err != nil {
				fmt.Printf("unexpected error: %v\n", err)
			}
			elapsed := time.Since(start)
			totalTime += elapsed


			// Read values out of decoder.
			got := make([]float64, 0, len(values))
			start2 := time.Now()
			var dec tsm1.FloatDecoder
			if err := dec.SetBytes(b); err != nil {
				fmt.Printf("%s\n", err)
			}
			for dec.Next() {
				got = append(got, dec.Values())
			}
			elapsed2 := time.Since(start2)
			decodingTime += elapsed2
		}
	}

	fmt.Printf("Total size: %v, Total Blocks: %v, Execution took %d, Decoding time %d\n", totalSize, totalBlocks, totalTime.Nanoseconds(), decodingTime.Nanoseconds())

}


func TestCompress_Rel_Humidity_RHMean(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../NEON_rel-humidity-buoy-RHMean.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			currentRow = 0
			start := time.Now()
			if b, err := tsm1.Values(values).Encode(nil); err == nil {
				//fmt.Println(len(b))
				totalSize += len(b)
			}
			elapsed := time.Since(start)
			totalTime += elapsed
		}
	}

	fmt.Printf("Total size: %v, Execution took %s\n", totalSize, totalTime)

}


func TestCompress_Rel_Humidity_TempRHMean(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../NEON_rel-humidity-buoy-tempRHMean.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			currentRow = 0
			start := time.Now()
			if b, err := tsm1.Values(values).Encode(nil); err == nil {
				//fmt.Println(len(b))
				totalSize += len(b)
			}
			elapsed := time.Since(start)
			totalTime += elapsed
		}
	}

	fmt.Printf("Total size: %v, Execution took %s\n", totalSize, totalTime)

}


func TestCompress_Pressure_Air_StaPresMean(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../NEON_pressure-air_staPresMean.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalBlocks := 0
	totalTime := time.Duration(0)
	decodingTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			totalBlocks += 1
			currentRow = 0
			start := time.Now()
			b, err := tsm1.Values(values).Encode(nil)
			//fmt.Println(len(b))
			totalSize += len(b)
			if err != nil {
				fmt.Printf("unexpected error: %v\n", err)
			}
			elapsed := time.Since(start)
			totalTime += elapsed


			// Read values out of decoder.
			got := make([]float64, 0, len(values))
			start2 := time.Now()
			var dec tsm1.FloatDecoder
			if err := dec.SetBytes(b); err != nil {
				fmt.Printf("%s\n", err)
			}
			for dec.Next() {
				got = append(got, dec.Values())
			}
			elapsed2 := time.Since(start2)
			decodingTime += elapsed2
		}
	}

	fmt.Printf("Total size: %v, Total Blocks: %v, Execution took %d, Decoding time %d\n", totalSize, totalBlocks, totalTime.Nanoseconds(), decodingTime.Nanoseconds())

}


func TestCompress_Temp_BioMean(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../NEON_temp-bio-bioTempMean.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalBlocks := 0
	totalTime := time.Duration(0)
	decodingTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			totalBlocks += 1
			currentRow = 0
			start := time.Now()
			b, err := tsm1.Values(values).Encode(nil)
			//fmt.Println(len(b))
			totalSize += len(b)
			if err != nil {
				fmt.Printf("unexpected error: %v\n", err)
			}
			elapsed := time.Since(start)
			totalTime += elapsed


			// Read values out of decoder.
			got := make([]float64, 0, len(values))
			start2 := time.Now()
			var dec tsm1.FloatDecoder
			if err := dec.SetBytes(b); err != nil {
				fmt.Printf("%s\n", err)
			}
			for dec.Next() {
				got = append(got, dec.Values())
			}
			elapsed2 := time.Since(start2)
			decodingTime += elapsed2
		}
	}

	fmt.Printf("Total size: %v, Total Blocks: %v, Execution took %d, Decoding time %d\n", totalSize, totalBlocks, totalTime.Nanoseconds(), decodingTime.Nanoseconds())

}

func TestCompress_Size_Dust_Particulate_PM10Median(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../NEON_size-dust-particulate-PM10Median.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalBlocks := 0
	totalTime := time.Duration(0)
	decodingTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			totalBlocks += 1
			currentRow = 0
			start := time.Now()
			b, err := tsm1.Values(values).Encode(nil)
			//fmt.Println(len(b))
			totalSize += len(b)
			if err != nil {
				fmt.Printf("unexpected error: %v\n", err)
			}
			elapsed := time.Since(start)
			totalTime += elapsed


			// Read values out of decoder.
			got := make([]float64, 0, len(values))
			start2 := time.Now()
			var dec tsm1.FloatDecoder
			if err := dec.SetBytes(b); err != nil {
				fmt.Printf("%s\n", err)
			}
			for dec.Next() {
				got = append(got, dec.Values())
			}
			elapsed2 := time.Since(start2)
			decodingTime += elapsed2
		}
	}

	fmt.Printf("Total size: %v, Total Blocks: %v, Execution took %d, Decoding time %d\n", totalSize, totalBlocks, totalTime.Nanoseconds(), decodingTime.Nanoseconds())

}



func TestCompress_Size_Dust_Particulate_PM10sub50RHMedian(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../NEON_size-dust-particulate-PM10sub50RHMedian.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			currentRow = 0
			start := time.Now()
			if b, err := tsm1.Values(values).Encode(nil); err == nil {
				//fmt.Println(len(b))
				totalSize += len(b)
			}
			elapsed := time.Since(start)
			totalTime += elapsed
		}
	}

	fmt.Printf("Total size: %v, Execution took %s\n", totalSize, totalTime)

}

func TestCompress_Size_Dust_Particulate_PM15Median(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../NEON_size-dust-particulate-PM15Median.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			currentRow = 0
			start := time.Now()
			if b, err := tsm1.Values(values).Encode(nil); err == nil {
				//fmt.Println(len(b))
				totalSize += len(b)
			}
			elapsed := time.Since(start)
			totalTime += elapsed
		}
	}

	fmt.Printf("Total size: %v, Execution took %s\n", totalSize, totalTime)

}



func TestCompress_Size_Dust_Particulate_PM15sub50RHMedian(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../NEON_size-dust-particulate-PM15sub50RHMedian.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			currentRow = 0
			start := time.Now()
			if b, err := tsm1.Values(values).Encode(nil); err == nil {
				//fmt.Println(len(b))
				totalSize += len(b)
			}
			elapsed := time.Since(start)
			totalTime += elapsed
		}
	}

	fmt.Printf("Total size: %v, Execution took %s\n", totalSize, totalTime)

}

func TestCompress_Size_Dust_Particulate_PM1Median(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../NEON_size-dust-particulate-PM1Median.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			currentRow = 0
			start := time.Now()
			if b, err := tsm1.Values(values).Encode(nil); err == nil {
				//fmt.Println(len(b))
				totalSize += len(b)
			}
			elapsed := time.Since(start)
			totalTime += elapsed
		}
	}

	fmt.Printf("Total size: %v, Execution took %s\n", totalSize, totalTime)

}



func TestCompress_Size_Dust_Particulate_PM1sub50RHMedian(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../NEON_size-dust-particulate-PM1sub50RHMedian.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			currentRow = 0
			start := time.Now()
			if b, err := tsm1.Values(values).Encode(nil); err == nil {
				//fmt.Println(len(b))
				totalSize += len(b)
			}
			elapsed := time.Since(start)
			totalTime += elapsed
		}
	}

	fmt.Printf("Total size: %v, Execution took %s\n", totalSize, totalTime)

}

func TestCompress_Size_Dust_Particulate_PM25Median(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../NEON_size-dust-particulate-PM2.5Median.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			currentRow = 0
			start := time.Now()
			if b, err := tsm1.Values(values).Encode(nil); err == nil {
				//fmt.Println(len(b))
				totalSize += len(b)
			}
			elapsed := time.Since(start)
			totalTime += elapsed
		}
	}

	fmt.Printf("Total size: %v, Execution took %s\n", totalSize, totalTime)

}



func TestCompress_Size_Dust_Particulate_PM25sub50RHMedian(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../NEON_size-dust-particulate-PM2.5sub50RHMedian.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			currentRow = 0
			start := time.Now()
			if b, err := tsm1.Values(values).Encode(nil); err == nil {
				//fmt.Println(len(b))
				totalSize += len(b)
			}
			elapsed := time.Since(start)
			totalTime += elapsed
		}
	}

	fmt.Printf("Total size: %v, Execution took %s\n", totalSize, totalTime)

}

func TestCompress_Size_Dust_Particulate_PM4Median(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../NEON_size-dust-particulate-PM4Median.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			currentRow = 0
			start := time.Now()
			if b, err := tsm1.Values(values).Encode(nil); err == nil {
				//fmt.Println(len(b))
				totalSize += len(b)
			}
			elapsed := time.Since(start)
			totalTime += elapsed
		}
	}

	fmt.Printf("Total size: %v, Execution took %s\n", totalSize, totalTime)

}



func TestCompress_Size_Dust_Particulate_PM4sub50RHMedian(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../NEON_size-dust-particulate-PM4sub50RHMedian.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			currentRow = 0
			start := time.Now()
			if b, err := tsm1.Values(values).Encode(nil); err == nil {
				//fmt.Println(len(b))
				totalSize += len(b)
			}
			elapsed := time.Since(start)
			totalTime += elapsed
		}
	}

	fmt.Printf("Total size: %v, Execution took %s\n", totalSize, totalTime)

}

func TestCompress_Wind_2d_windDirMean(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../NEON_wind-2d_windDirMean.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalBlocks := 0
	totalTime := time.Duration(0)
	decodingTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			totalBlocks += 1
			currentRow = 0
			start := time.Now()
			b, err := tsm1.Values(values).Encode(nil)
			//fmt.Println(len(b))
			totalSize += len(b)
			if err != nil {
				fmt.Printf("unexpected error: %v\n", err)
			}
			elapsed := time.Since(start)
			totalTime += elapsed


			// Read values out of decoder.
			got := make([]float64, 0, len(values))
			start2 := time.Now()
			var dec tsm1.FloatDecoder
			if err := dec.SetBytes(b); err != nil {
				fmt.Printf("%s\n", err)
			}
			for dec.Next() {
				got = append(got, dec.Values())
			}
			elapsed2 := time.Since(start2)
			decodingTime += elapsed2
		}
	}

	fmt.Printf("Total size: %v, Total Blocks: %v, Execution took %d, Decoding time %d\n", totalSize, totalBlocks, totalTime.Nanoseconds(), decodingTime.Nanoseconds())

}

func TestCompress_Air_Sensor_Data(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../air-sensor-data.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalBlocks := 0
	totalTime := time.Duration(0)
	decodingTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			totalBlocks += 1
			currentRow = 0
			start := time.Now()
			b, err := tsm1.Values(values).Encode(nil)
			//fmt.Println(len(b))
			totalSize += len(b)
			if err != nil {
				fmt.Printf("unexpected error: %v\n", err)
			}
			elapsed := time.Since(start)
			totalTime += elapsed


			// Read values out of decoder.
			got := make([]float64, 0, len(values))
			start2 := time.Now()
			var dec tsm1.FloatDecoder
			if err := dec.SetBytes(b); err != nil {
				fmt.Printf("%s\n", err)
			}
			for dec.Next() {
				got = append(got, dec.Values())
			}
			elapsed2 := time.Since(start2)
			decodingTime += elapsed2
		}
	}

	fmt.Printf("Total size: %v, Total Blocks: %v, Execution took %d, Decoding time %d\n", totalSize, totalBlocks, totalTime.Nanoseconds(), decodingTime.Nanoseconds())

}

func TestCompress_Bird_Migration_Data(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../bird-migration-data.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalBlocks := 0
	totalTime := time.Duration(0)
	decodingTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			totalBlocks += 1
			currentRow = 0
			start := time.Now()
			b, err := tsm1.Values(values).Encode(nil)
			//fmt.Println(len(b))
			totalSize += len(b)
			if err != nil {
				fmt.Printf("unexpected error: %v\n", err)
			}
			elapsed := time.Since(start)
			totalTime += elapsed


			// Read values out of decoder.
			got := make([]float64, 0, len(values))
			start2 := time.Now()
			var dec tsm1.FloatDecoder
			if err := dec.SetBytes(b); err != nil {
				fmt.Printf("%s\n", err)
			}
			for dec.Next() {
				got = append(got, dec.Values())
			}
			elapsed2 := time.Since(start2)
			decodingTime += elapsed2
		}
	}

	fmt.Printf("Total size: %v, Total Blocks: %v, Execution took %d, Decoding time %d\n", totalSize, totalBlocks, totalTime.Nanoseconds(), decodingTime.Nanoseconds())

}

func TestCompress_Bitcoin_Price_Data(t *testing.T) {
	size := 1000
	layout := "01/02/2006 15:04:05"
	values := make([]tsm1.Value, size)

	f, err := os.Open("../../../bitcoin-price-data.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	currentRow := 0
	totalSize := 0
	totalBlocks := 0
	totalTime := time.Duration(0)
	decodingTime := time.Duration(0)
	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
			//fmt.Printf("%d: %v\n", t.UnixNano(), value)
		}
		currentRow += 1
		if currentRow == size {
			totalBlocks += 1
			currentRow = 0
			start := time.Now()
			b, err := tsm1.Values(values).Encode(nil)
			//fmt.Println(len(b))
			totalSize += len(b)
			if err != nil {
				fmt.Printf("unexpected error: %v\n", err)
			}
			elapsed := time.Since(start)
			totalTime += elapsed


			// Read values out of decoder.
			got := make([]float64, 0, len(values))
			start2 := time.Now()
			var dec tsm1.FloatDecoder
			if err := dec.SetBytes(b); err != nil {
				fmt.Printf("%s\n", err)
			}
			for dec.Next() {
				got = append(got, dec.Values())
			}
			elapsed2 := time.Since(start2)
			decodingTime += elapsed2
		}
	}

	fmt.Printf("Total size: %v, Total Blocks: %v, Execution took %d, Decoding time %d\n", totalSize, totalBlocks, totalTime.Nanoseconds(), decodingTime.Nanoseconds())

}
//
//func TestCompress_Basel_Temp(t *testing.T) {
//        size := 1000
//        layout := "01/02/2006 15:04:05"
//        values := make([]tsm1.Value, size)
//
//        f, err := os.Open("../../../datasets/basel-temp.csv.gz")
//        defer f.Close()
//        gz, err := gzip.NewReader(f)
//        if err != nil {
//                fmt.Println(err)
//        }
//        defer gz.Close()
//        scanner := bufio.NewScanner(gz)
//        currentRow := 0
//        totalSize := 0
//        totalBlocks := 0
//        totalTime := time.Duration(0)
//        decodingTime := time.Duration(0)
//        for scanner.Scan() {
//                row := strings.Split(scanner.Text(), ",")
//                t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
//                if err != nil {
//                        fmt.Println(err)
//                }
//                if value, err := strconv.ParseFloat(row[2], 64); err == nil {
//                        values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
//                }
//                currentRow += 1
//                if currentRow == size {
//                        totalBlocks += 1
//                        currentRow = 0
//                        start := time.Now()
//                        b, err := tsm1.Values(values).Encode(nil)
//                        totalSize += len(b)
//                        if err != nil {
//                                fmt.Printf("unexpected error: %v\n", err)
//                        }
//                        elapsed := time.Since(start)
//                        totalTime += elapsed
//
//
//                        // Read values out of decoder.
//                        got := make([]float64, 0, len(values))
//                        start2 := time.Now()
//                        var dec tsm1.FloatDecoder
//                        if err := dec.SetBytes(b); err != nil {
//                                fmt.Printf("%s\n", err)
//                        }
//                        for dec.Next() {
//                                got = append(got, dec.Values())
//                        }
//                        elapsed2 := time.Since(start2)
//                        decodingTime += elapsed2
//                }
//        }
//
//        fmt.Printf("Bits per value: %v\nCompression time per block %v\nDecoding time per block %v\n", float64(totalSize*8)/float64(totalBlocks*1000), float64(totalTime.Nanoseconds())/float64(totalBlocks), float64(decodingTime.Nanoseconds())/float64(totalBlocks))
//
//}
//
//
//func TestCompress_Basel_Winc_Speed(t *testing.T) {
//        size := 1000
//        layout := "01/02/2006 15:04:05"
//        values := make([]tsm1.Value, size)
//
//        f, err := os.Open("../../../datasets/basel-wind-speed.csv.gz")
//        defer f.Close()
//        gz, err := gzip.NewReader(f)
//        if err != nil {
//                fmt.Println(err)
//        }
//        defer gz.Close()
//        scanner := bufio.NewScanner(gz)
//        currentRow := 0
//        totalSize := 0
//        totalBlocks := 0
//        totalTime := time.Duration(0)
//        decodingTime := time.Duration(0)
//        for scanner.Scan() {
//                row := strings.Split(scanner.Text(), ",")
//                t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
//                if err != nil {
//                        fmt.Println(err)
//                }
//                if value, err := strconv.ParseFloat(row[2], 64); err == nil {
//                        values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
//                }
//                currentRow += 1
//                if currentRow == size {
//                        totalBlocks += 1
//                        currentRow = 0
//                        start := time.Now()
//                        b, err := tsm1.Values(values).Encode(nil)
//                        totalSize += len(b)
//                        if err != nil {
//                                fmt.Printf("unexpected error: %v\n", err)
//                        }
//                        elapsed := time.Since(start)
//                        totalTime += elapsed
//
//
//                        // Read values out of decoder.
//                        got := make([]float64, 0, len(values))
//                        start2 := time.Now()
//                        var dec tsm1.FloatDecoder
//                        if err := dec.SetBytes(b); err != nil {
//                                fmt.Printf("%s\n", err)
//                        }
//                        for dec.Next() {
//                                got = append(got, dec.Values())
//                        }
//                        elapsed2 := time.Since(start2)
//                        decodingTime += elapsed2
//                }
//        }
//
//        fmt.Printf("Bits per value: %v\nCompression time per block %v\nDecoding time per block %v\n", float64(totalSize*8)/float64(totalBlocks*1000), float64(totalTime.Nanoseconds())/float64(totalBlocks*1000), float64(decodingTime.Nanoseconds())/float64(totalBlocks*1000))
//
//}
//
func TestCompress_Basel_CR_and_Thru(t *testing.T) {
        size := 1000
        layout := "01/02/2006 15:04:05"
        values := make([]tsm1.Value, size)

        f, err := os.Open("../../../datasets/basel-temp.csv.gz")
        defer f.Close()
        gz, err := gzip.NewReader(f)
        if err != nil {
                fmt.Println(err)
        }
        defer gz.Close()
        scanner := bufio.NewScanner(gz)
        currentRow := 0
        totalSize := 0
        totalBlocks := 0
        totalTime := time.Duration(0)
        decodingTime := time.Duration(0)
        for scanner.Scan() {
                row := strings.Split(scanner.Text(), ",")
                t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
                if err != nil {
                        fmt.Println(err)
                }
                if value, err := strconv.ParseFloat(row[2], 64); err == nil {
                        values[currentRow] = tsm1.NewValue(t.UnixNano(), value)
                }
                currentRow += 1
                if currentRow == size {
                        totalBlocks += 1
                        currentRow = 0
                        start := time.Now()
                        b, err := tsm1.Values(values).Encode(nil)
                        totalSize += len(b)
                        if err != nil {
                                fmt.Printf("unexpected error: %v\n", err)
                        }
                        elapsed := time.Since(start)
                        totalTime += elapsed

                        // Read values out of decoder.
                        got := make([]float64, 0, len(values))
                        start2 := time.Now()
                        var dec tsm1.FloatDecoder
                        if err := dec.SetBytes(b); err != nil {
                                fmt.Printf("%s\n", err)
                        }
                        for dec.Next() {
                                got = append(got, dec.Values())
                        }
                        elapsed2 := time.Since(start2)
                        decodingTime += elapsed2
                }
        }

        fmt.Fprintf(os.Stderr, "Comp-bytes: %v cr: %v Compression time: %v Decoding time: %v\n", totalSize, float64(totalSize)/float64(totalBlocks*1000), float64(totalTime.Nanoseconds()), float64(decodingTime.Nanoseconds()))
}
