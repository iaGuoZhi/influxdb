package tsm1_test

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
)

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
