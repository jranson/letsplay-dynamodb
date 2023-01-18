package csv

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type FieldList []string
type ItemData []interface{}
type ItemDataList []ItemData
type Item map[string]interface{}

type CSVData struct {
	FieldCount int
	RowCount   int
	Fields     FieldList
	Items      ItemDataList
	Delimiter  string
}

var ErrInvalidDataLine = errors.New("invalid data line in csv")

// LoadFile reads the provided file and returns a corresponding CSVData pointer
func LoadFile(filename string) (*CSVData, error) {
	delimiter := ","
	// support TSV as well
	if strings.HasSuffix(strings.ToLower(filename), ".tsv") {
		delimiter = "\t"
	}
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	data := &CSVData{}
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if data.Fields == nil {
			data.ParseFieldNames(line, delimiter)
			data.Items = make(ItemDataList, 0, 2048)
			continue
		}
		item, err := data.ParseLine(line, delimiter)
		if err != nil {
			return nil, err
		}
		data.Items = append(data.Items, item)
	}
	data.RowCount = len(data.Items)
	f.Close()
	return data, nil
}

// ParseCSVFieldNames parses the first line of a CSV and returns the fields list
func (d *CSVData) ParseFieldNames(input, delimiter string) {
	d.Fields = strings.Split(input, delimiter)
	d.FieldCount = len(d.Fields)
}

// ParseLine will parse a CSV line into an Item
// this is currently a basic parser and doesn't handle quote-wrapped fields
func (d *CSVData) ParseLine(input, delimiter string) (ItemData, error) {
	parts := strings.Split(input, delimiter)
	if len(parts) != d.FieldCount {
		return nil, ErrInvalidDataLine
	}
	item := make(ItemData, d.FieldCount)
	for i, p := range parts {
		if n, err := strconv.ParseInt(p, 10, 64); err == nil {
			item[i] = n
			continue
		}
		if f, err := strconv.ParseFloat(p, 64); err == nil {
			item[i] = f
			continue
		}
		// as ParseBool comes after ParseInt, this will only capture
		// t, T, true, True, TRUE, f, F, false, False, and FALSE
		if b, err := strconv.ParseBool(p); err == nil {
			item[i] = b
			continue
		}
		// else use the initial string
		item[i] = p
	}
	return item, nil
}

// String outputs the CSVData in CSV format
func (d *CSVData) String() string {
	if d.Delimiter == "" {
		d.Delimiter = ","
	}
	var sb strings.Builder
	sb.WriteString(strings.Join(d.Fields, d.Delimiter))
	sb.WriteString("\n")
	for i, item := range d.Items {
		for j, field := range item {
			// wrap string values containing commas with double quotes
			if d.Delimiter == "," {
				if s, ok := field.(string); ok && strings.Contains(s, d.Delimiter) {
					field = fmt.Sprintf(`"%s"`, s)
				}
			}
			sb.WriteString(fmt.Sprintf("%v", field))
			if j < d.FieldCount-1 {
				sb.WriteString(d.Delimiter)
			}
		}
		if i < d.RowCount-1 {
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

// ItemDataToItem returns an Item (map) from an ItemData (slice)
func (d *CSVData) ItemDataToItem(i ItemData) (Item, error) {
	if len(i) != d.FieldCount {
		return nil, ErrInvalidDataLine
	}
	item := make(Item)
	for j, v := range i {
		item[d.Fields[j]] = v
	}
	return item, nil
}
