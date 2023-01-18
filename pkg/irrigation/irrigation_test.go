package irrigation

import (
	"context"
	"fmt"
	"letsplay-dynamodb/pkg/csv"
	"letsplay-dynamodb/pkg/dynamodb"
	"letsplay-dynamodb/pkg/loader"
	"letsplay-dynamodb/pkg/session"
	"letsplay-dynamodb/pkg/uuid"
	"testing"
)

const (
	irrigationsTableName = "irrigations"

	fileExampleIrrigationHistory = "../../data/example_irrigation_history.tsv"

	// you will need to update this as needed once data has been loaded
	// since the UUIDs are generated on-the-fly at load tine
	testID = "b75b990c-3398-408e-b485-14f42da4eb5e"
)

// TestLoadIrrigationData loads the CSV data into the example irrigations table
func TestLoadIrrigationData(t *testing.T) {
	data, err := csv.LoadFile(fileExampleIrrigationHistory)
	if err != nil {
		t.Error(err)
	}

	sess, err := session.NewSession()
	if err != nil {
		t.Error(err)
	}

	// since there is no id field in the source data, this adds a UUID
	// to each item before performing the batch load
	data.Fields = append(data.Fields, "id")
	data.FieldCount++
	for i := range data.Items {
		data.Items[i] = append(data.Items[i], uuid.NewString())
	}

	err = loader.LoadCSVDataToDynamoDB(sess, context.Background(),
		irrigationsTableName, data)
	if err != nil {
		t.Error(err)
	}
}

// TestQueryIrrigationDataByLocation executes a query for records in a single location
// and returns the results in reverse-chronological order
func TestQueryIrrigationDataByLocation(t *testing.T) {
	sess, err := session.NewSession()
	if err != nil {
		t.Error(err)
	}

	input := dynamodb.NewItemInput(irrigationsTableName, "location", "", "Front Yard", nil)
	input = input.WithIndex("LocationReportTimeIndex")
	result, err := input.Query(sess, 10, -1, true)
	if err != nil {
		t.Error(err)
	}
	if len(result.Items) == 0 {
		return
	}
	fmt.Println("report_time,location,duration")
	for _, item := range result.Items {
		var event IrrigationEvent
		err = dynamodb.UnmarshalMap(item, &event)
		if err != nil {
			t.Error(err)
		}
		fmt.Printf("%s,%s,%d,%s\n", event.ReportTime, event.Location, event.IrrigationDuration, event.ID)
	}

}

// TestGetIrrigationItemByID executes a GetItem request for a single record
func TestGetIrrigationItemByID(t *testing.T) {
	sess, err := session.NewSession()
	if err != nil {
		t.Error(err)
	}

	// Option 1: Provide the full Primary Key and corresponding Values
	result, err := dynamodb.GetItem(sess, irrigationsTableName,
		[]string{"id"}, []interface{}{testID})
	if err != nil {
		t.Error(err)
	}

	var event1, event2 IrrigationEvent
	err = dynamodb.UnmarshalMap(result.Item, &event1)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("%s,%s,%d,%s\n", event1.ReportTime, event1.Location,
		event1.IrrigationDuration, event1.ID)

	fmt.Println()
	fmt.Println()

	// Option 2: you can use GetItemByID if the PK is just an id field
	result, err = dynamodb.GetItemByID(sess, irrigationsTableName, testID)
	if err != nil {
		t.Error(err)
	}
	err = dynamodb.UnmarshalMap(result.Item, &event2)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("%s,%s,%d,%s\n", event2.ReportTime, event2.Location,
		event2.IrrigationDuration, event2.ID)

}

func TestMassiveLoadIrrigationData(t *testing.T) {
	for i := 0; i < 100; i++ {
		TestLoadIrrigationData(t)
	}
}

func TestTruncateIrrigationsTable(t *testing.T) {
	sess, err := session.NewSession()
	if err != nil {
		t.Error(err)
	}

	out, err := dynamodb.ScanTable(sess, irrigationsTableName, 0, -1)
	if err != nil {
		t.Error(err)
	}

	if len(out.Items) == 0 {
		return
	}

	items := make(dynamodb.Items, 0, len(out.Items))
	for _, item := range out.Items {
		if val, ok := item["id"]; ok {
			items = append(items, dynamodb.Item{"id": val})
		}
	}

	_, errs := dynamodb.WriteBatch(context.Background(), sess, irrigationsTableName,
		nil, items)

	if len(errs) > 0 {
		panic(errs[0])
	}

}
