package loader

import (
	"context"
	"letsplay-dynamodb/pkg/csv"
	"letsplay-dynamodb/pkg/dynamodb"
	"letsplay-dynamodb/pkg/session"
)

func LoadCSVDataToDynamoDB(sess session.Session, ctx context.Context,
	tableName string, data *csv.CSVData) error {
	if len(data.Items) == 0 {
		return nil
	}

	items := make(dynamodb.Items, len(data.Items))
	for i, csvItem := range data.Items {
		item, err := data.ItemDataToItem(csvItem)
		if err != nil {
			return err
		}
		items[i] = dynamodb.Attributes(item).ToItem()
	}

	_, errs := dynamodb.WriteBatch(context.Background(), sess, tableName, items, nil)
	if len(errs) > 0 && errs[0] != nil {
		// currently just returns the first error if there are more than one
		return errs[0]
	}

	return nil
}
