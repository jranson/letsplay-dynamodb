// package dynamodb provides conveniences for interacting with DynamoDB
package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"letsplay-dynamodb/pkg/session"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Item represents an Item in DynamoDB
type Item map[string]types.AttributeValue

// Items is a slice of type Item
type Items []map[string]types.AttributeValue

// Attributes represents a DynamoDB Attribute Map
type Attributes map[string]interface{}

// ItemInput is the base type for this package's reader/writer objects
type ItemInput struct {
	TableName         string
	PartitionKeyName  string
	PartitionKeyValue interface{}
	SortKeyName       string
	SortKeyValue      interface{}
	Attributes        Attributes
	Context           context.Context
	IndexName         string
	FilterExpression  string
	ExpressionValues  Attributes
}

var client *dynamodb.Client
var mtx sync.Mutex
var ErrInvalidInput = errors.New("invalid input")

// connect will instantiate the DynamoDB client. Safe for use with concurrency.
func connect(sess session.Session) {
	if client == nil {
		mtx.Lock()
		// once mutex is acquired, re-check for nil in case another
		// invocation was also called and acquired a lock first
		if client == nil {
			client = dynamodb.NewFromConfig(sess.Config())
		}
		mtx.Unlock()
	}
}

// NewItemInput returns a new ItemInput
func NewItemInput(tableName, partitionKeyName, sortKeyName string,
	partitionKeyValue, sortKeyValue interface{}) *ItemInput {
	i := &ItemInput{
		TableName:         tableName,
		PartitionKeyName:  partitionKeyName,
		PartitionKeyValue: partitionKeyValue,
		Attributes:        make(Attributes),
		Context:           context.Background(),
		ExpressionValues:  make(Attributes),
	}
	if sortKeyName != "" {
		i.SortKeyName = sortKeyName
		i.SortKeyValue = sortKeyValue
	}
	return i
}

func getAttributeValue(input interface{}) types.AttributeValue {
	if input == nil {
		return nil
	}
	switch t := input.(type) {
	case string:
		return &types.AttributeValueMemberS{
			Value: t,
		}
	case int:
		return &types.AttributeValueMemberN{
			Value: strconv.Itoa(t),
		}
	case int64:
		return &types.AttributeValueMemberN{
			Value: strconv.FormatInt(t, 10),
		}
	case float32:
		return &types.AttributeValueMemberN{
			Value: strconv.FormatFloat(float64(t), 'f', -1, 32),
		}
	case float64:
		return &types.AttributeValueMemberN{
			Value: strconv.FormatFloat(float64(t), 'f', -1, 64),
		}
		// TODO: support maps, lists and other complex types
	}
	return nil
}

// ToItem creates a DynamoDB item from a generic Attributes map
func (a Attributes) ToItem() Item {
	i := make(Item)
	for k, v := range a {
		av := getAttributeValue(v)
		if av != nil {
			i[k] = av
		}
	}
	return i
}

// Merge merges the elements in the passed Item into the base Item
func (i Item) Merge(i2 Item) {
	for k, v := range i2 {
		i[k] = v
	}
}

// GetKey returns an Item containing only the key components of the Input
func (i *ItemInput) GetKey() Item {
	key := make(Item)
	if av := getAttributeValue(i.PartitionKeyValue); av != nil {
		key[i.PartitionKeyName] = av
	}
	if av := getAttributeValue(i.SortKeyValue); av != nil {
		key[i.SortKeyName] = av
	}
	return key
}

// GetPuttableItem returns a version of the Input that is suitable for a PUT
// operation (when the Partition and Sort keys are part of the Item attributes)
func (i *ItemInput) GetPuttableItem() Item {
	var item Item
	if i.Attributes != nil {
		item = i.Attributes.ToItem()
	}
	if item == nil {
		item = i.GetKey()
	} else {
		item.Merge(i.GetKey())
	}
	return item
}

// UpdateWithObject updates the DynamoDB item using the provided
// object's exportable members for the Attributes to update
func (i *ItemInput) UpdateWithObject(sess session.Session,
	obj interface{}, recordLastModified bool) (*dynamodb.UpdateItemOutput,
	error) {
	connect(sess)
	m, err := attributevalue.MarshalMap(obj)
	if err != nil {
		return nil, err
	}
	item := Item(m)
	builder := expression.UpdateBuilder{}
	for k, v := range item {
		builder = builder.Set(expression.Name(k),
			expression.Value(v))
	}
	return i.UpdateWithBuilder(sess, &builder, recordLastModified)
}

// Update updates the DynamoDB item based on the input's Attributes
func (i *ItemInput) Update(sess session.Session,
	recordLastModified bool) (*dynamodb.UpdateItemOutput, error) {
	builder := expression.UpdateBuilder{}
	for k, v := range i.Attributes {
		builder = builder.Set(expression.Name(k),
			expression.Value(v))
	}
	return i.UpdateWithBuilder(sess, &builder, recordLastModified)
}

// UpdateWithBuilder updates the DynamoDB item using the provided
// builder for the Attributes to update
func (i *ItemInput) UpdateWithBuilder(sess session.Session,
	builder *expression.UpdateBuilder,
	recordLastModified bool) (*dynamodb.UpdateItemOutput, error) {
	connect(sess)
	if recordLastModified {
		updateLastModified(sess, builder, time.Now())
	}
	expr, err := expression.NewBuilder().WithUpdate(*builder).Build()
	if err != nil {
		return nil, err
	}
	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(i.TableName),
		Key:                       i.GetKey(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
	}
	output, err := client.UpdateItem(i.Context, input)
	if err != nil {
		return output, err
	}
	return output, nil
}

// CreateWithObject creates a DynamoDB item using the provided
// object's exportable members for the Attributes to include
// This operation will fail if the item key already exists
func (i *ItemInput) CreateWithObject(sess session.Session,
	obj interface{}) (*dynamodb.PutItemOutput, error) {
	dam, err := attributevalue.MarshalMap(obj)
	if err != nil {
		return nil, err
	}
	item := Item(dam)
	return i.CreateWithItem(sess, item)
}

// CreateWithItem creates a DynamoDB item based on the provided Item Attributes
// This operation will fail if the item key already exists
func (i *ItemInput) CreateWithItem(sess session.Session,
	item Item) (*dynamodb.PutItemOutput, error) {
	connect(sess)
	item.Merge(i.GetKey())

	putInput := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(i.TableName),
	}
	var condition string
	if i.SortKeyName == "" {
		condition = "attribute_not_exists(#hk)"
		putInput.ExpressionAttributeNames =
			map[string]string{"#hk": i.PartitionKeyName}
	} else {
		condition =
			"(attribute_not_exists(#hk)) AND (attribute_not_exists(#sk))"
		putInput.ExpressionAttributeNames =
			map[string]string{"#hk": i.PartitionKeyName, "#sk": i.SortKeyName}
	}
	putInput.ConditionExpression = &condition

	out, err := client.PutItem(i.Context, putInput)
	if err != nil {
		return out, err
	}
	return out, nil
}

// Create creates the DynamoDB item based on the input's Attributes
// This operation will fail if the item key already exists
func (i *ItemInput) Create(sess session.Session) (*dynamodb.PutItemOutput,
	error) {
	connect(sess)
	return i.CreateWithItem(sess, i.GetPuttableItem())
}

// UpsertWithObject creates or updates the DynamoDB item using the provided
// object's exportable members for the Attributes to include
func (i *ItemInput) UpsertWithObject(sess session.Session,
	obj interface{}) (*dynamodb.PutItemOutput, error) {
	connect(sess)

	dam, err := attributevalue.MarshalMap(obj)
	if err != nil {
		return nil, err
	}
	item := Item(dam)
	item.Merge(i.GetKey())
	putInput := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(i.TableName),
	}
	out, err := client.PutItem(i.Context, putInput)
	if err != nil {
		return out, err
	}
	return out, nil
}

// Upsert creates or updates the DynamoDB item using the input's Attributes
func (i *ItemInput) Upsert(sess session.Session) (
	*dynamodb.PutItemOutput, error) {
	connect(sess)
	putInput := &dynamodb.PutItemInput{
		Item:      i.GetPuttableItem(),
		TableName: aws.String(i.TableName),
	}
	out, err := client.PutItem(i.Context, putInput)
	if err != nil {
		return out, err
	}
	return out, nil
}

// Delete removes the DynamoDB item from the input table
func (i *ItemInput) Delete(
	sess session.Session) (*dynamodb.DeleteItemOutput, error) {
	connect(sess)
	input := &dynamodb.DeleteItemInput{
		Key:       i.GetKey(),
		TableName: aws.String(i.TableName),
	}
	out, err := client.DeleteItem(i.Context, input)
	if err != nil {
		return out, err
	}
	return out, nil

}

// Get retrieves a single Item from DynamoDB based on the Input
func (i *ItemInput) Get(sess session.Session) (*dynamodb.GetItemOutput, error) {
	connect(sess)
	input := &dynamodb.GetItemInput{
		TableName: aws.String(i.TableName),
		Key:       i.GetKey(),
	}
	result, err := client.GetItem(i.Context, input)
	if err != nil {
		return nil, err
	}
	if result == nil || result.Item == nil {
		return result, fmt.Errorf("item does not exist: [%v:%v]",
			i.PartitionKeyValue, i.SortKeyValue)
	}
	return result, nil
}

// Query retrieves 0 or more Items from DynamoDB based on the Input
// results are limited to the provided itemLimit count, or the provided
// pageLimit count, whichever is smaller.
// A itemLimit <= 0 will return all records permitted in the pageLimit.
// if pageLimit == 0, 1 page is returned. if pageLimit < 0, all results returned
func (i *ItemInput) Query(sess session.Session, itemLimit, pageLimit int,
	reverse bool) (*dynamodb.QueryOutput, error) {
	connect(sess)
	var input = &dynamodb.QueryInput{
		TableName:              aws.String(i.TableName),
		KeyConditionExpression: aws.String("#hk = :hv"),
		ScanIndexForward:       aws.Bool(!reverse),
	}
	input.ExpressionAttributeNames =
		map[string]string{"#hk": i.PartitionKeyName}
	input.ExpressionAttributeValues = map[string]types.AttributeValue{
		":hv": getAttributeValue(i.PartitionKeyValue),
	}
	for k, v := range i.ExpressionValues {
		input.ExpressionAttributeValues[k] = getAttributeValue(v)
	}
	if i.IndexName != "" {
		input.IndexName = aws.String(i.IndexName)
	}
	if i.FilterExpression != "" {
		input.FilterExpression = aws.String(i.FilterExpression)
	}

	var pageCount, itemCount int
	var pagesOut *dynamodb.QueryOutput
	if pageLimit == 0 {
		pageLimit = 1
	}
	for {
		// if the previous result was paginated, set the new startKey
		if pagesOut != nil && pagesOut.LastEvaluatedKey != nil {
			input.ExclusiveStartKey = pagesOut.LastEvaluatedKey
		}
		// reduce the itemLimit by the previously-retreived itemCount
		if itemLimit > 0 {
			input.Limit = aws.Int32(int32(itemLimit - itemCount))
		}
		out, err := client.Query(i.Context, input)
		// in this case, there was an error, or only one page of results
		if err != nil || out == nil || pageLimit == 1 ||
			(pagesOut == nil && len(out.LastEvaluatedKey) == 0) {
			return out, err
		}
		// if this point is reached, there are multiple pages
		itemCount += int(out.Count)
		if pagesOut == nil {
			// instantiate pagesOut on the first iteration
			pagesOut = out
		} else {
			// otherwise, merge the new output data into the running output data
			pagesOut.ConsumedCapacity = MergeConsumedCapacity(
				pagesOut.ConsumedCapacity, out.ConsumedCapacity)
			pagesOut.Count += out.Count
			pagesOut.ScannedCount += out.ScannedCount
			pagesOut.Items = append(pagesOut.Items, out.Items...)
			pagesOut.LastEvaluatedKey = out.LastEvaluatedKey
		}
		pageCount++
		// break if itemLimit or pageLimit reached, or if the last page reached
		if (pageLimit > 1 && pageCount == pageLimit) ||
			(itemLimit > 0 && itemCount >= itemLimit) ||
			len(out.LastEvaluatedKey) == 0 {
			break
		}
	}
	return pagesOut, nil
}

// Scan performs a table scan and returns the results from DynamoDB
// results are limited to the provided itemLimit count, or the provided
// pageLimit count, whichever is smaller.
// A itemLimit <= 0 will return all records permitted in the pageLimit.
// if pageLimit == 0, 1 page is returned. if pageLimit < 0, all results returned
func (i *ItemInput) Scan(sess session.Session,
	itemLimit, pageLimit int) (*dynamodb.ScanOutput, error) {
	connect(sess)
	input := &dynamodb.ScanInput{
		TableName: &i.TableName,
	}
	if i.FilterExpression != "" {
		input.FilterExpression = aws.String(i.FilterExpression)
	}
	var pageCount, itemCount int
	var pagesOut *dynamodb.ScanOutput
	if pageLimit == 0 {
		pageLimit = 1
	}
	for {
		// if the previous result was paginated, set the new startKey
		if pagesOut != nil && pagesOut.LastEvaluatedKey != nil {
			input.ExclusiveStartKey = pagesOut.LastEvaluatedKey
		}
		// reduce the itemLimit by the previously-retreived itemCount
		if itemLimit > 0 {
			input.Limit = aws.Int32(int32(itemLimit - itemCount))
		}
		out, err := client.Scan(i.Context, input)
		// in this case, there was an error, or only one page of results
		if err != nil || out == nil || pageLimit == 1 ||
			(pagesOut == nil && len(out.LastEvaluatedKey) == 0) {
			return out, err
		}
		// if this point is reached, there are multiple pages
		itemCount += int(out.Count)
		if pagesOut == nil {
			// instantiate pagesOut on the first iteration
			pagesOut = out
		} else {
			// otherwise, merge the new output data into the running output data
			pagesOut.ConsumedCapacity = MergeConsumedCapacity(
				pagesOut.ConsumedCapacity, out.ConsumedCapacity)
			pagesOut.Count += out.Count
			pagesOut.ScannedCount += out.ScannedCount
			pagesOut.Items = append(pagesOut.Items, out.Items...)
			pagesOut.LastEvaluatedKey = out.LastEvaluatedKey
		}
		pageCount++
		// break if itemLimit or pageLimit reached, or if the last page reached
		if (pageLimit > 1 && pageCount == pageLimit) ||
			(itemLimit > 0 && itemCount >= itemLimit) ||
			len(out.LastEvaluatedKey) == 0 {
			break
		}
	}
	return pagesOut, nil
}

// WithIndex sets specified the IndexName for the Query and
// returns the updated ItemInput
func (i *ItemInput) WithIndex(indexName string) *ItemInput {
	i.IndexName = indexName
	return i
}

func updateLastModified(sess session.Session, builder *expression.UpdateBuilder,
	lastModified time.Time) {
	if !lastModified.IsZero() {
		builder.Set(expression.Name("lastModified"),
			expression.Value(lastModified.Unix()))
	}
}

// Package-level Convenience Functions

// ScanTable performs a table scan and returns all results from DynamoDB
// Use limit or maxPages to limit the result set. Page sizes are 1MB
func ScanTable(sess session.Session, tableName string,
	itemLimit, pageLimit int) (*dynamodb.ScanOutput, error) {
	i := &ItemInput{
		TableName: tableName,
		Context:   context.Background(),
	}
	return i.Scan(sess, itemLimit, pageLimit)
}

// GetItem retrieves an item from DynamoDB based on the provided keys and values
func GetItem(sess session.Session, tableName string,
	// len(keys) MUST be 1 if table does not have a sort key, or 2 if it does
	keys []string,
	vals []interface{}, // must be true: len(vals) == len(keys)
) (*dynamodb.GetItemOutput, error) {
	kl := len(keys)
	if (kl != 1 && kl != 2) || (kl != len(vals)) {
		return nil, ErrInvalidInput
	}
	input := NewItemInput(tableName, keys[0], "", vals[0], nil)
	if kl == 2 {
		input.SortKeyName = keys[1]
		input.SortKeyValue = vals[1]
	}
	return input.Get(sess)
}

// GetItemByID retrieves an item from DynamoDB assuming the table uses a
// Partition Key of 'id' with no Sort Key
func GetItemByID(sess session.Session,
	tableName, id string) (*dynamodb.GetItemOutput, error) {
	return GetItem(sess, tableName, []string{"id"}, []interface{}{id})
}

// UpdateItemByID updates the item in DynamoDB with the provided id and builder.
// The underlying table must use a Partition Key of 'id' with no Sort Key
func UpdateItemByID(sess session.Session, builder *expression.UpdateBuilder,
	id, tableName string, setLastModified bool) error {
	connect(sess)
	if builder == nil {
		builder = &expression.UpdateBuilder{}
	}
	input := NewItemInput(tableName, "id", "", id, nil)
	_, err := input.UpdateWithBuilder(sess, builder, setLastModified)
	if err != nil {
		return err
	}
	return nil
}

// Unmarshal is a passthrough to attributevalue.UnmarshalMap so downstream users
// of this package do not have to directly import the AWS DynamoDB package too
func UnmarshalMap(item Item, dest interface{}) error {
	return attributevalue.UnmarshalMap(item, dest)
}

// Marshal is a passthrough to attributevalue.MarshalMap so downstream users
// of this package do not have to directly import the AWS DynamoDB package too
func MarshalMap(in interface{}) (Item, error) {
	return attributevalue.MarshalMap(in)
}

// MergeConsumedCapacity merges and aggregates the members c2 into c1
// integrities of inputs are not guaranteed post-merge; use the result only
func MergeConsumedCapacity(c1,
	c2 *types.ConsumedCapacity) *types.ConsumedCapacity {
	if c1 == nil {
		return c2
	}
	if c2 == nil {
		return c1
	}

	if c1.CapacityUnits == nil {
		c1.CapacityUnits = c2.CapacityUnits
	} else {
		c1.CapacityUnits = aws.Float64(*c1.CapacityUnits + *c2.CapacityUnits)
	}

	if c1.ReadCapacityUnits == nil {
		c1.ReadCapacityUnits = c2.ReadCapacityUnits
	} else {
		c1.ReadCapacityUnits =
			aws.Float64(*c1.ReadCapacityUnits + *c2.ReadCapacityUnits)
	}

	if c1.WriteCapacityUnits == nil {
		c1.WriteCapacityUnits = c2.WriteCapacityUnits
	} else {
		c1.WriteCapacityUnits =
			aws.Float64(*c1.WriteCapacityUnits + *c2.WriteCapacityUnits)
	}

	c1.GlobalSecondaryIndexes = MergeCapacityMap(c1.GlobalSecondaryIndexes,
		c2.GlobalSecondaryIndexes)

	c1.LocalSecondaryIndexes = MergeCapacityMap(c1.LocalSecondaryIndexes,
		c2.LocalSecondaryIndexes)

	return c1
}

// MergeCapacity merges and aggregates the members c2 into c1
// integrities of inputs are not guaranteed post-merge; use the return val
func MergeCapacity(c1, c2 *types.Capacity) *types.Capacity {
	if c1 == nil {
		return c2
	}
	if c2 == nil {
		return c1
	}
	if c1.CapacityUnits == nil {
		c1.CapacityUnits = c2.CapacityUnits
	} else {
		c1.CapacityUnits = aws.Float64(*c1.CapacityUnits + *c2.CapacityUnits)
	}
	if c1.ReadCapacityUnits == nil {
		c1.ReadCapacityUnits = c2.ReadCapacityUnits
	} else {
		c1.ReadCapacityUnits =
			aws.Float64(*c1.ReadCapacityUnits + *c2.ReadCapacityUnits)
	}
	if c1.WriteCapacityUnits == nil {
		c1.WriteCapacityUnits = c2.WriteCapacityUnits
	} else {
		c1.WriteCapacityUnits =
			aws.Float64(*c1.WriteCapacityUnits + *c2.WriteCapacityUnits)
	}
	return c1
}

// MergeCapacityMap merges and aggregates the members c2 into c1
// integrities of inputs are not guaranteed post-merge; use the return val
func MergeCapacityMap(c1,
	c2 map[string]types.Capacity) map[string]types.Capacity {
	if len(c2) == 0 {
		return c1
	}
	if len(c1) == 0 {
		return c2
	}
	for k, v := range c1 {
		if v2, ok := c2[k]; ok {
			c1[k] = *MergeCapacity(&v, &v2)
		}
	}
	return c1
}
