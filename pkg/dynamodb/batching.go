package dynamodb

import (
	"context"
	"encoding/json"
	"letsplay-dynamodb/pkg/session"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// MaxBatchItems represents the maximum # of items allowed in a batch by AWS
const MaxBatchItemCount = 25

// MaxBatchItems is the maximum byte size of data allowed in a batch by AWS.
const MaxBatchByteSize = 16 * 1024 * 1024

type writeBatchState struct {
	ctx          context.Context
	mtx          sync.Mutex
	wg           sync.WaitGroup
	sess         session.Session
	batchSize    int
	outputs      []*dynamodb.BatchWriteItemOutput
	pendingItems []types.WriteRequest
	tableName    string
	errors       []error
}

// WriteBatch writes or deletes the provided items in the provided tableName.
// Items for deletion should only include their Primary Key fields.
func WriteBatch(ctx context.Context, sess session.Session, tableName string,
	puts Items, deletes Items) ([]*dynamodb.BatchWriteItemOutput, []error) {
	if tableName == "" || (len(puts) == 0 && len(deletes) == 0) {
		return nil, nil
	}
	connect(sess)
	// this object keeps track of the queue state during sub-batch processing
	state := &writeBatchState{
		ctx:  ctx,
		sess: sess,
		// initial size is the calc'ed number of sub-batches based on item #
		// but will auto resize if the bytes-based max batch size is exceeded
		outputs: make([]*dynamodb.BatchWriteItemOutput, 0,
			((len(puts)+len(deletes))/MaxBatchItemCount)+1),
		pendingItems: make([]types.WriteRequest, 0, MaxBatchItemCount),
		// will auto-resize after 25 errors are appended
		errors:    make([]error, 0, MaxBatchItemCount),
		tableName: tableName,
	}

	for _, item := range puts {
		processItem(state, item, false)
	}
	for _, item := range deletes {
		processItem(state, item, true)
	}
	// send up any remaining items
	if len(state.pendingItems) > 0 {
		state.wg.Add(1)
		go enqueueWriteBatchPart(state, state.pendingItems)
	}
	state.wg.Wait()

	return state.outputs, state.errors
}

func processItem(state *writeBatchState, item Item, isDelete bool) {
	// this gets the size of the marshaled item to assist with sub-batching
	b, err := json.Marshal(item)
	if err != nil {
		state.errors = append(state.errors, err)
		return
	}
	// if MaxBatchByteSize would be exceeded by adding this record, send
	// up the current batch for processing via a simple queue and reset
	// the pendingItems slice before adding the record
	if len(b)+state.batchSize > MaxBatchByteSize {
		state.wg.Add(1)
		go enqueueWriteBatchPart(state, state.pendingItems)
		state.pendingItems = make([]types.WriteRequest, 0, MaxBatchItemCount)
	}
	if isDelete {
		state.pendingItems = append(state.pendingItems, types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{
				Key: item,
			},
		})
	} else {
		state.pendingItems = append(state.pendingItems, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: item,
			},
		})
	}
	// if MaxBatchItemCount is met by adding this record, send
	// up the current batch for processing via a simple queue and reset
	// the pendingItems slice
	if len(state.pendingItems) == MaxBatchItemCount {
		state.wg.Add(1)
		go enqueueWriteBatchPart(state, state.pendingItems)
		state.pendingItems = make([]types.WriteRequest, 0, MaxBatchItemCount)
	}
}

// enqueueWriteBatchPart ensures that only one batch part is in-flight to
// dynamo at any given time, while allowing the calling func to prepare the
// next batch at the same time
// requests is passed separately so as to allow the calling func to reset the
// state's pending requests while the Batch Write is happening concurrently
func enqueueWriteBatchPart(state *writeBatchState, requests []types.WriteRequest) {
	if len(requests) == 0 {
		state.wg.Done()
		return
	}
	// since the func is only called from within this pkg,
	// it should be safe to assume that state is non-nil
	state.mtx.Lock()
	out, err := client.BatchWriteItem(state.ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			state.tableName: requests,
		},
	})
	if err != nil {
		state.errors = append(state.errors, err)
	}
	state.outputs = append(state.outputs, out)
	state.mtx.Unlock()
	state.wg.Done()
}
