/*
 * Copyright (c) 2019 VMware, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
// The implementation is derived from https://github.com/patrobinson/gokini
//
// Copyright 2018 Patrick robinson
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
package checkpoint

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	cfg "github.com/vmware/vmware-go-kcl/clientlibrary/config"
	par "github.com/vmware/vmware-go-kcl/clientlibrary/partition"
)

const (
	FAILOVER_TIME_MILLIS = 30000
	SYNC_INTERVAL_MILLIS = 5000

	SHARD_ID = "0001"
)

func TestTableDoesNotExist(t *testing.T) {
	dynamo := newDynamoClient(t)
	checkpoint := newTestSubject(t, dynamo)

	if checkpoint.doesTableExist() {
		t.Error("Table does not exist but returned true")
	}
}

func TestTableCreatedOnInit(t *testing.T) {
	dynamo := newDynamoClient(t)
	checkpoint := newTestSubject(t, dynamo)

	err := checkpoint.Init("test-worker")
	assert.Nil(t, err)

	if !checkpoint.doesTableExist() {
		t.Error("Table exists but returned false")
	}
}

func TestLeaseAcquiredOnEmptyTable(t *testing.T) {
	dynamo := newDynamoClient(t)
	checkpoint := newTestSubject(t, dynamo)
	checkpoint.Init("test-worker")

	err := checkpoint.GetLease(&par.ShardStatus{
		ID:         SHARD_ID,
		Checkpoint: "",
		Mux:        &sync.Mutex{},
	})
	assert.Nil(t, err, "Did not get lease on empty table")
}

func TestLeaseAcquiredOnExpiredExistingRow(t *testing.T) {
	dynamo := newDynamoClient(t)
	checkpoint := newTestSubject(t, dynamo)
	checkpoint.Init("test-worker")

	dynamo.createInitialStateRecord(
		t,
		SHARD_ID,
		"",
		"other-worker",
		time.Now().Add(-1*time.Second),
	)

	err := checkpoint.GetLease(&par.ShardStatus{
		ID:         SHARD_ID,
		Checkpoint: "",
		Mux:        &sync.Mutex{},
	})
	assert.Nil(t, err, "Did not get lease on top of expired record")
}

func TestLeaseNotAcquiredOnCurrentExistingRow(t *testing.T) {
	dynamo := newDynamoClient(t)
	checkpoint := newTestSubject(t, dynamo)
	checkpoint.Init("test-worker")

	dynamo.createInitialStateRecord(
		t,
		SHARD_ID,
		"",
		"other-worker",
		time.Now().Add(0.5*FAILOVER_TIME_MILLIS*time.Millisecond),
	)

	err := checkpoint.GetLease(&par.ShardStatus{
		ID:         SHARD_ID,
		Checkpoint: "",
		Mux:        &sync.Mutex{},
	})
	assert.Equal(t, ErrLeaseNotAquired, err, "Was granted lease which was already owned")
}

func TestLeaseNotAcquiredByCompetingCheckpointer(t *testing.T) {
	dynamo := newDynamoClient(t)
	checkpoint := newTestSubject(t, dynamo)
	checkpoint.Init("test-worker")

	err := checkpoint.GetLease(&par.ShardStatus{
		ID:         SHARD_ID,
		Checkpoint: "",
		Mux:        &sync.Mutex{},
	})
	assert.Nil(t, err)

	otherCheckpoint := newTestSubject(t, dynamo)
	otherCheckpoint.Init("other-worker")
	err = otherCheckpoint.GetLease(&par.ShardStatus{
		ID:         SHARD_ID,
		Checkpoint: "",
		Mux:        &sync.Mutex{},
	})
	assert.Equal(t, ErrLeaseNotAquired, err, "Was granted lease which was already owned")
}

func TestLeaseCanBeRelinquished(t *testing.T) {
	dynamo := newDynamoClient(t)
	checkpoint := newTestSubject(t, dynamo)
	checkpoint.Init("test-worker")

	// take the lease
	err := checkpoint.GetLease(&par.ShardStatus{
		ID:         SHARD_ID,
		Checkpoint: "",
		Mux:        &sync.Mutex{},
	})
	assert.Nil(t, err)

	// give up the lease
	err = checkpoint.RemoveLeaseOwner(SHARD_ID)
	assert.Nil(t, err)

	// lease should be available to another checkpointer
	otherCheckpoint := newTestSubject(t, dynamo)
	otherCheckpoint.Init("other-worker")
	err = otherCheckpoint.GetLease(&par.ShardStatus{
		ID:         SHARD_ID,
		Checkpoint: "",
		Mux:        &sync.Mutex{},
	})
	assert.Nil(t, err)
}

func TestCannotRelinquishAnotherWorkersLease(t *testing.T) {
	dynamo := newDynamoClient(t)
	otherCheckpoint := newTestSubject(t, dynamo)
	otherCheckpoint.Init("other-worker")

	// the other worker already holds the lease
	err := otherCheckpoint.GetLease(&par.ShardStatus{
		ID:         SHARD_ID,
		Checkpoint: "",
		Mux:        &sync.Mutex{},
	})
	assert.Nil(t, err)

	checkpoint := newTestSubject(t, dynamo)
	checkpoint.Init("test-worker")

	// try to give up the lease when we don't own it
	err = checkpoint.RemoveLeaseOwner(SHARD_ID)
	assert.NotNil(t, err, "Removing another lease owner did not fail")

	// ensure that the lease still belongs to the other worker and that
	// we still can't take it ourselves
	err = checkpoint.GetLease(&par.ShardStatus{
		ID:         SHARD_ID,
		Checkpoint: "",
		Mux:        &sync.Mutex{},
	})
	assert.Equal(t, ErrLeaseNotAquired, err, "Was granted lease which should not have been available")
}

func newTestSubject(t *testing.T, dynamo *testDynamoClient) *DynamoCheckpoint {
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "local", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(SYNC_INTERVAL_MILLIS).
		WithFailoverTimeMillis(FAILOVER_TIME_MILLIS).
		WithTableName(dynamo.tableName)

	return NewDynamoCheckpoint(kclConfig).WithDynamoDB(dynamo)
}

func newDynamoClient(t *testing.T) *testDynamoClient {
	s, err := session.NewSession(&aws.Config{
		Region:      aws.String("local"),
		Endpoint:    aws.String("dynamo:8000"),
		Credentials: credentials.NewStaticCredentials("blah", "blah", "blah"),
		MaxRetries:  aws.Int(0),
		DisableSSL:  aws.Bool(true),
	})

	if err != nil {
		// no need to move forward
		t.Fatalf("Failed in getting DynamoDB session for test")
	}

	return &testDynamoClient{
		DynamoDBAPI: dynamodb.New(s),
		tableName:   t.Name(),
	}
}

type testDynamoClient struct {
	dynamodbiface.DynamoDBAPI
	tableName string
}

func (d *testDynamoClient) createInitialStateRecord(
	t *testing.T,
	shardID string,
	checkpoint string,
	assignedTo string,
	leaseTimeout time.Time,
) {
	marshalled := map[string]*dynamodb.AttributeValue{
		LEASE_KEY_KEY: {
			S: aws.String(shardID),
		},
		LEASE_OWNER_KEY: {
			S: aws.String(assignedTo),
		},
		LEASE_TIMEOUT_KEY: {
			S: aws.String(leaseTimeout.UTC().Format(time.RFC3339)),
		},
		CHECKPOINT_SEQUENCE_NUMBER_KEY: {
			S: aws.String(checkpoint),
		},
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item:      marshalled,
	}
	_, err := d.PutItem(input)
	assert.Nil(t, err)
}
