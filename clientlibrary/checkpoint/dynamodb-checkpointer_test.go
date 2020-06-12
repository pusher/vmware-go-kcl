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

func TestDoesTableExist(t *testing.T) {
	checkpoint := newTestSubject(t)

	if checkpoint.doesTableExist() {
		t.Error("Table does not exist but returned true")
	}

	err := checkpoint.Init("test-worker")
	assert.Nil(t, err)

	if !checkpoint.doesTableExist() {
		t.Error("Table exists but returned false")
	}
}

func TestGetLeaseNotAquired(t *testing.T) {
	checkpoint := newTestSubject(t)
	checkpoint.createTable()
	checkpoint.Init("abcd-efgh")
	err := checkpoint.GetLease(&par.ShardStatus{
		ID:         "0001",
		Checkpoint: "",
		Mux:        &sync.Mutex{},
	})
	if err != nil {
		t.Errorf("Error getting lease %s", err)
	}

	checkpoint2 := newTestSubject(t)
	checkpoint2.Init("ijkl-mnop")
	err = checkpoint2.GetLease(&par.ShardStatus{
		ID:         "0001",
		Checkpoint: "",
		Mux:        &sync.Mutex{},
	})
	if err == nil || err != ErrLeaseNotAquired {
		t.Errorf("Got a lease when it was already held by abcd-efgh: %s", err)
	}
}

func TestGetLeaseAquired(t *testing.T) {
	existingWorkerID := "abcd-efgh"
	checkpoint := newTestSubject(t)
	thisWorkerID := "ijkl-mnop"
	checkpoint.Init(thisWorkerID)

	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		"ShardID": {
			S: aws.String("0001"),
		},
		"AssignedTo": {
			S: aws.String(existingWorkerID),
		},
		"LeaseTimeout": {
			S: aws.String(time.Now().AddDate(0, -1, 0).UTC().Format(time.RFC3339)),
		},
		"SequenceID": {
			S: aws.String("deadbeef"),
		},
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String(checkpoint.kclConfig.TableName),
		Item:      marshalledCheckpoint,
	}
	_, err := checkpoint.svc.PutItem(input)
	assert.Nil(t, err)

	shard := &par.ShardStatus{
		ID:         "0001",
		Checkpoint: "deadbeef",
		Mux:        &sync.Mutex{},
	}
	err = checkpoint.GetLease(shard)

	if err != nil {
		t.Errorf("Lease not aquired after timeout %s", err)
	}

	status := &par.ShardStatus{
		ID:  shard.ID,
		Mux: &sync.Mutex{},
	}
	checkpoint.FetchCheckpoint(status)
	assert.Equal(t, "deadbeef", status.Checkpoint)

	// release owner info
	err = checkpoint.RemoveLeaseOwner(shard.ID)
	assert.Nil(t, err)

	status = &par.ShardStatus{
		ID:  shard.ID,
		Mux: &sync.Mutex{},
	}
	checkpoint.FetchCheckpoint(status)

	// checkpointer and parent shard id should be the same
	assert.Equal(t, shard.Checkpoint, status.Checkpoint)
	assert.Equal(t, shard.ParentShardId, status.ParentShardId)

	// Only the lease owner has been wiped out
	assert.Equal(t, "", status.GetLeaseOwner())
}

func newTestSubject(t *testing.T) *DynamoCheckpoint {
	svc := localDynamoClient(t)

	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "local", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithTableName(t.Name())

	return NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
}

func localDynamoClient(t *testing.T) dynamodbiface.DynamoDBAPI {
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

	return dynamodb.New(s)
}
