package dbsql

import (
	"context"
)

type contextKey string

const (
	openSessionHook       contextKey = "OPEN_SESSION_HOOK"
	operationMetadataHook contextKey = "OPERATION_METADATA_HOOK"
)

// WithOpenSessionHook registers a callback that will be executed with the
// Databricks session ID as input when a session is acquired for running a query,
// whether by reusing a cached session ID or by creating a new session.
func WithOpenSessionHook(
	ctx context.Context,
	fn func(string),
) context.Context {
	return context.WithValue(ctx, openSessionHook, fn)
}

func callOpenSessionHook(ctx context.Context, sessionId string) {
	callContextHook(ctx, openSessionHook, sessionId)
}

type OperationMetadata interface {
	GetOperationId() string
	HasResultSet() bool
	RowsAffected() float64
}

// WithOperationMetadataHook registers a callback that will be executed after an
// ExecuteStatement thrift request.
func WithOperationMetadataHook(
	ctx context.Context,
	fn func(OperationMetadata),
) context.Context {
	return context.WithValue(ctx, operationMetadataHook, fn)
}

func callOperationMetadataHook(ctx context.Context, metadata OperationMetadata) {
	callContextHook(ctx, operationMetadataHook, metadata)
}

func callContextHook[T any](ctx context.Context, key contextKey, input T) {
	val := ctx.Value(key)
	if val == nil {
		return
	}
	fn, ok := val.(func(T))
	if !ok {
		return
	}
	fn(input)
}
