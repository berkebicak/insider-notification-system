package tracing

import (
	"context"
	"fmt"
	"os"

	"github.com/bicak/notification-system/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func Init(ctx context.Context, cfg config.TracingConfig) (func(context.Context) error, error) {
	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}

	var (
		exporter sdktrace.SpanExporter
		err      error
	)
	if cfg.OTLPEndpoint != "" {
		exporter, err = otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(cfg.OTLPEndpoint), otlptracehttp.WithInsecure())
	} else {
		exporter, err = stdouttrace.New(stdouttrace.WithWriter(os.Stdout))
	}
	if err != nil {
		return nil, fmt.Errorf("create trace exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(cfg.ServiceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("create trace resource: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)

	return provider.Shutdown, nil
}
