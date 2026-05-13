package templates

import (
	"bytes"
	"context"
	"fmt"
	"text/template"
	"time"

	"github.com/bicak/notification-system/internal/models"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Service struct {
	db *pgxpool.Pool
}

func NewService(db *pgxpool.Pool) *Service {
	return &Service{db: db}
}

func (s *Service) Create(ctx context.Context, name string, channel models.Channel, content string) (*models.Template, error) {
	if _, err := template.New("test").Parse(content); err != nil {
		return nil, fmt.Errorf("invalid template syntax: %w", err)
	}

	t := &models.Template{
		ID:        uuid.New(),
		Name:      name,
		Channel:   channel,
		Content:   content,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	_, err := s.db.Exec(ctx,
		`INSERT INTO templates (id, name, channel, content) VALUES ($1, $2, $3, $4)`,
		t.ID, t.Name, t.Channel, t.Content,
	)
	if err != nil {
		return nil, fmt.Errorf("insert template: %w", err)
	}

	return t, nil
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (*models.Template, error) {
	t := &models.Template{}
	err := s.db.QueryRow(ctx,
		`SELECT id, name, channel, content, created_at, updated_at FROM templates WHERE id = $1`,
		id,
	).Scan(&t.ID, &t.Name, &t.Channel, &t.Content, &t.CreatedAt, &t.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (s *Service) List(ctx context.Context) ([]*models.Template, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, name, channel, content, created_at, updated_at FROM templates ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*models.Template
	for rows.Next() {
		t := &models.Template{}
		if err := rows.Scan(&t.ID, &t.Name, &t.Channel, &t.Content, &t.CreatedAt, &t.UpdatedAt); err != nil {
			continue
		}
		result = append(result, t)
	}
	return result, nil
}

// Render applies the stored template with the provided variables.
func (s *Service) Render(ctx context.Context, templateID uuid.UUID, vars map[string]string) (string, error) {
	t, err := s.Get(ctx, templateID)
	if err != nil {
		return "", fmt.Errorf("get template: %w", err)
	}

	tmpl, err := template.New(t.Name).Option("missingkey=error").Parse(t.Content)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, vars); err != nil {
		return "", fmt.Errorf("render template: %w", err)
	}

	return buf.String(), nil
}
