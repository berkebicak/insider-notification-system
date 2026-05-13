package handlers

import (
	"github.com/bicak/notification-system/internal/models"
	"github.com/bicak/notification-system/internal/templates"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

type TemplateHandler struct {
	svc *templates.Service
}

func NewTemplateHandler(svc *templates.Service) *TemplateHandler {
	return &TemplateHandler{svc: svc}
}

type CreateTemplateRequest struct {
	Name    string         `json:"name"`
	Channel models.Channel `json:"channel"`
	Content string         `json:"content"`
}

// CreateTemplate godoc
// @Summary Create a message template
// @Tags templates
// @Accept json
// @Produce json
// @Param body body CreateTemplateRequest true "Template"
// @Success 201 {object} models.Template
// @Router /api/v1/templates [post]
func (h *TemplateHandler) Create(c *fiber.Ctx) error {
	var req CreateTemplateRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid body"})
	}
	if req.Name == "" || req.Content == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "name and content are required"})
	}
	if req.Channel != models.ChannelSMS && req.Channel != models.ChannelEmail && req.Channel != models.ChannelPush {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "channel must be one of: sms, email, push"})
	}

	t, err := h.svc.Create(c.Context(), req.Name, req.Channel, req.Content)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	return c.Status(fiber.StatusCreated).JSON(t)
}

// ListTemplates godoc
// @Summary List all templates
// @Tags templates
// @Produce json
// @Success 200 {array} models.Template
// @Router /api/v1/templates [get]
func (h *TemplateHandler) List(c *fiber.Ctx) error {
	list, err := h.svc.List(c.Context())
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to list templates"})
	}
	if list == nil {
		list = []*models.Template{}
	}
	return c.JSON(list)
}

// GetTemplate godoc
// @Summary Get template by ID
// @Tags templates
// @Produce json
// @Param id path string true "Template ID"
// @Success 200 {object} models.Template
// @Router /api/v1/templates/{id} [get]
func (h *TemplateHandler) Get(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid id"})
	}

	t, err := h.svc.Get(c.Context(), id)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "template not found"})
	}

	return c.JSON(t)
}
