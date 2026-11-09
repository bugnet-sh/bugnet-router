package koyeb

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// organization holds the fields of Koyeb's organization that say whether it may start services
type organization struct {
	ID                 string `json:"id"`
	Status             string `json:"status"`
	StatusMessage      string `json:"status_message"`
	DeactivationReason string `json:"deactivation_reason"`
}

// bannedStatus lists the organization states that end a service's life or hold it locked
var bannedStatus = map[string]bool{
	"LOCKED": true, "DEACTIVATING": true, "DEACTIVATED": true, "DELETING": true, "DELETED": true,
}

// unfundedStatus lists the states that say the organization has no way to pay
var unfundedStatus = map[string]bool{"PAYMENT_FAILURE": true, "BILLING_INFO_MISSING": true}

// quotaUsage holds the counts of the organization's quota usage Koyeb caches for 60 s
type quotaUsage struct {
	ServicesUsed  flexInt `json:"services_used"`
	ServicesLimit flexInt `json:"services_limit"`
	AppsLimit     flexInt `json:"apps_limit"`
}

// organization reads the account's organization
func (p *Provider) organization(ctx context.Context) (organization, error) {
	return record[organization](ctx, p.api, false, http.MethodGet, "/v1/account/organization",
		nil, nil, "organization")
}

// organizationID is the setting, else the account's organization read once
func (p *Provider) organizationID(ctx context.Context) (string, error) {
	p.mu.Lock()
	id := p.orgID
	p.mu.Unlock()
	if id != "" {
		return id, nil
	}
	org, err := p.organization(ctx)
	if err != nil {
		return "", err
	}
	if org.ID == "" {
		return "", internal(fmt.Errorf("koyeb organization reply holds no id"))
	}
	p.mu.Lock()
	p.orgID = org.ID
	p.mu.Unlock()
	return org.ID, nil
}

// ReadLimits reads the organization's quota, each sandbox being one app and one service
func (p *Provider) ReadLimits(ctx context.Context) (provider.Limits, error) {
	id, err := p.organizationID(ctx)
	if err != nil {
		return provider.Limits{}, err
	}
	var reply struct {
		quotaUsage
		Usage quotaUsage `json:"usage"`
	}
	route := "/v1/quotas/organizations/" + url.PathEscape(id) + "/usage"
	if err := p.api.send(ctx, false, http.MethodGet, route, nil, nil, &reply); err != nil {
		return provider.Limits{}, err
	}
	q := reply.quotaUsage
	if reply.Usage != (quotaUsage{}) {
		q = reply.Usage
	}
	capacity := int(q.ServicesLimit)
	if apps := int(q.AppsLimit); apps > 0 && (capacity == 0 || apps < capacity) {
		capacity = apps
	}
	return provider.Limits{MaxConcurrent: capacity, Active: int(q.ServicesUsed)}, nil
}

// CheckAccount reads the organization's status, WARNING being logged and never a block
func (p *Provider) CheckAccount(ctx context.Context) (provider.Account, error) {
	org, err := p.organization(ctx)
	if err != nil {
		return provider.AccountReading(err)
	}
	switch {
	case bannedStatus[org.Status] || org.DeactivationReason == "LOCKED_BY_ADMIN":
		return provider.Account{
			Block: provider.BlockBanned, Detail: "organization " + org.Status,
		}, nil
	case unfundedStatus[org.StatusMessage] || unfundedStatus[org.Status]:
		return provider.Account{
			Block: provider.BlockUnfunded, Detail: "organization " + org.StatusMessage,
		}, nil
	case org.Status == "WARNING":
		p.log.Warn("koyeb organization in warning", "message", org.StatusMessage)
	}
	return provider.Account{}, nil
}
