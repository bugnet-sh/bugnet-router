package api

import (
	"context"
	"errors"
	"slices"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/estimator"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/rules"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
)

// maxEstimateNeeds is the most needs an Estimate may list, counted before any is read
const maxEstimateNeeds = 8

const (
	reasonSourceInvalid     = "SOURCE_INVALID"
	reasonSecretInArgument  = "SECRET_IN_ARGUMENT"
	reasonEstimateUnknown   = "ESTIMATE_UNKNOWN"
	estimateRetryAtLeast    = time.Second
	estimateRateLimitedText = "too many quotes, slow down"
)

// estimateNeeds merges the needs of an Estimate into its requirements
func estimateNeeds(r placement.Requirements, needs []routerv1.Need) placement.Requirements {
	for _, n := range needs {
		switch n {
		case routerv1.Need_NEED_SSH:
			r.SSH = true
		case routerv1.Need_NEED_PREVIEW:
			r.Preview = true
		case routerv1.Need_NEED_SECRETS:
			r.Secrets = true
		case routerv1.Need_NEED_GPU:
			r.GPU = true
		}
	}
	return r
}

func workloadOf(w routerv1.Workload) rules.Workload {
	return map[routerv1.Workload]rules.Workload{
		routerv1.Workload_WORKLOAD_TESTS:     rules.WorkloadTests,
		routerv1.Workload_WORKLOAD_BUILD:     rules.WorkloadBuild,
		routerv1.Workload_WORKLOAD_SCRIPT:    rules.WorkloadScript,
		routerv1.Workload_WORKLOAD_SERVE:     rules.WorkloadServe,
		routerv1.Workload_WORKLOAD_INFERENCE: rules.WorkloadInference,
		routerv1.Workload_WORKLOAD_OTHER:     rules.WorkloadOther,
	}[w]
}

func needsOf(ns []routerv1.Need) []rules.Need {
	var out []rules.Need
	for _, n := range ns {
		switch n {
		case routerv1.Need_NEED_SSH:
			out = append(out, rules.NeedSSH)
		case routerv1.Need_NEED_PREVIEW:
			out = append(out, rules.NeedPreview)
		case routerv1.Need_NEED_SECRETS:
			out = append(out, rules.NeedSecrets)
		case routerv1.Need_NEED_GPU:
			out = append(out, rules.NeedGPU)
		}
	}
	return out
}

// estimateStatus turns an estimator error into the Connect error Estimate answers with
func estimateStatus(err error) error {
	var lim *estimator.RateLimitedError
	switch {
	case errors.As(err, &lim):
		ce := connect.NewError(connect.CodeResourceExhausted, errors.New(estimateRateLimitedText))
		return withRetry(withInfo(ce, reasonRateLimited), max(lim.Wait, estimateRetryAtLeast))
	case errors.Is(err, estimator.ErrSecretInArgument):
		return withInfo(invalid("the source or command holds a secret, pass secrets by the "+
			"secret link"), reasonSecretInArgument)
	case errors.Is(err, estimator.ErrSourceInvalid):
		return withInfo(invalid("the source is not a GitHub repository, a Hugging Face model "+
			"or an https URL"), reasonSourceInvalid)
	case errors.Is(err, estimator.ErrInvalidArgument):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, errors.New("the quote was cut short"))
	}
	return connect.NewError(connect.CodeInternal, errors.New("the quote failed"))
}

var kindOf = map[string]routerv1.SourceKind{
	"none":        routerv1.SourceKind_SOURCE_KIND_NONE,
	"github":      routerv1.SourceKind_SOURCE_KIND_GITHUB,
	"huggingface": routerv1.SourceKind_SOURCE_KIND_HUGGINGFACE_MODEL,
	"url":         routerv1.SourceKind_SOURCE_KIND_URL,
}

var unreadOf = map[rules.UnreadReason]routerv1.UnreadReason{
	rules.UnreadNotFound:      routerv1.UnreadReason_UNREAD_REASON_NOT_FOUND,
	rules.UnreadLegal:         routerv1.UnreadReason_UNREAD_REASON_LEGAL,
	rules.UnreadRateLimited:   routerv1.UnreadReason_UNREAD_REASON_RATE_LIMITED,
	rules.UnreadTimeout:       routerv1.UnreadReason_UNREAD_REASON_TIMEOUT,
	rules.UnreadNotSupported:  routerv1.UnreadReason_UNREAD_REASON_NOT_SUPPORTED,
	rules.UnreadUpstreamError: routerv1.UnreadReason_UNREAD_REASON_UPSTREAM_ERROR,
}

var dimensionOf = map[rules.Dimension]routerv1.Dimension{
	rules.DimensionVCPU:      routerv1.Dimension_DIMENSION_VCPU,
	rules.DimensionMemory:    routerv1.Dimension_DIMENSION_MEMORY,
	rules.DimensionDisk:      routerv1.Dimension_DIMENSION_DISK,
	rules.DimensionGPU:       routerv1.Dimension_DIMENSION_GPU,
	rules.DimensionGPUMemory: routerv1.Dimension_DIMENSION_GPU_MEMORY,
	rules.DimensionMinutes:   routerv1.Dimension_DIMENSION_MINUTES,
}

var gpuOf = map[rules.GPUNeed]routerv1.GpuNeed{
	rules.GPUNone:        routerv1.GpuNeed_GPU_NEED_NONE,
	rules.GPURecommended: routerv1.GpuNeed_GPU_NEED_RECOMMENDED,
	rules.GPURequired:    routerv1.GpuNeed_GPU_NEED_REQUIRED,
}

var basisOf = map[rules.Basis]routerv1.EstimateBasis{
	rules.BasisDefaults: routerv1.EstimateBasis_ESTIMATE_BASIS_DEFAULTS,
	rules.BasisSignals:  routerv1.EstimateBasis_ESTIMATE_BASIS_SIGNALS,
	rules.BasisHistory:  routerv1.EstimateBasis_ESTIMATE_BASIS_HISTORY,
}

var confidenceOf = map[rules.Confidence]routerv1.Confidence{
	rules.ConfidenceLow:    routerv1.Confidence_CONFIDENCE_LOW,
	rules.ConfidenceMedium: routerv1.Confidence_CONFIDENCE_MEDIUM,
	rules.ConfidenceHigh:   routerv1.Confidence_CONFIDENCE_HIGH,
}

// estimateResponse builds the wire answer, each start option carrying its fit
func estimateResponse(
	ans estimator.Answer, opts []*routerv1.PackageOption,
) *routerv1.EstimateResponse {
	for _, o := range opts {
		i := slices.IndexFunc(ans.Output.Fits, func(f rules.Fit) bool {
			return f.PackageID == o.GetPackageId()
		})
		if i < 0 {
			continue
		}
		f := ans.Output.Fits[i]
		o.Fits, o.ExtensionsNeeded = f.Fits, uint32(f.ExtensionsNeeded)
		o.ExtensionPackageId, o.TotalPriceMicros = f.ExtensionPackageID, f.TotalPriceMicros
		for _, d := range f.ShortOn {
			o.ShortOn = append(o.ShortOn, dimensionOf[d])
		}
	}
	fl := ans.Output.Floor
	resp := &routerv1.EstimateResponse{
		Options: opts, EstimateId: ans.EstimateID, EstimatorVersion: ans.Version,
		InputsSha256: ans.InputsSHA256, RecommendedPackageId: ans.Rec.PackageID,
		Basis: basisOf[ans.Output.Basis], Confidence: confidenceOf[ans.Output.Confidence],
		Partial: ans.Output.Partial,
		Source: &routerv1.ResolvedSource{
			Kind: kindOf[ans.Source.Kind], Canonical: ans.Source.Canonical, Ref: ans.Source.Ref,
			Revision: ans.Source.Revision, Read: ans.Source.Read,
			UnreadReason: unreadOf[ans.Source.Unread],
		},
		Floor: &routerv1.ResourceFloor{
			Resources: &routerv1.Resources{
				Vcpu: uint32(fl.VCPU), MemoryMib: uint32(fl.MemoryMiB), DiskGib: uint32(fl.DiskGiB),
				GpuCount: uint32(fl.GPUCount), GpuMemoryGib: uint32(fl.GPUMemoryGiB),
			},
			Minutes: uint32(fl.Minutes), DownloadBytes: uint64(fl.DownloadBytes),
			Gpu: gpuOf[fl.GPU], ContextTokens: uint32(fl.ContextTokens),
		},
	}
	for _, r := range ans.Output.Reasons {
		resp.Reasons = append(resp.Reasons,
			&routerv1.EstimateReason{RuleId: r.RuleID, Message: r.Message})
	}
	for _, w := range ans.Rec.Warnings {
		resp.Warnings = append(resp.Warnings, &routerv1.EstimateWarning{
			Code: routerv1.WarningCode(w.Code), Message: w.Message,
		})
	}
	return resp
}

// GetEstimate returns a stored estimate with its facts and catalog, for replay
func (o *OperatorServer) GetEstimate(
	ctx context.Context, req *connect.Request[routerv1.GetEstimateRequest],
) (*connect.Response[routerv1.GetEstimateResponse], error) {
	if o.s.d.Estimator == nil {
		return nil, withInfo(connect.NewError(connect.CodeFailedPrecondition,
			errors.New("the router runs without an estimator")), reasonFeatureUnavailable)
	}
	id, err := uuid.Parse(req.Msg.GetEstimateId())
	if err != nil {
		return nil, invalid("estimate_id is not a UUID")
	}
	st, err := o.s.d.Estimator.Get(ctx, id)
	switch {
	case errors.Is(err, estimator.ErrEstimateUnknown):
		return nil, withInfo(connect.NewError(connect.CodeNotFound,
			errors.New("no such estimate")), reasonEstimateUnknown)
	case err != nil:
		o.s.d.Log.Error("api: get estimate", "err", err)
		return nil, connect.NewError(connect.CodeInternal, errors.New("the estimate is unreadable"))
	}
	return connect.NewResponse(&routerv1.GetEstimateResponse{
		EstimateId: st.ID.String(), EstimatorVersion: st.EstimatorVersion, Inputs: st.Inputs,
		Output: st.Output, InputsSha256: st.InputsSHA256,
		CreatedAt: timestamppb.New(st.CreatedAt), Catalog: st.Catalog,
	}), nil
}
