package hf

// Job stages, every value of status.stage HF documents
const (
	stageScheduling = "SCHEDULING"
	stageRunning    = "RUNNING"
	stageCompleted  = "COMPLETED"
	stageCanceled   = "CANCELED"
	stageDeleted    = "DELETED"
	stageError      = "ERROR"
)

// liveStages are a Job that can still serve, the stages every list asks for
var liveStages = []string{stageScheduling, stageRunning}

// liveStage reports a stage of a Job that can still serve
func liveStage(stage string) bool { return stage == stageScheduling || stage == stageRunning }
