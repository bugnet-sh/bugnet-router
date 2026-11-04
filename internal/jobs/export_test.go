package jobs

// StopRenewing keeps s from renewing its lease on the job, so the lease expires while s streams
func StopRenewing(s *Service, jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.owners[jobID]
	if ok {
		o.renewPaused.Store(true)
	}
	return ok
}

// Owns reports whether s runs an owner loop for the job
func Owns(s *Service, jobID string) bool { return s.owns(jobID) }
