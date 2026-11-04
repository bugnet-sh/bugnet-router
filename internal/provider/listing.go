package provider

// MaxListDirs bounds how many directories one ListFiles walk reads
const MaxListDirs = 1000

// Listing gathers at most limit entries from at most MaxListDirs directories
type Listing struct {
	Entries   []FileEntry
	Truncated bool // entries were left out at a limit of the router or of the provider
	limit     int
	dirs      int
}

// NewListing starts a listing of at most limit entries
func NewListing(limit int) *Listing {
	return &Listing{limit: limit}
}

// Add keeps e and reports whether the listing has room for another entry
func (l *Listing) Add(e FileEntry) bool {
	if len(l.Entries) >= l.limit {
		l.Truncated = true
		return false
	}
	l.Entries = append(l.Entries, e)
	return true
}

// Room returns how many more entries the listing takes
func (l *Listing) Room() int { return max(l.limit-len(l.Entries), 0) }

// Full reports whether the listing holds as many entries as it may
func (l *Listing) Full() bool { return len(l.Entries) >= l.limit }

// Visit counts one directory about to be read and reports false once the walk must stop
func (l *Listing) Visit() bool {
	if l.Full() || l.dirs >= MaxListDirs {
		l.Truncated = true
		return false
	}
	l.dirs++
	return true
}
