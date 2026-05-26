package trends

import "searchtrends/internal/domain"

type Repository interface {
	AddEvent(event domain.SearchEvent) domain.IngestResult
	GetTop(limit int) []domain.TrendEntry
	WindowSeconds() int
	AddStopWord(word string) bool
	DeleteStopWord(word string) bool
	StopWords() []string
	Close()
}

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) Ingest(event domain.SearchEvent) domain.IngestResult {
	return s.repo.AddEvent(event)
}

func (s *Service) Top(limit int) (int, []domain.TrendEntry) {
	return s.repo.WindowSeconds(), s.repo.GetTop(limit)
}

func (s *Service) AddStopWord(word string) bool {
	return s.repo.AddStopWord(word)
}

func (s *Service) DeleteStopWord(word string) bool {
	return s.repo.DeleteStopWord(word)
}

func (s *Service) StopWords() []string {
	return s.repo.StopWords()
}

func (s *Service) Close() {
	s.repo.Close()
}
