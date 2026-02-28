package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"pipeline"
)

// ─────────────────────────────────────────────────────────────────────────────
// Colour palette  (Tokyo Night–inspired)
// ─────────────────────────────────────────────────────────────────────────────

var (
	colorSurface  = lipgloss.Color("#24283b")
	colorBorder   = lipgloss.Color("#3b4261")
	colorFocus    = lipgloss.Color("#7aa2f7")
	colorAccent   = lipgloss.Color("#7aa2f7")
	colorGreen    = lipgloss.Color("#9ece6a")
	colorRed      = lipgloss.Color("#f7768e")
	colorYellow   = lipgloss.Color("#e0af68")
	colorCyan     = lipgloss.Color("#2ac3de")
	colorMagenta  = lipgloss.Color("#bb9af7")
	colorDim      = lipgloss.Color("#565f89")
	colorWhite    = lipgloss.Color("#c0caf5")
	colorOrange   = lipgloss.Color("#ff9e64")
	colorTeal     = lipgloss.Color("#1abc9c")
	colorSelected = lipgloss.Color("#364A82")
)

// ─────────────────────────────────────────────────────────────────────────────
// Styles
// ─────────────────────────────────────────────────────────────────────────────

var (
	stylePanelTitle = lipgloss.NewStyle().Foreground(colorAccent).Bold(true).Padding(0, 1)
	styleLabel      = lipgloss.NewStyle().Foreground(colorDim)
	styleValue      = lipgloss.NewStyle().Foreground(colorWhite).Bold(true)
	styleGood       = lipgloss.NewStyle().Foreground(colorGreen).Bold(true)
	styleBad        = lipgloss.NewStyle().Foreground(colorRed).Bold(true)
	styleWarn       = lipgloss.NewStyle().Foreground(colorYellow).Bold(true)
	styleInfo       = lipgloss.NewStyle().Foreground(colorCyan)
	styleDim        = lipgloss.NewStyle().Foreground(colorDim)
	styleRunning    = lipgloss.NewStyle().Foreground(colorGreen)
	styleDone       = lipgloss.NewStyle().Foreground(colorDim)
	styleStageID    = lipgloss.NewStyle().Foreground(colorMagenta).Bold(true)
	styleWorkerID   = lipgloss.NewStyle().Foreground(colorCyan)
	styleKeyHint    = lipgloss.NewStyle().Foreground(colorDim)
	styleKeyName    = lipgloss.NewStyle().Foreground(colorAccent).Bold(true)
	styleFilterTag  = lipgloss.NewStyle().Foreground(colorOrange).Bold(true)
	styleCursor     = lipgloss.NewStyle().Background(colorSelected).Foreground(colorWhite)
	styleScrollBar  = lipgloss.NewStyle().Foreground(colorDim)
)

// ─────────────────────────────────────────────────────────────────────────────
// Focus panel enum
// ─────────────────────────────────────────────────────────────────────────────

type focusPanel int

const (
	focusLog    focusPanel = iota // right panel
	focusStages                   // left stages panel
)

// ─────────────────────────────────────────────────────────────────────────────
// Log filter enum
// ─────────────────────────────────────────────────────────────────────────────

type logFilter int

const (
	filterAll        logFilter = iota // every event
	filterErrors                      // ProcessFailed / RouterFailed
	filterStructural                  // stage / worker / router lifecycle
	filterItems                       // ProcessCompleted + ProcessFailed
	filterCount                       // sentinel – number of filters
)

func (f logFilter) label() string {
	switch f {
	case filterAll:
		return "ALL"
	case filterErrors:
		return "ERRORS"
	case filterStructural:
		return "LIFECYCLE"
	case filterItems:
		return "ITEMS"
	default:
		return "?"
	}
}

func (f logFilter) accepts(kind pipeline.EventType) bool {
	switch f {
	case filterAll:
		return true
	case filterErrors:
		return kind == pipeline.ProcessFailed || kind == pipeline.RouterFailed
	case filterStructural:
		return kind == pipeline.StageStarted || kind == pipeline.StageCompleted ||
			kind == pipeline.WorkerStarted || kind == pipeline.WorkerCompleted ||
			kind == pipeline.RouterStarted || kind == pipeline.RouterCompleted
	case filterItems:
		return kind == pipeline.ProcessCompleted || kind == pipeline.ProcessFailed
	default:
		return true
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TUI Observer – bridges pipeline events into tea.Msg
// ─────────────────────────────────────────────────────────────────────────────

type eventMsg struct {
	event pipeline.Eventful
	ts    time.Time
}

type tickMsg time.Time

type TUIObserver struct{ program *tea.Program }

func newTUIObserver(p *tea.Program) *TUIObserver { return &TUIObserver{program: p} }

func (t *TUIObserver) OnEvent(_ context.Context, event pipeline.Eventful) {
	t.program.Send(eventMsg{event: event, ts: time.Now()})
}

// ─────────────────────────────────────────────────────────────────────────────
// Throughput tracker
// ─────────────────────────────────────────────────────────────────────────────

const sparklineWidth = 20

type throughputTracker struct {
	mu       sync.Mutex
	samples  [sparklineWidth]int64
	lastTick int64
	head     int
}

func (t *throughputTracker) tick(total int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delta := total - t.lastTick
	if delta < 0 {
		delta = 0
	}
	t.lastTick = total
	t.samples[t.head] = delta
	t.head = (t.head + 1) % sparklineWidth
}

func (t *throughputTracker) snapshot() []int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]int64, sparklineWidth)
	for i := range sparklineWidth {
		out[i] = t.samples[(t.head+i)%sparklineWidth]
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// Log ring buffer  (stores ALL entries; filtering happens at render time)
// ─────────────────────────────────────────────────────────────────────────────

const maxLogEntries = 2000

type logEntry struct {
	ts      time.Time
	kind    pipeline.EventType
	id      string
	stageID string
}

type logRing struct {
	mu      sync.Mutex
	entries []logEntry
}

func (l *logRing) push(e logEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) >= maxLogEntries {
		l.entries = l.entries[1:]
	}
	l.entries = append(l.entries, e)
}

// all returns a copy of every entry (caller must not hold mu).
func (l *logRing) all() []logEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	cp := make([]logEntry, len(l.entries))
	copy(cp, l.entries)
	return cp
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage / worker state
// ─────────────────────────────────────────────────────────────────────────────

type workerState struct {
	id        string
	running   bool
	processed int64
	failed    int64
}

type stageState struct {
	id        string
	running   bool
	workers   map[string]*workerState
	processed int64
	failed    int64
	startTime time.Time
	endTime   time.Time
}

// ─────────────────────────────────────────────────────────────────────────────
// Main model
// ─────────────────────────────────────────────────────────────────────────────

type model struct {
	// pipeline
	collector  *pipeline.MetricsCollector
	throughput throughputTracker
	logRing    logRing

	// stage/worker state (updated from events)
	stagesMu sync.RWMutex
	stages   map[string]*stageState

	// terminal size
	width, height int

	// animation tick
	tick uint64

	// pipeline lifecycle
	pipelineDone bool
	startTime    time.Time

	// ── interactive state ───────────────────────────────────────────────────

	// which panel has keyboard focus
	focus focusPanel

	// ── log panel ──────────────────────────────────────────────────────────
	logFilter    logFilter
	logOffset    int  // index into filtered slice; 0 = oldest visible at top
	logAutoScroll bool // follow tail unless user has scrolled up

	// ── stages panel ───────────────────────────────────────────────────────
	stagesCursor   int            // index in sorted stage list
	stagesOffset   int            // scroll offset (first visible row index)
	stagesCollapsed map[string]bool // collapsed accordion state per stage ID
}

func newModel(collector *pipeline.MetricsCollector) model {
	return model{
		collector:       collector,
		stages:          make(map[string]*stageState),
		startTime:       time.Now(),
		logAutoScroll:   true,
		stagesCollapsed: make(map[string]bool),
		focus:           focusLog,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Init / Update / View
// ─────────────────────────────────────────────────────────────────────────────

func (m model) Init() tea.Cmd {
	return tea.Batch(tea.EnterAltScreen, tickCmd())
}

func tickCmd() tea.Cmd {
	return tea.Tick(250*time.Millisecond, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "tab":
			if m.focus == focusLog {
				m.focus = focusStages
			} else {
				m.focus = focusLog
			}
		default:
			switch m.focus {
			case focusLog:
				m = m.updateLog(msg)
			case focusStages:
				m = m.updateStages(msg)
			}
		}

	case tickMsg:
		m.tick++
		processed, _ := m.collector.GetRealtimeTotals()
		m.throughput.tick(processed)
		// If auto-scroll is on, keep offset pinned to the tail.
		if m.logAutoScroll {
			m.logOffset = m.logTailOffset()
		}
		return m, tickCmd()

	case eventMsg:
		m.handlePipelineEvent(msg)
		if m.logAutoScroll {
			m.logOffset = m.logTailOffset()
		}
	}

	return m, nil
}

// updateLog handles key events when the log panel is focused.
func (m model) updateLog(msg tea.KeyMsg) model {
	visibleH := m.logVisibleHeight()
	filtered := m.filteredLog()
	maxOffset := clamp(len(filtered)-visibleH, 0, len(filtered))

	switch msg.String() {
	case "up", "k":
		m.logAutoScroll = false
		m.logOffset = clamp(m.logOffset-1, 0, maxOffset)
	case "down", "j":
		m.logOffset = clamp(m.logOffset+1, 0, maxOffset)
		if m.logOffset >= maxOffset {
			m.logAutoScroll = true
		}
	case "pgup", "u":
		m.logAutoScroll = false
		m.logOffset = clamp(m.logOffset-visibleH, 0, maxOffset)
	case "pgdown", "d":
		m.logOffset = clamp(m.logOffset+visibleH, 0, maxOffset)
		if m.logOffset >= maxOffset {
			m.logAutoScroll = true
		}
	case "G", "end":
		m.logAutoScroll = true
		m.logOffset = maxOffset
	case "g", "home":
		m.logAutoScroll = false
		m.logOffset = 0
	case "f":
		m.logFilter = (m.logFilter + 1) % filterCount
		// revalidate offset after filter change
		filtered = m.filteredLog()
		maxOffset = clamp(len(filtered)-visibleH, 0, len(filtered))
		if m.logAutoScroll {
			m.logOffset = maxOffset
		} else {
			m.logOffset = clamp(m.logOffset, 0, maxOffset)
		}
	}
	return m
}

// updateStages handles key events when the stages panel is focused.
func (m model) updateStages(msg tea.KeyMsg) model {
	ids := m.sortedStageIDs()
	if len(ids) == 0 {
		return m
	}

	switch msg.String() {
	case "up", "k":
		m.stagesCursor = clamp(m.stagesCursor-1, 0, len(ids)-1)
		m.stagesScrollToCursor(ids)
	case "down", "j":
		m.stagesCursor = clamp(m.stagesCursor+1, 0, len(ids)-1)
		m.stagesScrollToCursor(ids)
	case "enter", " ":
		if m.stagesCursor < len(ids) {
			sid := ids[m.stagesCursor]
			m.stagesCollapsed[sid] = !m.stagesCollapsed[sid]
		}
	}
	return m
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline event handler
// ─────────────────────────────────────────────────────────────────────────────

func (m *model) handlePipelineEvent(msg eventMsg) {
	e := msg.event
	et := e.GetType()
	meta := e.GetMetadata()
	stageID := metaString(meta, "stage-id")
	workerID := metaString(meta, "worker-id")

	m.stagesMu.Lock()
	switch et {
	case pipeline.StageStarted:
		m.stages[e.GetID()] = &stageState{
			id:        e.GetID(),
			running:   true,
			workers:   make(map[string]*workerState),
			startTime: msg.ts,
		}
	case pipeline.StageCompleted:
		if s, ok := m.stages[e.GetID()]; ok {
			s.running = false
			s.endTime = msg.ts
		}
		m.pipelineDone = m.allStageDone()
	case pipeline.WorkerStarted:
		if s, ok := m.stages[stageID]; ok {
			s.workers[e.GetID()] = &workerState{id: e.GetID(), running: true}
		}
	case pipeline.WorkerCompleted:
		if s, ok := m.stages[stageID]; ok {
			if w, ok := s.workers[e.GetID()]; ok {
				w.running = false
			}
		}
	case pipeline.ProcessCompleted:
		if s, ok := m.stages[stageID]; ok {
			s.processed++
			if w, ok := s.workers[workerID]; ok {
				w.processed++
			}
		}
	case pipeline.ProcessFailed:
		if s, ok := m.stages[stageID]; ok {
			s.failed++
			if w, ok := s.workers[workerID]; ok {
				w.failed++
			}
		}
	}
	m.stagesMu.Unlock()

	// Push to log ring (skip the very noisy ProcessStarted)
	if et != pipeline.ProcessStarted {
		m.logRing.push(logEntry{
			ts: msg.ts, kind: et, id: e.GetID(), stageID: stageID,
		})
	}
}

func (m *model) allStageDone() bool {
	for _, s := range m.stages {
		if s.running {
			return false
		}
	}
	return len(m.stages) > 0
}

// ─────────────────────────────────────────────────────────────────────────────
// Log helpers
// ─────────────────────────────────────────────────────────────────────────────

func (m *model) filteredLog() []logEntry {
	all := m.logRing.all()
	if m.logFilter == filterAll {
		return all
	}
	out := make([]logEntry, 0, len(all))
	for _, e := range all {
		if m.logFilter.accepts(e.kind) {
			out = append(out, e)
		}
	}
	return out
}

// logVisibleHeight returns the number of entry rows visible inside the log panel.
func (m *model) logVisibleHeight() int {
	// logInner = m.height - headerRows(2) - footerRows(2) - borderRows(2)
	// visible  = logInner - titleRow(1)
	return clamp(m.height-7, 1, m.height)
}

// logTailOffset returns the offset that shows the very last entries.
func (m *model) logTailOffset() int {
	filtered := m.filteredLog()
	visibleH := m.logVisibleHeight()
	return clamp(len(filtered)-visibleH, 0, len(filtered))
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage helpers
// ─────────────────────────────────────────────────────────────────────────────

func (m *model) sortedStageIDs() []string {
	m.stagesMu.RLock()
	defer m.stagesMu.RUnlock()
	ids := make([]string, 0, len(m.stages))
	for id := range m.stages {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// buildStageRows renders all stage+worker rows into a flat string slice,
// also returning which row index corresponds to the cursor stage header.
func (m *model) buildStageRows(contentW int) (rows []string, cursorRow int) {
	m.stagesMu.RLock()
	defer m.stagesMu.RUnlock()

	ids := make([]string, 0, len(m.stages))
	for id := range m.stages {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	cursorRow = 0
	for i, sid := range ids {
		s := m.stages[sid]

		// ── stage header row ────────────────────────────────────────────
		collapsed := m.stagesCollapsed[sid]
		isCursor := (i == m.stagesCursor && m.focus == focusStages)

		accordion := styleDim.Render("▶")
		if !collapsed {
			accordion = styleDim.Render("▼")
		}

		statusIcon := styleRunning.Render("◉")
		if !s.running {
			statusIcon = styleDone.Render("◎")
		}

		dur := ""
		if !s.startTime.IsZero() {
			d := time.Since(s.startTime).Truncate(time.Millisecond)
			if !s.endTime.IsZero() {
				d = s.endTime.Sub(s.startTime).Truncate(time.Millisecond)
			}
			dur = styleDim.Render(" " + d.String())
		}

		failPart := ""
		if s.failed > 0 {
			failPart = "  " + styleBad.Render(fmt.Sprintf("✗%d", s.failed))
		}

		header := fmt.Sprintf("%s %s %s%s  %s%s",
			accordion,
			statusIcon,
			styleStageID.Render(sid),
			dur,
			styleGood.Render(fmt.Sprintf("↑%d", s.processed)),
			failPart,
		)

		if isCursor {
			// Pad to full width so the highlight spans the row
			plain := lipgloss.Width(header)
			if plain < contentW {
				header += strings.Repeat(" ", contentW-plain)
			}
			header = styleCursor.Render(header)
			cursorRow = len(rows)
		}
		rows = append(rows, header)

		if collapsed {
			continue
		}

		// ── worker rows ────────────────────────────────────────────────
		wids := make([]string, 0, len(s.workers))
		for wid := range s.workers {
			wids = append(wids, wid)
		}
		sort.Strings(wids)

		for _, wid := range wids {
			w := s.workers[wid]
			wIcon := styleRunning.Render("·")
			if !w.running {
				wIcon = styleDim.Render("·")
			}
			wFail := ""
			if w.failed > 0 {
				wFail = "  " + styleBad.Render(fmt.Sprintf("✗%d", w.failed))
			}
			rows = append(rows, fmt.Sprintf("   %s %s  %s%s",
				wIcon,
				styleWorkerID.Render("worker-"+wid),
				styleDim.Render(fmt.Sprintf("↑%d", w.processed)),
				wFail,
			))
		}
	}
	return rows, cursorRow
}

// stagesScrollToCursor adjusts stagesOffset so the cursor row is always visible.
func (m *model) stagesScrollToCursor(ids []string) {
	if len(ids) == 0 {
		return
	}
	// Approximate: cursor is somewhere around stagesCursor * (1 + avgWorkers).
	// We use the actual built rows for precision.
	contentW := clamp(m.width*2/5, 30, 60) - 4
	rows, cursorRow := m.buildStageRows(contentW)
	visibleH := m.stagesVisibleHeight()
	maxOffset := clamp(len(rows)-visibleH, 0, len(rows))

	if cursorRow < m.stagesOffset {
		m.stagesOffset = cursorRow
	} else if cursorRow >= m.stagesOffset+visibleH {
		m.stagesOffset = cursorRow - visibleH + 1
	}
	m.stagesOffset = clamp(m.stagesOffset, 0, maxOffset)
}

func (m *model) stagesVisibleHeight() int {
	const (
		headerRows   = 2
		footerRows   = 2
		borderRows   = 2
		panelCount   = 3
		summaryInner = 6
		sparkInner   = 5
	)
	fixed := headerRows + footerRows + panelCount*borderRows + summaryInner + sparkInner
	stagesInner := clamp(m.height-fixed, 3, m.height)
	return clamp(stagesInner-1, 1, stagesInner) // -1 for title row
}

// ─────────────────────────────────────────────────────────────────────────────
// View
// ─────────────────────────────────────────────────────────────────────────────

func (m model) View() string {
	if m.width == 0 {
		return "Initialising…"
	}

	leftW := clamp(m.width*2/5, 30, 60)
	rightW := m.width - leftW - 3

	const (
		headerRows   = 2
		footerRows   = 2
		borderRows   = 2
		panelCount   = 3
		summaryInner = 6
		sparkInner   = 5
	)

	fixedRows := headerRows + footerRows + panelCount*borderRows + summaryInner + sparkInner
	stagesInner := clamp(m.height-fixedRows, 3, m.height)
	logInner := clamp(m.height-headerRows-footerRows-borderRows, 3, m.height)

	header := m.renderHeader()

	// Left column
	summary := m.renderSummary(leftW-4, summaryInner)
	spark := m.renderSparkline(leftW-4, sparkInner)
	stages := m.renderStages(leftW-4, stagesInner)

	logFocused := m.focus == focusLog
	stagesFocused := m.focus == focusStages

	left := lipgloss.JoinVertical(lipgloss.Left,
		panel("◈  Pipeline Summary", summary, leftW, summaryInner, false),
		panel("▲  Throughput / s", spark, leftW, sparkInner, false),
		panel("⬡  Stages & Workers  [tab]", stages, leftW, stagesInner, stagesFocused),
	)

	// Right column
	logContent := m.renderLog(clamp(rightW-4, 1, rightW), clamp(logInner-1, 1, logInner))
	right := panel("✦  Live Event Log  [tab]", logContent, rightW, logInner, logFocused)

	body := lipgloss.JoinHorizontal(lipgloss.Top, left, "  ", right)
	footer := m.renderFooter()

	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

// ─────────────────────────────────────────────────────────────────────────────
// Render helpers
// ─────────────────────────────────────────────────────────────────────────────

func (m model) renderHeader() string {
	elapsed := time.Since(m.startTime).Truncate(time.Millisecond)

	status := styleGood.Render("● RUNNING")
	if m.pipelineDone {
		status = styleDone.Render("◉ DONE")
	}

	title := lipgloss.NewStyle().Foreground(colorAccent).Bold(true).Render("  PIPELINE DASHBOARD")
	right := lipgloss.NewStyle().Foreground(colorDim).Render(fmt.Sprintf("elapsed %s  %s", elapsed, status))

	gap := m.width - lipgloss.Width(title) - lipgloss.Width(right) - 2
	if gap < 1 {
		gap = 1
	}

	bar := lipgloss.NewStyle().
		Background(colorSurface).
		Width(m.width).
		Render(title + strings.Repeat(" ", gap) + right)

	sep := lipgloss.NewStyle().Foreground(colorBorder).Render(strings.Repeat("─", m.width))
	return lipgloss.JoinVertical(lipgloss.Left, bar, sep)
}

func (m model) renderSummary(w, _ int) string {
	processed, failed := m.collector.GetRealtimeTotals()
	metrics := m.collector.GetMetrics()

	total := processed + failed
	successRate := 0.0
	if total > 0 {
		successRate = float64(processed) / float64(total) * 100
	}
	rateStyle := styleGood
	if successRate < 90 {
		rateStyle = styleBad
	} else if successRate < 99 {
		rateStyle = styleWarn
	}

	rows := []string{
		kv(w, "Processed", styleGood.Render(fmt.Sprintf("%d", processed))),
		kv(w, "Failed", func() string {
			if failed > 0 {
				return styleBad.Render(fmt.Sprintf("%d", failed))
			}
			return styleGood.Render("0")
		}()),
		kv(w, "Success rate", rateStyle.Render(fmt.Sprintf("%.1f%%", successRate))),
		kv(w, "Elapsed", styleValue.Render(metrics.Duration.Truncate(time.Millisecond).String())),
		kv(w, "Stages", styleValue.Render(fmt.Sprintf("%d", len(metrics.StagesMetrics)))),
	}
	return strings.Join(rows, "\n")
}

func (m model) renderSparkline(w, _ int) string {
	samples := m.throughput.snapshot()

	maxVal := int64(1)
	for _, v := range samples {
		if v > maxVal {
			maxVal = v
		}
	}

	current := samples[len(samples)-1] * 4 // 250 ms ticks → per second

	const barH = 4
	blocks := []string{"▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"}
	lines := make([]string, barH)
	for row := range barH {
		threshold := float64(barH-row) / float64(barH)
		var sb strings.Builder
		for _, v := range samples {
			norm := float64(v) / float64(maxVal)
			if norm >= threshold {
				idx := clamp(int(norm*float64(len(blocks)-1)), 0, len(blocks)-1)
				sb.WriteString(lipgloss.NewStyle().Foreground(colorTeal).Render(blocks[idx]))
			} else {
				sb.WriteString(styleDim.Render("╌"))
			}
		}
		lines[row] = sb.String()
	}

	rateLabel := lipgloss.NewStyle().Foreground(colorOrange).Bold(true).
		Render(fmt.Sprintf("%d /s", current))
	return strings.Join(lines, "\n") + "\n" + rateLabel
}

func (m model) renderStages(contentW, innerH int) string {
	m.stagesMu.RLock()
	empty := len(m.stages) == 0
	m.stagesMu.RUnlock()

	if empty {
		return styleDim.Render("  Waiting for stages…")
	}

	rows, _ := m.buildStageRows(contentW)
	visibleH := clamp(innerH-1, 1, innerH) // -1 for title row already in panel()

	maxOffset := clamp(len(rows)-visibleH, 0, len(rows))
	offset := clamp(m.stagesOffset, 0, maxOffset)

	end := clamp(offset+visibleH, 0, len(rows))
	visible := rows[offset:end]

	// Scrollbar
	if len(rows) > visibleH {
		visible = attachScrollbar(visible, visibleH, offset, len(rows), contentW)
	}

	// Hint line
	hint := ""
	if m.focus == focusStages {
		hint = styleKeyHint.Render("  ↑↓ navigate  space/enter collapse")
	}
	if hint != "" {
		return strings.Join(visible, "\n") + "\n" + hint
	}
	return strings.Join(visible, "\n")
}

func (m model) renderLog(contentW, visibleH int) string {
	filtered := m.filteredLog()

	// Build filter bar
	filterBar := styleKeyHint.Render("filter: ") +
		styleFilterTag.Render("["+m.logFilter.label()+"]") +
		styleKeyHint.Render("  f=cycle")

	autoTag := ""
	if m.logAutoScroll {
		autoTag = styleDim.Render("  ↓ auto")
	} else {
		autoTag = styleWarn.Render("  ↑ paused")
	}

	filterBar += autoTag

	// Available rows for entries = visibleH - 2 (filter bar + blank separator)
	entryRows := clamp(visibleH-2, 1, visibleH)

	if len(filtered) == 0 {
		return filterBar + "\n" + styleDim.Render("  No matching events…")
	}

	maxOffset := clamp(len(filtered)-entryRows, 0, len(filtered))
	offset := clamp(m.logOffset, 0, maxOffset)
	end := clamp(offset+entryRows, 0, len(filtered))
	visible := filtered[offset:end]

	lines := make([]string, len(visible))
	for i, e := range visible {
		ts := styleDim.Render(e.ts.Format("15:04:05.000"))
		label, labelStyle := eventKindLabel(e.kind)
		idPart := styleInfo.Render(truncate(e.id, 18))
		stagePart := ""
		if e.stageID != "" {
			stagePart = styleDim.Render(" @ ") + styleDim.Render(e.stageID)
		}
		lines[i] = fmt.Sprintf("%s  %s  %s%s",
			ts,
			labelStyle.Width(16).Render(label),
			idPart,
			stagePart,
		)
	}

	// Attach scrollbar to entry lines
	if len(filtered) > entryRows {
		lines = attachScrollbar(lines, entryRows, offset, len(filtered), contentW)
	}

	hint := ""
	if m.focus == focusLog {
		hint = styleKeyHint.Render("  ↑↓/jk scroll  u/d pgup/dn  g/G top/btm  f filter")
	}

	parts := []string{filterBar, strings.Join(lines, "\n")}
	if hint != "" {
		parts = append(parts, hint)
	}
	return strings.Join(parts, "\n")
}

func (m model) renderFooter() string {
	focusName := "log"
	if m.focus == focusStages {
		focusName = "stages"
	}

	keys := []struct{ k, v string }{
		{"tab", "switch panel"},
		{"q", "quit"},
	}

	var parts []string
	parts = append(parts,
		styleKeyHint.Render("focus: ")+styleKeyName.Render(focusName),
	)
	for _, kv := range keys {
		parts = append(parts,
			styleKeyName.Render(kv.k)+" "+styleKeyHint.Render(kv.v),
		)
	}

	hint := strings.Join(parts, styleDim.Render("  │  "))
	sep := lipgloss.NewStyle().Foreground(colorBorder).Render(strings.Repeat("─", m.width))
	return lipgloss.JoinVertical(lipgloss.Left, sep,
		lipgloss.NewStyle().Padding(0, 1).Render(hint))
}

// ─────────────────────────────────────────────────────────────────────────────
// Scrollbar helper
// ─────────────────────────────────────────────────────────────────────────────

// attachScrollbar appends a single-column scrollbar glyph to each visible row.
func attachScrollbar(rows []string, visibleH, offset, total, contentW int) []string {
	if visibleH <= 0 || total <= visibleH {
		return rows
	}

	thumbH := clamp(visibleH*visibleH/total, 1, visibleH)
	thumbTop := (offset * (visibleH - thumbH)) / clamp(total-visibleH, 1, total)

	out := make([]string, len(rows))
	for i, row := range rows {
		var bar string
		rowInTrack := i
		if rowInTrack >= thumbTop && rowInTrack < thumbTop+thumbH {
			bar = lipgloss.NewStyle().Foreground(colorAccent).Render("┃")
		} else {
			bar = styleScrollBar.Render("│")
		}
		// Pad row to contentW then append bar
		padded := row
		w := lipgloss.Width(row)
		if w < contentW-1 {
			padded += strings.Repeat(" ", contentW-1-w)
		}
		out[i] = padded + bar
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// Panel helper
// ─────────────────────────────────────────────────────────────────────────────

func panel(title, content string, w, innerH int, focused bool) string {
	contentH := clamp(innerH-1, 1, innerH)
	inner := lipgloss.NewStyle().Width(w - 4).Height(contentH).Render(content)

	borderColor := colorBorder
	if focused {
		borderColor = colorFocus
	}

	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(borderColor).
		Background(colorSurface).
		Width(w).
		Render(stylePanelTitle.Render(title) + "\n" + inner)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tiny utilities
// ─────────────────────────────────────────────────────────────────────────────

func kv(w int, label, value string) string {
	lbl := styleLabel.Render(label)
	pad := w - lipgloss.Width(lbl) - lipgloss.Width(value) - 4
	if pad < 1 {
		pad = 1
	}
	return lbl + strings.Repeat(" ", pad) + value
}

func metaString(meta map[string]any, key string) string {
	if meta == nil {
		return ""
	}
	if v, ok := meta[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func eventKindLabel(et pipeline.EventType) (string, lipgloss.Style) {
	switch et {
	case pipeline.StageStarted:
		return "stage started", lipgloss.NewStyle().Foreground(colorAccent)
	case pipeline.StageCompleted:
		return "stage done", lipgloss.NewStyle().Foreground(colorGreen)
	case pipeline.WorkerStarted:
		return "worker started", lipgloss.NewStyle().Foreground(colorCyan)
	case pipeline.WorkerCompleted:
		return "worker done", lipgloss.NewStyle().Foreground(colorDim)
	case pipeline.ProcessCompleted:
		return "item processed", lipgloss.NewStyle().Foreground(colorGreen)
	case pipeline.ProcessFailed:
		return "item failed", lipgloss.NewStyle().Foreground(colorRed)
	case pipeline.RouterStarted:
		return "router started", lipgloss.NewStyle().Foreground(colorMagenta)
	case pipeline.RouterCompleted:
		return "router done", lipgloss.NewStyle().Foreground(colorMagenta)
	case pipeline.RouterFailed:
		return "router error", lipgloss.NewStyle().Foreground(colorRed)
	default:
		return string(et), lipgloss.NewStyle().Foreground(colorDim)
	}
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-1] + "…"
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

var _ = math.MaxInt64 // keep math imported (used indirectly via clamp / sparkline)

// ─────────────────────────────────────────────────────────────────────────────
// Demo pipeline
// ─────────────────────────────────────────────────────────────────────────────

type Order struct {
	ID       string
	Product  string
	Quantity int
	Price    float64
}

func (o Order) GetID() string { return o.ID }

type EnrichedOrder struct {
	ID    string
	Total float64
}

func (e EnrichedOrder) GetID() string { return e.ID }

func runDemoPipeline(cfg *pipeline.Config) {
	const numOrders = 300
	products := []string{"Widget", "Gadget", "Doohickey", "Thingamajig", "Whatchamacallit"}

	source := make(chan Order, numOrders)
	for i := 1; i <= numOrders; i++ {
		source <- Order{
			ID:       fmt.Sprintf("order-%03d", i),
			Product:  products[i%len(products)],
			Quantity: i % 10,
			Price:    float64(i%50) * 1.99,
		}
	}
	close(source)

	validated, err := pipeline.StartStage(pipeline.NewStageConfig(
		cfg, "validate", 3,
		func(o Order) (Order, error) {
			time.Sleep(8 * time.Millisecond)
			if o.Quantity == 0 {
				return Order{}, fmt.Errorf("zero quantity for %s", o.ID)
			}
			return o, nil
		},
		source,
	))
	if err != nil {
		return
	}

	enriched, err := pipeline.StartStage(pipeline.NewStageConfig(
		cfg, "enrich", 4,
		func(o Order) (EnrichedOrder, error) {
			time.Sleep(12 * time.Millisecond)
			return EnrichedOrder{ID: o.ID, Total: float64(o.Quantity) * o.Price}, nil
		},
		validated,
	))
	if err != nil {
		return
	}

	persisted, err := pipeline.StartStage(pipeline.NewStageConfig(
		cfg, "persist", 2,
		func(e EnrichedOrder) (pipeline.StringItem, error) {
			time.Sleep(15 * time.Millisecond)
			if e.Total > 4000 {
				return pipeline.StringItem{}, fmt.Errorf("amount too large: %.2f", e.Total)
			}
			return pipeline.NewStringItemWithID(e.ID, "saved:"+e.ID), nil
		},
		enriched,
	))
	if err != nil {
		return
	}

	for range persisted {
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	cfg := pipeline.DefaultConfig()
	collector := pipeline.NewMetricsCollector(cfg)
	defer collector.Close()

	m := newModel(collector)

	p := tea.NewProgram(m, tea.WithAltScreen(), tea.WithMouseCellMotion())

	tuiObs := newTUIObserver(p)
	multi := pipeline.NewMultiObserver(collector, tuiObs)
	cfg = cfg.WithObserver(multi)

	go runDemoPipeline(cfg)

	if _, err := p.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
	}
}
