# UI Reference — CyberIndex

Self-contained Flask + vanilla JS. No frontend framework or build step. All CSS and JS are inline in `templates/index.html`.

---

## Design tokens (CSS variables)

```css
--bg:         #F8F9FA   /* page background */
--surface:    #FFFFFF   /* cards, header */
--surface-2:  #F1F3F5   /* table header, hover backgrounds */
--border:     #DEE2E6
--accent:     #2563EB   /* blue — buttons, links, focus rings */
--accent-bg:  #EFF6FF   /* light blue tint for hover/selection */
--text:       #111827
--text-2:     #6B7280   /* secondary / muted text */
--green:      #16A34A
--red:        #DC2626
--radius:     8px
--shadow:     0 1px 3px rgba(0,0,0,0.08), 0 1px 2px rgba(0,0,0,0.04)
--shadow-md:  0 4px 6px -1px rgba(0,0,0,0.08), 0 2px 4px -2px rgba(0,0,0,0.04)
--font:       -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif
```

Base font-size: `14px`, line-height: `1.5`.

---

## Layout

```
┌─────────────────────────────────────────────────────────┐
│  .app-header  (sticky, 56px, z-index 100)               │
├─────────────────────────────────────────────────────────┤
│  .main  (max-width 1440px, padding 24px)                │
│    .controls-card                                       │
│    #spinner / #error-banner                             │
│    #results-section                                     │
│      .chart-card                                        │
│      .table-card                                        │
└─────────────────────────────────────────────────────────┘
                                          ┌──────────────┐
                                          │  #ai-sidebar │
                                          │  (fixed, RHS)│
                                          └──────────────┘
```

When the sidebar opens, `.main` gets `padding-right: calc(420px + 24px)` via class `sidebar-open` — slides content left instead of overlapping.

---

## App header

```html
<header class="app-header">
  <a class="app-logo" href="#">Brand <span>Accent</span></a>
  <span class="header-sub">Subtitle text</span>
</header>
```

- Sticky, white, 1px bottom border, `--shadow`.
- `.app-logo span` renders in `--accent` blue.
- `.header-sub`: 12px, `--text-2`.

---

## Controls card

```html
<div class="controls-card">
  <div class="controls-group">
    <div class="controls-group-label">SECTION LABEL</div>
    <!-- inputs -->
  </div>
  <button id="pull-btn">&#9654; Action</button>
</div>
```

- `display: flex; flex-wrap: wrap; gap: 32px; align-items: flex-start`.
- `.controls-group-label`: 11px, uppercase, `letter-spacing: 0.6px`, `--text-2`.
- Button has `align-self: flex-end` — pins to card bottom regardless of group height.

### Radio mode rows

Two mutually exclusive input modes, one active and one dimmed:

```html
<div class="mode-row" id="mode-row-a">
  <input type="radio" class="mode-radio" name="mode" id="mode-a" checked>
  <label for="mode-a">Option A</label>
</div>
<div class="mode-row inactive" id="mode-row-b">
  <input type="radio" class="mode-radio" name="mode" id="mode-b">
  <label for="mode-b">Option B</label>
</div>
```

`.inactive` dims the row: `color: --text-2`, and child inputs get `opacity: 0.4–0.5; pointer-events: none`. The radio itself stays interactive.

Custom radio style:

```css
.mode-radio { appearance: none; width: 14px; height: 14px; border: 2px solid var(--border); border-radius: 50%; }
.mode-radio:checked { border-color: var(--accent); background: var(--accent); box-shadow: inset 0 0 0 2px #fff; }
```

---

## Primary button

```css
background: var(--accent); color: #fff;
border: 1px solid var(--accent); border-radius: 6px;
padding: 6px 16px; font-size: 13px; font-weight: 500;
display: inline-flex; align-items: center; gap: 6px;
```

Hover: `#1d4ed8`. Disabled: `opacity: 0.6; cursor: not-allowed`.

---

## Spinner

```html
<div id="spinner">   <!-- display:flex when active -->
  <div class="spin-ring"></div>
  <span>Loading…</span>
</div>
```

```css
.spin-ring {
  width: 18px; height: 18px;
  border: 2px solid var(--border);
  border-top-color: var(--accent);
  border-radius: 50%;
  animation: spin 0.75s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }
```

---

## Error banner

```html
<div id="error-banner">Message</div>
```

```css
background: #FEF2F2; border: 1px solid #FECACA;
border-left: 3px solid var(--red); color: #991B1B;
padding: 10px 14px; border-radius: 6px; font-size: 13px;
```

---

## Chart card

```html
<div class="chart-card">
  <div class="chart-card-label">LABEL</div>
  <div class="chart-subtitle">Description</div>
  <div id="chart-svg-wrap">
    <svg id="chart-svg" preserveAspectRatio="none"></svg>
    <div id="chart-tip"></div>   <!-- hover tooltip -->
  </div>
  <div class="chart-source">Source: …</div>
</div>
```

**Responsive SVG trick**: wrapper uses `padding-bottom: 42%; height: 0; position: relative`. SVG is `position: absolute; width: 100%; height: 100%`. This locks the aspect ratio at any container width.

**Tooltip** (`.chart-tip`, `.company-chart-tip`):

```css
position: absolute; pointer-events: none;
background: rgba(17,24,39,0.93); color: #fff;
border-radius: 6px; padding: 8px 12px; font-size: 12px; line-height: 1.6;
white-space: nowrap;
```

**Crosshair**: a dashed vertical `<line>` (`stroke-dasharray: 4,3; stroke: #94a3b8`) tracked by a transparent `<rect>` overlay that catches `mousemove`.

**Line colors**:

| Series | Color |
|---|---|
| Index / benchmark | `#4a9e6e` (green) |
| Top gainer | `#1d5c6e` (dark teal) |
| Top loser | `#5bb8d4` (light blue) |
| Company (positive) | `#1d5c6e` |
| Company (negative) | `#e02424` |
| Benchmark in mini-chart | `#d1d5db` (grey) |

**Event marker circles**: `r=7`, white stroke (`stroke-width: 2`), green (`#16a34a`) for up moves, red (`#dc2626`) for down moves. Appended after the overlay so they receive click events.

**Inline mini-chart** (expands below a clicked table row):

- Wrapper: `padding-bottom: 20%; height: 0` — shorter aspect ratio than main chart.
- Background: `--surface-2`.

---

## Table card

```html
<div class="table-card">
  <div class="table-card-header">
    <div class="table-card-title">Title</div>
    <div class="download-links">
      <a href="#">&#8659; Export</a>
    </div>
  </div>
  <div class="table-wrap">
    <div id="table-container"></div>
  </div>
</div>
```

**Download link buttons**:

```css
border: 1px solid var(--border); color: var(--text-2); border-radius: 6px;
padding: 5px 12px; font-size: 12px; font-weight: 500; text-decoration: none;
```

Hover: `background: --surface-2; color: --text; border-color: #9ca3af`.

**Table styles**:

- `thead th`: `--surface-2` bg, 11px uppercase, `letter-spacing: 0.5px`, `cursor: pointer`.
- Sort arrows via `::after` — class `sort-asc` → `▲`, `sort-desc` → `▼` (9px).
- `tbody tr:hover`: `background: --accent-bg`.
- `tbody tr:last-child td`: no bottom border.

**Special row variants**:

| Selector | Style |
|---|---|
| `.cyber-index-row td` | `color: #16a34a; font-weight: 600` |
| `.cyber-index-row:hover` | `background: #F0FDF4` |
| `tr[data-company]` | `cursor: pointer` |
| `.active-company-row > td` | `background: --accent-bg` |
| `.company-chart-row td` | `padding: 0; background: --surface-2` |

---

## AI Analyst Sidebar

```html
<div id="ai-sidebar">
  <div class="sidebar-header">
    <div>
      <div class="sidebar-title">AI Analyst</div>
      <div class="sidebar-meta"></div>
    </div>
    <button id="sidebar-close">&#10005;</button>
  </div>
  <div class="sidebar-messages"></div>
  <div class="sidebar-input-wrap">
    <textarea rows="1"></textarea>
    <button id="sidebar-send">Send</button>
  </div>
</div>
```

**Slide-in from right**:

```css
#ai-sidebar {
  position: fixed; top: 0; right: 0; height: 100vh; width: 420px; max-width: 100vw;
  background: var(--surface); border-left: 1px solid var(--border);
  box-shadow: -4px 0 24px rgba(0,0,0,0.12);
  transform: translateX(100%);
  transition: transform 0.28s cubic-bezier(0.4,0,0.2,1);
  z-index: 500;
}
#ai-sidebar.open { transform: translateX(0); }
```

**Message bubbles**:

```css
.msg { max-width: 88%; padding: 10px 14px; border-radius: 10px; font-size: 13px; white-space: pre-wrap; }
.msg-user      { background: var(--accent); color: #fff; align-self: flex-end; border-bottom-right-radius: 3px; }
.msg-assistant { background: var(--surface-2); color: var(--text); align-self: flex-start; border-bottom-left-radius: 3px; }
.msg-thinking  { color: var(--text-2); font-style: italic; font-size: 12px; align-self: flex-start; }
```

Messages container: `display: flex; flex-direction: column; gap: 12px; overflow-y: auto`.

**Auto-growing textarea** (capped at 120px):

```js
textarea.addEventListener('input', function () {
  this.style.height = 'auto';
  this.style.height = Math.min(this.scrollHeight, 120) + 'px';
});
```

Enter submits; Shift+Enter inserts a newline. Escape closes the sidebar.

---

## Custom date picker

No library. A `<div>` acting as a trigger opens a JS-rendered calendar popup.

```html
<div class="date-picker-wrap">
  <div class="date-picker-input placeholder" id="display-start">Select date</div>
  <div class="cal-popup" id="cal-start"></div>
</div>
```

**CSS classes**:

| Class | Style |
|---|---|
| `.date-picker-input` | `border: 1px solid --border; border-radius: 6px; padding: 5px 10px; cursor: pointer` |
| `.date-picker-input:hover` | `border-color: #9ca3af` |
| `.date-picker-input.placeholder` | `color: --text-2` |
| `.cal-popup` | `display: none`; `.open` → `display: block`. Absolute, `z-index: 200`, `--shadow-md` |
| `.cal-grid` | `display: grid; grid-template-columns: repeat(7, 1fr); gap: 2px` |
| `.cal-dow` | 10px, uppercase, `--text-2` |
| `.cal-day` | 12px, `border-radius: 6px`, hoverable |
| `.cal-empty` | `visibility: hidden` |
| `.cal-weekend` / `.cal-non-trading` | `color: --border; cursor: default` |
| `.cal-selected` | `background: --accent; color: #fff; font-weight: 600` |
| `.cal-in-range` | `background: --accent-bg; color: --accent` |
| `.cal-today` | `font-weight: 700` |
| `.cal-nav` | Borderless button, hover `background: --surface-2` |

Clicking outside closes all open popups via a `document` click listener.

---

## Responsive

```css
@media (max-width: 768px) {
  .app-header    { padding: 0 16px; gap: 16px; }
  .main          { padding: 16px; }
  .controls-card { gap: 20px; padding: 16px; }
}
```

Sidebar uses `max-width: 100vw` so it doesn't overflow on narrow screens.
