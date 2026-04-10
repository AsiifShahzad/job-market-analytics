# Frontend - React Dashboard

React 18 + Vite + Tailwind CSS dashboard for job market analytics visualization and exploration.

## Architecture

- **React 18**: UI framework
- **TypeScript**: Type-safe development
- **Vite 5**: Lightning-fast build tool and dev server
- **TanStack Query v5**: Server state management with caching and polling
- **Recharts**: Data visualization
- **Tailwind CSS v3**: Utility-first styling
- **React Router v6**: Client-side routing
- **Zustand**: Global state management for filters
- **shadcn/ui patterns**: Accessible, unstyled component patterns

## Features

### Pages

1. **Dashboard** (`/`)
   - Metric cards: Total jobs, unique skills, cities covered, last run time
   - Top 15 skills by demand (horizontal bar chart)
   - Emerging skills table with growth % and TF-IDF scores

2. **Skills Explorer** (`/skills`)
   - Sidebar filters: City, Country, Seniority, Category
   - Skills grid with cards showing job count, salary, TF-IDF score
   - Skill detail drawer with:
     - Trend chart (dual-axis: job count + TF-IDF)
     - Co-occurring skills
     - Salary bands (P25, P50, P75)

3. **Salary Insights** (`/salaries`)
   - Job title filter
   - Salary distribution chart
   - High/low premium skills tables
   - Complete skill premium analysis with delta coloring

4. **Pipeline Monitor** (`/pipeline`)
   - Runs table with status, timestamps, job counts
   - Trigger pipeline button
   - Live status panel with:
     - Polling every 3 seconds when running
     - Optional Server-Sent Events (SSE) log streaming
     - Metrics: jobs fetched, inserted, unique skills
     - Progress indicator

## Setup

### Prerequisites

- Node.js 18+
- npm or yarn
- Backend API running at `http://localhost:8000`

### Installation

```bash
cd frontend
npm install
```

### Configuration

Create `.env.local`:

```env
VITE_API_URL=http://localhost:8000
```

Or use `.env.example` as a template:

```bash
cp .env.example .env.local
```

### Development

```bash
npm run dev
```

Open `http://localhost:5173` in your browser.

### Build

```bash
npm run build
```

Output in `dist/` ready for production deployment.

### Preview

```bash
npm run preview
```

## Project Structure

```
frontend/
├── src/
│   ├── api/
│   │   ├── client.ts          # Typed fetch wrapper
│   │   └── hooks/
│   │       ├── useSkills.ts    # Skills data fetching
│   │       ├── useSalaries.ts  # Salary data fetching
│   │       ├── useTrends.ts    # Trends data fetching
│   │       └── usePipeline.ts  # Pipeline polling & SSE
│   ├── components/
│   │   ├── Navbar.tsx         # Top navigation
│   │   └── Sidebar.tsx        # Filter sidebar
│   ├── pages/
│   │   ├── Dashboard.tsx      # Home page
│   │   ├── SkillsExplorer.tsx # Skills catalog
│   │   ├── SalaryInsights.tsx # Compensation analysis
│   │   └── PipelineMonitor.tsx # ETL monitoring
│   ├── stores/
│   │   └── filterStore.ts     # Zustand filter state
│   ├── types/
│   │   └── api.ts             # TypeScript interfaces
│   ├── utils/
│   │   ├── chartColors.ts     # Color mapping by category
│   │   └── formatters.ts      # Number/date formatting
│   ├── App.tsx                # Root component with routing
│   ├── main.tsx               # Entry point
│   └── index.css              # Global styles
├── index.html                 # HTML template
├── package.json
├── tsconfig.json
├── tailwind.config.ts
├── postcss.config.js
└── vite.config.ts
```

## Key Features

### Type Safety

All API responses are strictly typed via `src/types/api.ts`. No `any` types used.

### Responsive Design

- Mobile-first approach
- Sidebar collapses to bottom navigation on screens < 768px
- Touch-friendly controls on mobile

### Data Fetching

- **TanStack Query v5**: Automatic caching (5-min stale time)
- **Polling**: Pipeline status polls every 3 seconds when running
- **Optional SSE**: Log streaming via Server-Sent Events (falls back to polling)
- **Error handling**: Toast notifications on failed requests

### Performance

- Code splitting via Vite
- Lazy loading of pages via React Router
- Chart responsiveness with Recharts
- Efficient re-renders with React Query

### State Management

- Global filters via Zustand (city, country, seniority, category)
- Persistent across navigation
- Local component state for UI interactions

## API Integration

All API calls go through `src/api/client.ts`:

```typescript
// Get skills with filters
const { data, isLoading } = useSkills({
  city: 'San Francisco',
  country: 'USA',
  limit: 100
})

// Trigger pipeline and get live status
const [runId, setRunId] = useState<number | null>(null)
const status = usePipelineStatus(runId, 3000) // poll every 3s

// Stream logs via SSE
const { logs, isConnected } = usePipelineLogsSSE(runId)
```

## Styling

### Tailwind CSS

- Custom colors for skill categories in `tailwind.config.ts`
- Responsive utilities: `sm:`, `md:`, `lg:` prefixes
- Custom animations: `animate-pulse-fast` for running status

### Theme

- **Language**: Purple (`#9333ea`)
- **Framework**: Teal (`#14b8a6`)
- **Cloud**: Amber (`#f59e0b`)
- **Tool**: Gray (`#6b7280`)
- **Data**: Blue (`#3b82f6`)
- **Soft**: Pink (`#ec4899`)

## Browser Support

- Chrome/Edge (latest)
- Firefox (latest)
- Safari (latest)
- Mobile browsers (iOS Safari, Chrome Android)

## Performance Tips

1. **Reduce polling interval** in Pipeline Monitor if needed (default 3s)
2. **Adjust stale time**: Change `STALE_TIME` in hook files
3. **Enable SSE** on backend for real-time log streaming (optional)
4. **Use production build** for faster page loads

## Troubleshooting

### API Connection Issues

Check `VITE_API_URL` in `.env.local` points to backend:

```bash
# Should return 200
curl http://localhost:8000/api/skills
```

### Vite Proxy Issues

Vite proxies `/api` to backend. If blocked, add to `vite.config.ts`:

```typescript
proxy: {
  '/api': {
    target: 'http://localhost:8000',
    changeOrigin: true,
  }
}
```

### Missing Data

- Verify backend is running: `http://localhost:8000/api/skills`
- Check Network tab in DevTools
- Review error toast messages

### SSE Not Working

Pipeline Monitor falls back to polling if SSE endpoint unavailable:

```bash
# Test SSE
curl -N http://localhost:8000/api/pipeline/1/logs
```

## License

MIT
