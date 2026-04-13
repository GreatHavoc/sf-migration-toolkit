# Frontend

Next.js 16 + Ant Design UI for the Snowflake migration control center.

## Project Structure and UX
 
 The UI has been refactored into a guided Context-driven Wizard to eliminate friction and redundant actions:
 
 - `src/components/dashboard/DashboardClient.tsx`: Top-level shell that renders the Ant Design `<Steps>` progress indicator and the `<DashboardProvider>`.
 - `src/components/dashboard/DashboardContext.tsx`: React Context holding all Snowflake connections, configuration state, history, and unified navigation actions.
 - `src/components/dashboard/steps/`:
   - `Step1Connections.tsx`: Handles unified source/target testing and frictionless MFA passcodes.
   - `Step2SetupAndAnalysis.tsx`: Provides One-Click setup mapping Azure infrastructure and schema prechecking.
   - `Step3MigrationRun.tsx`: Phase selection, dry-run toggling, and job launch.
   - `Step4MonitorAndHistory.tsx`: Real-time SSE monitor and historical job review.
 
 ## Package manager
 
 This frontend uses `pnpm`.

## Local development

```bash
corepack enable
corepack prepare pnpm@10.33.0 --activate
pnpm install
pnpm dev --hostname 0.0.0.0 --port 3000
```

Set API base URL when needed:

```bash
set NEXT_PUBLIC_API_BASE_URL=http://localhost:8000/api
```
