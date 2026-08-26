create table if not exists public.runs (
  run_id text primary key,
  scenario text not null,
  status text not null,
  created_at timestamptz not null,
  updated_at timestamptz not null,
  outcome text
);

create table if not exists public.workflow_events (
  id text primary key,
  run_id text not null references public.runs(run_id) on delete cascade,
  ts timestamptz not null,
  kind text not null,
  title text not null,
  message text not null,
  payload jsonb
);

create index if not exists workflow_events_run_id_ts_idx on public.workflow_events(run_id, ts);
alter table public.runs enable row level security;
alter table public.workflow_events enable row level security;
