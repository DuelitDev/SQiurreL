<script lang="ts">
  import Icon from '@iconify/svelte';
  import { invoke } from '@tauri-apps/api/core';
  import QueryEditor from '$lib/QueryEditor.svelte';
  import QueryRowsResult from '$lib/QueryRowsResult.svelte';

  type QueryResult =
    | { type: 'Success' }
    | { type: 'Count'; data: number }
    | { type: 'Rows'; data: { columns: string[]; rows: string[][] } }
    | { type: 'Error'; data: string };

  let query = $state('');
  let results = $state<QueryResult[]>([]);
  let running = $state(false);
  let elapsedMs = $state<number | null>(null);

  async function run() {
    if (!query.trim()) return;
    const startedAt = performance.now();
    running = true;
    try {
      results = await invoke<QueryResult[]>('run_query', { src: query });
    } catch (e) {
      results = [{ type: 'Error', data: String(e) }];
    } finally {
      elapsedMs = Math.round(performance.now() - startedAt);
      running = false;
    }
  }
</script>

<div class="flex h-screen flex-col">
  <div class="flex min-h-0 flex-1">
    <!-- Left Pane: Query Editor -->
    <div class="border-base-300 flex w-1/2 flex-col border-r">
      <div class="border-base-300 flex items-center border-b px-4 py-2">
        <h2 class="text-md font-semibold">Query</h2>
      </div>
      <div class="relative flex-1">
        <QueryEditor bind:value={query} onrun={run} />
        <div class="pointer-events-none absolute right-4 bottom-4 z-10">
          <button
            class="btn btn-success btn-circle btn-md pointer-events-auto shadow-lg"
            disabled={running || !query.trim()}
            onclick={run}
          >
            {#if running}
              <span class="loading loading-spinner loading-xs"></span>
            {:else}
              <Icon icon="lucide:play" width={20} height={20} />
            {/if}
          </button>
        </div>
      </div>
    </div>
    <!-- Right: Result -->
    <div class="flex w-1/2 flex-col">
      <div class="border-base-300 border-b px-4 py-2">
        <h2 class="text-md font-semibold">Result</h2>
      </div>
      <div class="flex-1 overflow-auto p-4 font-mono text-sm">
        {#if results.length === 0}
          <span class="text-base-content/40">Run a query to see results.</span>
        {:else}
          {#each results as result, i (i)}
            <div class="mb-2">
              {#if result.type === 'Success'}
                <div class="alert alert-success alert-soft px-2 py-1">OK</div>
              {:else if result.type === 'Count'}
                <div class="alert alert-success alert-soft px-2 py-1">
                  {result.data} row{result.data === 1 ? '' : 's'} affected
                </div>
              {:else if result.type === 'Rows'}
                <QueryRowsResult cols={result.data.columns} rows={result.data.rows} />
              {:else if result.type === 'Error'}
                <div class="alert alert-error alert-soft px-2 py-1">
                  {result.data}
                </div>
              {/if}
            </div>
          {/each}
        {/if}
      </div>
    </div>
  </div>

  <div
    class="border-base-300 bg-base-200 text-base-content flex items-center justify-between border-t px-4 py-2 text-sm"
  >
    <div class="flex items-center gap-2 font-medium">
      <kbd class="kbd kbd-sm">⌘</kbd>
      <span>+</span>
      <kbd class="kbd kbd-sm">Enter</kbd>
      <span class="text-base-content/70">to run query</span>
    </div>
    <div class="text-base-content/70 font-mono">
      {#if running}
        Running...
      {:else if elapsedMs !== null}
        {elapsedMs} ms
      {:else}
        -- ms
      {/if}
    </div>
  </div>
</div>
