<script lang="ts">
  import Icon from '@iconify/svelte';
  import { invoke } from '@tauri-apps/api/core';
  import { onMount, tick } from 'svelte';
  import {
    explainQueryError,
    generateQueryFromPrompt,
    loadAiSettings,
    saveAiSettings,
    type AiSettings
  } from '$lib/ai';
  import QueryEditor from '$lib/QueryEditor.svelte';
  import QueryRowsResult from '$lib/QueryRowsResult.svelte';

  type QueryResult =
    | { type: 'Success' }
    | { type: 'Count'; data: number }
    | { type: 'Rows'; data: { columns: string[]; rows: string[][] } }
    | { type: 'Err'; data: string };

  let query = $state('');
  let results = $state<QueryResult[]>([]);
  let running = $state(false);
  let elapsedMs = $state<number | null>(null);
  let settingsDialog = $state<HTMLDialogElement | null>(null);
  let promptDialog = $state<HTMLDialogElement | null>(null);
  let promptInput = $state<HTMLTextAreaElement | null>(null);
  let aiSettings = $state<AiSettings>({ apiKey: '', endpoint: '' });
  let settingsLoading = $state(true);
  let settingsSaving = $state(false);
  let settingsError = $state('');
  let settingsNotice = $state('');
  let promptText = $state('');
  let promptError = $state('');
  let generatingQuery = $state(false);
  let explanationDialog = $state<HTMLDialogElement | null>(null);
  let explainingError = $state(false);
  let explanationError = $state('');
  let errorExplanation = $state('');
  let selectedError = $state('');

  onMount(async () => {
    try {
      const saved = await loadAiSettings();
      if (saved) {
        aiSettings.apiKey = saved.apiKey;
        aiSettings.endpoint = saved.endpoint;
      }
    } catch (error) {
      settingsError = error instanceof Error ? error.message : 'Failed to load AI settings.';
    } finally {
      settingsLoading = false;
    }
  });

  async function run() {
    if (!query.trim()) return;
    const startedAt = performance.now();
    running = true;
    results = await invoke<QueryResult[]>('run_query', { src: query });
    elapsedMs = Math.round(performance.now() - startedAt);
    running = false;
  }

  function openSettings() {
    settingsError = '';
    settingsNotice = '';
    settingsDialog?.showModal();
  }

  async function openPromptDialog() {
    promptError = '';
    promptDialog?.showModal();
    await tick();
    promptInput?.focus();
  }

  async function generateQuery() {
    promptError = '';

    if (!promptText.trim()) {
      promptError = 'Prompt is required.';
      return;
    }

    generatingQuery = true;

    try {
      const generatedQuery = await generateQueryFromPrompt(promptText, query);
      if (!generatedQuery.trim()) {
        throw new Error('The AI response did not contain SQL.');
      }
      query = generatedQuery;
      promptDialog?.close();
    } catch (error) {
      promptError = error instanceof Error ? error.message : 'Failed to generate a query.';
    } finally {
      generatingQuery = false;
    }
  }

  async function explainError(errorMessage: string) {
    selectedError = errorMessage;
    errorExplanation = '';
    explanationError = '';
    explainingError = true;
    explanationDialog?.showModal();

    try {
      errorExplanation = await explainQueryError(query, errorMessage);
    } catch (error) {
      explanationError = error instanceof Error ? error.message : 'Failed to explain the error.';
    } finally {
      explainingError = false;
    }
  }

  async function saveSettings() {
    settingsError = '';
    settingsNotice = '';

    if (!aiSettings.apiKey.trim()) {
      settingsError = 'API key is required.';
      return;
    }

    if (!aiSettings.endpoint.trim()) {
      settingsError = 'Endpoint is required.';
      return;
    }

    settingsSaving = true;

    try {
      await saveAiSettings({
        apiKey: aiSettings.apiKey.trim(),
        endpoint: aiSettings.endpoint.trim()
      });
      settingsNotice = 'Saved.';
      settingsDialog?.close();
    } catch (error) {
      settingsError = error instanceof Error ? error.message : 'Failed to save AI settings.';
    } finally {
      settingsSaving = false;
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
        <QueryEditor bind:value={query} onrun={run} onprompt={openPromptDialog} />
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
              {:else if result.type === 'Err'}
                <div class="alert alert-error alert-soft flex items-start justify-between gap-3 px-2 py-1">
                  <span>{result.data}</span>
                  <button class="btn btn-xs btn-ghost" onclick={() => explainError(result.data)}>
                    Explain
                  </button>
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
      <span class="text-base-content/30">|</span>
      <kbd class="kbd kbd-sm">⌘</kbd>
      <span>+</span>
      <kbd class="kbd kbd-sm">I</kbd>
      <span class="text-base-content/70">to prompt AI</span>
    </div>
    <div class="flex items-center gap-2">
      {#if settingsNotice}
        <span class="text-success text-xs">{settingsNotice}</span>
      {/if}
      <div class="text-base-content/70 min-w-16 text-right font-mono">
        {#if running}
          Running...
        {:else if elapsedMs !== null}
          {elapsedMs} ms
        {:else}
          -- ms
        {/if}
      </div>
      <button
        class="btn btn-ghost btn-xs btn-square"
        aria-label="Open AI settings"
        onclick={openSettings}
      >
        <Icon icon="lucide:settings-2" width={14} height={14} />
      </button>
    </div>
  </div>

  <dialog bind:this={settingsDialog} class="modal">
    <div class="modal-box max-w-md">
      <h3 class="text-base font-semibold">Generative AI</h3>
      <p class="text-base-content/70 mt-2 text-sm">
        Configure the API key and chat completions endpoint for the GUI assistant.
      </p>

      <div class="mt-4 space-y-4">
        <fieldset class="fieldset">
          <legend class="fieldset-legend">Endpoint</legend>
          <input
            class="input w-full font-mono text-xs"
            type="url"
            placeholder="https://api.openai.com/v1/chat/completions"
            bind:value={aiSettings.endpoint}
            autocomplete="off"
            disabled={settingsLoading || settingsSaving}
          />
          <p class="label">Saved to the app settings file as entered.</p>
        </fieldset>

        <fieldset class="fieldset">
          <legend class="fieldset-legend">API Key</legend>
          <input
            class="input w-full"
            type="password"
            placeholder="sk-..."
            bind:value={aiSettings.apiKey}
            autocomplete="off"
            disabled={settingsLoading || settingsSaving}
          />
          <p class="label">Stored locally for this app.</p>
        </fieldset>

        {#if settingsError}
          <div role="alert" class="alert alert-error alert-soft text-sm">{settingsError}</div>
        {/if}
      </div>

      <div class="modal-action">
        <form method="dialog">
          <button class="btn btn-ghost">Cancel</button>
        </form>
        <button class="btn btn-primary" onclick={saveSettings} disabled={settingsLoading || settingsSaving}>
          {#if settingsSaving}
            <span class="loading loading-spinner loading-xs"></span>
          {:else}
            Save
          {/if}
        </button>
      </div>
    </div>
    <form method="dialog" class="modal-backdrop">
      <button>close</button>
    </form>
  </dialog>

  <dialog bind:this={promptDialog} class="modal">
    <div class="modal-box max-w-2xl">
      <h3 class="text-base font-semibold">Generate Query</h3>
      <p class="text-base-content/70 mt-2 text-sm">
        Describe the query you want. The AI can inspect the current database DDL when needed.
      </p>

      <div class="mt-4 space-y-4">
        <fieldset class="fieldset">
          <legend class="fieldset-legend">Prompt</legend>
          <textarea
            bind:this={promptInput}
            class="textarea h-36 w-full"
            placeholder="List the last 20 inserted rows from orders with customer email and total amount."
            bind:value={promptText}
            disabled={generatingQuery}
          ></textarea>
          <p class="label">The generated SQL will replace the current editor contents.</p>
        </fieldset>

        {#if promptError}
          <div role="alert" class="alert alert-error alert-soft text-sm">{promptError}</div>
        {/if}
      </div>

      <div class="modal-action">
        <form method="dialog">
          <button class="btn btn-ghost" disabled={generatingQuery}>Cancel</button>
        </form>
        <button class="btn btn-primary" onclick={generateQuery} disabled={generatingQuery}>
          {#if generatingQuery}
            <span class="loading loading-spinner loading-xs"></span>
          {:else}
            Generate
          {/if}
        </button>
      </div>
    </div>
    <form method="dialog" class="modal-backdrop">
      <button disabled={generatingQuery}>close</button>
    </form>
  </dialog>

  <dialog bind:this={explanationDialog} class="modal">
    <div class="modal-box max-w-2xl">
      <h3 class="text-base font-semibold">Why This Error Happened</h3>
      <p class="text-base-content/70 mt-2 text-sm">
        The AI can inspect the current schema and run small read-only diagnostic queries if needed.
      </p>

      <div class="mt-4 space-y-4">
        <fieldset class="fieldset">
          <legend class="fieldset-legend">Error</legend>
          <div class="bg-base-200 rounded-box px-3 py-2 text-sm">{selectedError}</div>
        </fieldset>

        {#if explainingError}
          <div class="flex items-center gap-2 text-sm">
            <span class="loading loading-spinner loading-sm"></span>
            <span>Diagnosing the error...</span>
          </div>
        {:else if explanationError}
          <div role="alert" class="alert alert-error alert-soft text-sm">{explanationError}</div>
        {:else if errorExplanation}
          <div class="bg-base-200 rounded-box whitespace-pre-wrap px-3 py-3 text-sm leading-6">
            {errorExplanation}
          </div>
        {/if}
      </div>

      <div class="modal-action">
        <form method="dialog">
          <button class="btn btn-ghost" disabled={explainingError}>Close</button>
        </form>
      </div>
    </div>
    <form method="dialog" class="modal-backdrop">
      <button disabled={explainingError}>close</button>
    </form>
  </dialog>
</div>
