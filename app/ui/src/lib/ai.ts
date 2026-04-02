import { invoke } from '@tauri-apps/api/core';

export type AiSettings = {
  apiKey: string;
  endpoint: string;
};

export type ChatMessage = {
  role: string;
  content: string;
};

export type ChatCompletionRequest = {
  model: string;
  messages: ChatMessage[];
  temperature?: number;
  maxTokens?: number;
};

export type ChatCompletionChoice = {
  index: number;
  message: ChatMessage;
  finishReason?: string;
};

export type ChatCompletionResponse = {
  id?: string;
  model?: string;
  choices: ChatCompletionChoice[];
};

export async function loadAiSettings(): Promise<AiSettings | null> {
  return invoke<AiSettings | null>('load_ai_settings');
}

export async function saveAiSettings(settings: AiSettings): Promise<void> {
  await invoke('save_ai_settings', { settings });
}

export async function completeAiChat(
  request: ChatCompletionRequest
): Promise<ChatCompletionResponse> {
  return invoke<ChatCompletionResponse>('complete_ai_chat', { request });
}

const QUERY_GENERATION_MODEL = 'gpt-4.1-mini';
const ERROR_EXPLANATION_MODEL = 'gpt-4.1-mini';

const LITESQRL_LIMITATIONS = [
  'LiteSQRL currently supports CREATE TABLE, ALTER TABLE ADD COLUMN, ALTER TABLE DROP COLUMN, ALTER TABLE RENAME TO, INSERT ... VALUES, SELECT ... FROM ... with optional WHERE, UPDATE ... SET ... WHERE, DELETE ... WHERE, TRUNCATE TABLE, and DROP TABLE.',
  'Many common SQL features are not implemented yet. In particular, be cautious about JOIN, GROUP BY, HAVING, ORDER BY, LIMIT, INSERT ... SELECT, and other advanced dialect features.',
  'If the error looks like unsupported syntax or an unimplemented feature, say that explicitly and suggest a rewrite using only supported LiteSQRL syntax.'
].join(' ');

function stripMarkdownFences(content: string): string {
  const trimmed = content.trim();
  const fenced = trimmed.match(/^```(?:sql)?\s*([\s\S]*?)\s*```$/i);
  return fenced?.[1]?.trim() ?? trimmed;
}

export async function generateQueryFromPrompt(
  prompt: string,
  currentQuery?: string
): Promise<string> {
  const context = currentQuery?.trim()
    ? `Current draft SQL:\n${currentQuery.trim()}`
    : 'There is no current draft SQL.';

  const response = await completeAiChat({
    model: QUERY_GENERATION_MODEL,
    temperature: 0.2,
    messages: [
      {
        role: 'system',
        content:
          'You generate LiteSQRL SQL for the editor. Use the available tools when schema context is needed. Return only the final SQL query text with no markdown fences, no explanation, and no surrounding commentary.'
      },
      {
        role: 'user',
        content: `${context}\n\nInstruction:\n${prompt.trim()}`
      }
    ]
  });

  const content = response.choices[0]?.message.content ?? '';
  return stripMarkdownFences(content);
}

export async function explainQueryError(
  currentQuery: string,
  errorMessage: string
): Promise<string> {
  const response = await completeAiChat({
    model: ERROR_EXPLANATION_MODEL,
    temperature: 0.2,
    messages: [
      {
        role: 'system',
        content: `You explain LiteSQRL query failures. ${LITESQRL_LIMITATIONS} Use the available tools when schema inspection or small read-only diagnostic SELECT queries would improve the diagnosis. Explain the likely cause, say clearly if LiteSQRL likely does not support the attempted syntax, and suggest a concrete next query or rewrite. Keep the answer concise and practical.`
      },
      {
        role: 'user',
        content: `Query:\n${currentQuery.trim() || '(empty query)'}\n\nError:\n${errorMessage.trim()}`
      }
    ]
  });

  return response.choices[0]?.message.content?.trim() || 'No explanation was returned.';
}