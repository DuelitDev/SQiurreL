use litesqrl::executor::{Executor, QueryResult};
use litesqrl::query::{Lexer, Parser, Stmt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Mutex;
use tauri::{AppHandle, Manager, Runtime};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiSettings {
    pub api_key: String,
    pub endpoint: String,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub lang: String,
}

impl Default for AiSettings {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            endpoint: String::new(),
            model: String::new(),
            lang: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatCompletionRequest {
    #[serde(default)]
    pub model: String,
    pub messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatCompletionChoice {
    pub index: usize,
    pub message: ChatMessage,
    #[serde(default)]
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatCompletionResponse {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    pub choices: Vec<ChatCompletionChoice>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OpenAiMessage {
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(rename = "tool_call_id", skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
    #[serde(rename = "tool_calls", skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<OpenAiToolCall>>,
}

impl From<ChatMessage> for OpenAiMessage {
    fn from(value: ChatMessage) -> Self {
        Self {
            role: value.role,
            content: Some(value.content),
            tool_call_id: None,
            tool_calls: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OpenAiToolCall {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    function: OpenAiFunctionCall,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OpenAiFunctionCall {
    name: String,
    arguments: String,
}

#[derive(Debug, Deserialize)]
struct RunDiagnosticQueryArgs {
    sql: String,
}

#[derive(Debug, Serialize)]
struct OpenAiCompatPayload<'a> {
    model: &'a str,
    messages: &'a [OpenAiMessage],
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(rename = "max_tokens", skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<&'a [OpenAiToolSpec]>,
}

#[derive(Debug, Clone, Serialize)]
struct OpenAiToolSpec {
    #[serde(rename = "type")]
    kind: &'static str,
    function: OpenAiToolFunctionSpec,
}

#[derive(Debug, Clone, Serialize)]
struct OpenAiToolFunctionSpec {
    name: &'static str,
    description: &'static str,
    parameters: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct OpenAiCompatErrorEnvelope {
    error: OpenAiCompatError,
}

#[derive(Debug, Deserialize)]
struct OpenAiCompatError {
    message: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiCompatResponse {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    choices: Vec<OpenAiCompatChoice>,
}

#[derive(Debug, Deserialize)]
struct OpenAiModelsResponse {
    data: Vec<OpenAiModel>,
}

#[derive(Debug, Deserialize)]
struct OpenAiModel {
    id: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiCompatChoice {
    index: usize,
    message: OpenAiMessage,
    #[serde(default)]
    finish_reason: Option<String>,
}

pub struct OpenAiCompatClient {
    http: reqwest::Client,
    settings: AiSettings,
}

impl OpenAiCompatClient {
    pub fn new(settings: AiSettings) -> Self {
        Self { http: reqwest::Client::new(), settings }
    }

    async fn chat_completion_raw(
        &self,
        model: &str,
        messages: &[OpenAiMessage],
        temperature: Option<f32>,
        max_tokens: Option<u32>,
        tools: Option<&[OpenAiToolSpec]>,
    ) -> Result<OpenAiCompatResponse, String> {
        if self.settings.api_key.trim().is_empty() {
            return Err("AI API key is not configured.".to_string());
        }
        if model.trim().is_empty() {
            return Err("Chat completion model is required.".to_string());
        }
        if messages.is_empty() {
            return Err("At least one chat message is required.".to_string());
        }

        let endpoint = self.settings.endpoint.trim();
        if endpoint.is_empty() {
            return Err("AI endpoint is not configured.".to_string());
        }

        let payload =
            OpenAiCompatPayload { model, messages, temperature, max_tokens, tools };

        let response = self
            .http
            .post(endpoint)
            .bearer_auth(self.settings.api_key.trim())
            .json(&payload)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.map_err(|e| e.to_string())?;
            let message = serde_json::from_str::<OpenAiCompatErrorEnvelope>(&body)
                .map(|parsed| parsed.error.message)
                .unwrap_or(body);
            return Err(format!("AI request failed with {}: {}", status, message));
        }

        response.json::<OpenAiCompatResponse>().await.map_err(|e| e.to_string())
    }

    async fn list_models(&self) -> Result<Vec<String>, String> {
        if self.settings.api_key.trim().is_empty() {
            return Err("AI API key is not configured.".to_string());
        }

        let models_endpoint = models_endpoint(&self.settings.endpoint)?;

        let response = self
            .http
            .get(&models_endpoint)
            .bearer_auth(self.settings.api_key.trim())
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.map_err(|e| e.to_string())?;
            let message = serde_json::from_str::<OpenAiCompatErrorEnvelope>(&body)
                .map(|parsed| parsed.error.message)
                .unwrap_or(body);
            return Err(format!(
                "AI models request failed with {}: {}",
                status, message
            ));
        }

        let mut models = response
            .json::<OpenAiModelsResponse>()
            .await
            .map_err(|e| e.to_string())?
            .data
            .into_iter()
            .map(|model| model.id)
            .collect::<Vec<_>>();
        models.sort();
        models.dedup();
        Ok(models)
    }
}

fn models_endpoint(endpoint: &str) -> Result<String, String> {
    let trimmed = endpoint.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err("AI endpoint is not configured.".to_string());
    }

    if let Some(prefix) = trimmed.strip_suffix("/chat/completions") {
        return Ok(format!("{prefix}/models"));
    }

    Ok(format!("{trimmed}/models"))
}

fn public_response_from_raw(response: OpenAiCompatResponse) -> ChatCompletionResponse {
    ChatCompletionResponse {
        id: response.id,
        model: response.model,
        choices: response
            .choices
            .into_iter()
            .map(|choice| ChatCompletionChoice {
                index: choice.index,
                message: ChatMessage {
                    role: choice.message.role,
                    content: choice.message.content.unwrap_or_default(),
                },
                finish_reason: choice.finish_reason,
            })
            .collect(),
    }
}

fn ai_tools() -> Vec<OpenAiToolSpec> {
    vec![
        OpenAiToolSpec {
            kind: "function",
            function: OpenAiToolFunctionSpec {
                name: "get_database_ddl",
                description: "Return the current database schema as CREATE TABLE DDL statements.",
                parameters: json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
            },
        },
        OpenAiToolSpec {
            kind: "function",
            function: OpenAiToolFunctionSpec {
                name: "run_diagnostic_query",
                description: "Execute a read-only LiteSQRL SELECT query to inspect the current database for debugging. Only SELECT statements are allowed.",
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "sql": {
                            "type": "string",
                            "description": "A LiteSQRL SELECT query used only for diagnostics. Keep it small and focused."
                        }
                    },
                    "required": ["sql"],
                    "additionalProperties": false
                }),
            },
        },
    ]
}

fn format_diagnostic_results(results: Vec<QueryResult>) -> Result<String, String> {
    let rendered = results
        .into_iter()
        .map(|result| match result {
            QueryResult::Success => json!({ "type": "success" }),
            QueryResult::Count(count) => json!({ "type": "count", "count": count }),
            QueryResult::Rows { columns, rows } => {
                let total_rows = rows.len();
                let preview_rows = rows.into_iter().take(50).collect::<Vec<_>>();
                json!({
                    "type": "rows",
                    "columns": columns,
                    "rowCount": total_rows,
                    "truncated": total_rows > preview_rows.len(),
                    "rows": preview_rows,
                })
            }
            QueryResult::Err(message) => json!({ "type": "error", "message": message }),
        })
        .collect::<Vec<_>>();

    serde_json::to_string_pretty(&rendered).map_err(|e| e.to_string())
}

fn run_diagnostic_query(
    sql: &str,
    executor: &Mutex<Executor>,
) -> Result<String, String> {
    if sql.trim().is_empty() {
        return Err("Diagnostic SQL is required.".to_string());
    }

    let lexer = Lexer::new(sql);
    let mut parser = Parser::new(lexer).map_err(|e| e.to_string())?;
    let statements = parser.parse().map_err(|e| e.to_string())?;

    if statements.is_empty() {
        return Err("Diagnostic SQL did not produce any statements.".to_string());
    }

    if statements.iter().any(|statement| !matches!(statement.stmt, Stmt::Select { .. }))
    {
        return Err(
            "Only SELECT statements are allowed in diagnostic queries.".to_string()
        );
    }

    let mut executor = executor.lock().map_err(|e| e.to_string())?;
    let mut results = Vec::with_capacity(statements.len());
    for statement in statements {
        results.push(executor.run(statement.stmt).map_err(|e| e.to_string())?);
    }

    format_diagnostic_results(results)
}

fn execute_tool_call(
    tool_call: &OpenAiToolCall,
    executor: &Mutex<Executor>,
) -> Result<String, String> {
    if tool_call.kind != "function" {
        return Err(format!("Unsupported tool call type: {}", tool_call.kind));
    }

    match tool_call.function.name.as_str() {
        "get_database_ddl" => {
            if !tool_call.function.arguments.trim().is_empty() {
                serde_json::from_str::<serde_json::Value>(
                    &tool_call.function.arguments,
                )
                .map_err(|e| format!("Invalid tool arguments: {e}"))?;
            }
            let executor = executor.lock().map_err(|e| e.to_string())?;
            Ok(executor.schema_ddl())
        }
        "run_diagnostic_query" => {
            let args = serde_json::from_str::<RunDiagnosticQueryArgs>(
                &tool_call.function.arguments,
            )
            .map_err(|e| format!("Invalid tool arguments: {e}"))?;
            run_diagnostic_query(&args.sql, executor)
        }
        name => Err(format!("Unknown AI tool: {}", name)),
    }
}

fn ai_settings_path<R: Runtime>(
    app: &AppHandle<R>,
) -> tauri::Result<std::path::PathBuf> {
    let data_dir = app.path().app_data_dir()?;
    std::fs::create_dir_all(&data_dir)?;
    Ok(data_dir.join("ai-settings.json"))
}

pub fn load_ai_settings<R: Runtime>(
    app: &AppHandle<R>,
) -> Result<Option<AiSettings>, String> {
    let path = ai_settings_path(app).map_err(|e| e.to_string())?;
    if !path.exists() {
        return Ok(None);
    }

    let raw = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let settings =
        serde_json::from_str::<AiSettings>(&raw).map_err(|e| e.to_string())?;
    Ok(Some(settings))
}

pub fn save_ai_settings<R: Runtime>(
    app: &AppHandle<R>,
    settings: AiSettings,
) -> Result<(), String> {
    let path = ai_settings_path(app).map_err(|e| e.to_string())?;
    let normalized = AiSettings {
        api_key: settings.api_key.trim().to_string(),
        endpoint: settings.endpoint.trim().to_string(),
        model: settings.model.trim().to_string(),
        lang: settings.lang.trim().to_string(),
    };
    let raw = serde_json::to_string_pretty(&normalized).map_err(|e| e.to_string())?;
    std::fs::write(path, raw).map_err(|e| e.to_string())
}

pub async fn list_models_with_saved_settings<R: Runtime>(
    app: &AppHandle<R>,
    settings_override: AiSettings,
) -> Result<Vec<String>, String> {
    let saved = load_ai_settings(app)?.unwrap_or_default();
    let settings = AiSettings {
        api_key: if settings_override.api_key.trim().is_empty() {
            saved.api_key
        } else {
            settings_override.api_key.trim().to_string()
        },
        endpoint: if settings_override.endpoint.trim().is_empty() {
            saved.endpoint
        } else {
            settings_override.endpoint.trim().to_string()
        },
        model: if settings_override.model.trim().is_empty() {
            saved.model
        } else {
            settings_override.model.trim().to_string()
        },
        lang: if settings_override.lang.trim().is_empty() {
            saved.lang
        } else {
            settings_override.lang.trim().to_string()
        },
    };

    OpenAiCompatClient::new(settings).list_models().await
}

pub async fn complete_chat_with_saved_settings<R: Runtime>(
    app: &AppHandle<R>,
    executor: &Mutex<Executor>,
    request: ChatCompletionRequest,
) -> Result<ChatCompletionResponse, String> {
    let settings = load_ai_settings(app)?
        .filter(|settings| !settings.api_key.trim().is_empty())
        .ok_or_else(|| "AI API key is not configured.".to_string())?;

    let client = OpenAiCompatClient::new(settings);
    let tools = ai_tools();
    let model = if request.model.trim().is_empty() {
        client.settings.model.trim().to_string()
    } else {
        request.model.trim().to_string()
    };
    if model.is_empty() {
        return Err("AI model is not configured.".to_string());
    }
    let temperature = request.temperature;
    let max_tokens = request.max_tokens;
    let mut messages =
        request.messages.into_iter().map(OpenAiMessage::from).collect::<Vec<_>>();

    for _ in 0..4 {
        let response = client
            .chat_completion_raw(
                &model,
                &messages,
                temperature,
                max_tokens,
                Some(&tools),
            )
            .await?;

        let needs_tools = response.choices.iter().any(|choice| {
            choice.message.tool_calls.as_ref().is_some_and(|calls| !calls.is_empty())
        });

        if !needs_tools {
            return Ok(public_response_from_raw(response));
        }

        let mut new_messages = Vec::new();
        for choice in &response.choices {
            if let Some(tool_calls) = &choice.message.tool_calls {
                new_messages.push(choice.message.clone());
                for tool_call in tool_calls {
                    let content = execute_tool_call(tool_call, executor)?;
                    new_messages.push(OpenAiMessage {
                        role: "tool".to_string(),
                        content: Some(content),
                        tool_call_id: Some(tool_call.id.clone()),
                        tool_calls: None,
                    });
                }
            }
        }

        messages.extend(new_messages);
    }

    Err("AI tool execution exceeded the maximum number of round trips.".to_string())
}
