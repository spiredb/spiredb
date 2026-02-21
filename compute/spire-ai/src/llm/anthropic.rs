//! Anthropic Claude LLM provider.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::llm::Llm;
use crate::llm::types::{
    ChatContent, ChatMessage, ChatResponse, ChatRole, ToolCallRequest, ToolDef,
};

/// Anthropic Claude LLM provider.
pub struct AnthropicLlm {
    client: reqwest::Client,
    api_key: String,
    model: String,
}

impl AnthropicLlm {
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
            model: model.into(),
        }
    }
}

#[async_trait]
impl Llm for AnthropicLlm {
    async fn generate(&self, prompt: &str) -> Result<String> {
        self.generate_with_system("You are a helpful assistant.", prompt)
            .await
    }

    async fn generate_with_system(&self, system: &str, user: &str) -> Result<String> {
        let request = AnthropicSimpleRequest {
            model: &self.model,
            max_tokens: 4096,
            system,
            messages: vec![AnthropicSimpleMessage {
                role: "user",
                content: user,
            }],
        };

        let response: AnthropicSimpleResponse = self
            .client
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::Llm(format!("Anthropic request failed: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Llm(format!("Anthropic response parse failed: {e}")))?;

        response
            .content
            .into_iter()
            .next()
            .map(|c| c.text)
            .ok_or_else(|| Error::Llm("Anthropic returned no content".into()))
    }

    async fn chat(&self, messages: &[ChatMessage], tools: &[ToolDef]) -> Result<ChatResponse> {
        let mut system_text = String::new();
        let mut api_messages: Vec<Value> = Vec::new();

        for msg in messages {
            match (&msg.role, &msg.content) {
                (ChatRole::System, ChatContent::Text { text }) => {
                    system_text = text.clone();
                }
                (ChatRole::User, ChatContent::Text { text }) => {
                    api_messages.push(serde_json::json!({
                        "role": "user",
                        "content": text,
                    }));
                }
                (ChatRole::Assistant, ChatContent::Text { text }) => {
                    api_messages.push(serde_json::json!({
                        "role": "assistant",
                        "content": text,
                    }));
                }
                (ChatRole::Assistant, ChatContent::ToolCalls { text, calls }) => {
                    let mut blocks: Vec<Value> = Vec::new();
                    if let Some(t) = text {
                        blocks.push(serde_json::json!({"type": "text", "text": t}));
                    }
                    for call in calls {
                        blocks.push(serde_json::json!({
                            "type": "tool_use",
                            "id": call.id,
                            "name": call.name,
                            "input": call.arguments,
                        }));
                    }
                    api_messages.push(serde_json::json!({
                        "role": "assistant",
                        "content": blocks,
                    }));
                }
                (ChatRole::Tool, ChatContent::ToolResult { call_id, result }) => {
                    // Anthropic: tool results go as user messages with tool_result content blocks
                    api_messages.push(serde_json::json!({
                        "role": "user",
                        "content": [{
                            "type": "tool_result",
                            "tool_use_id": call_id,
                            "content": result,
                        }],
                    }));
                }
                _ => {}
            }
        }

        let api_tools: Vec<Value> = tools
            .iter()
            .map(|t| {
                serde_json::json!({
                    "name": t.name,
                    "description": t.description,
                    "input_schema": t.parameters,
                })
            })
            .collect();

        let mut body = serde_json::json!({
            "model": self.model,
            "max_tokens": 4096,
            "messages": api_messages,
        });

        if !system_text.is_empty() {
            body["system"] = Value::String(system_text);
        }
        if !api_tools.is_empty() {
            body["tools"] = Value::Array(api_tools);
        }

        let response: AnthropicChatResponse = self
            .client
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::Llm(format!("Anthropic chat request failed: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Llm(format!("Anthropic chat response parse failed: {e}")))?;

        // Parse content blocks
        let mut text_parts = Vec::new();
        let mut tool_calls = Vec::new();

        for block in &response.content {
            match block.r#type.as_str() {
                "text" => {
                    if let Some(t) = &block.text {
                        text_parts.push(t.clone());
                    }
                }
                "tool_use" => {
                    if let (Some(id), Some(name), Some(input)) =
                        (&block.id, &block.name, &block.input)
                    {
                        tool_calls.push(ToolCallRequest {
                            id: id.clone(),
                            name: name.clone(),
                            arguments: input.clone(),
                        });
                    }
                }
                _ => {}
            }
        }

        if !tool_calls.is_empty() || response.stop_reason.as_deref() == Some("tool_use") {
            let text = if text_parts.is_empty() {
                None
            } else {
                Some(text_parts.join("\n"))
            };
            Ok(ChatResponse::ToolCalls {
                text,
                calls: tool_calls,
            })
        } else {
            Ok(ChatResponse::Message(text_parts.join("\n")))
        }
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}

// -- Simple generate types --

#[derive(Serialize)]
struct AnthropicSimpleRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    system: &'a str,
    messages: Vec<AnthropicSimpleMessage<'a>>,
}

#[derive(Serialize)]
struct AnthropicSimpleMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Deserialize)]
struct AnthropicSimpleResponse {
    content: Vec<AnthropicSimpleContent>,
}

#[derive(Deserialize)]
struct AnthropicSimpleContent {
    text: String,
}

// -- Chat types --

#[derive(Deserialize)]
struct AnthropicChatResponse {
    content: Vec<AnthropicContentBlock>,
    #[serde(default)]
    stop_reason: Option<String>,
}

#[derive(Deserialize)]
struct AnthropicContentBlock {
    r#type: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    input: Option<Value>,
}
