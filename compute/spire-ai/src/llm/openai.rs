//! OpenAI LLM provider.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::llm::Llm;
use crate::llm::types::{
    ChatContent, ChatMessage, ChatResponse, ChatRole, ToolCallRequest, ToolDef,
};

/// OpenAI LLM provider.
pub struct OpenAiLlm {
    client: reqwest::Client,
    api_key: String,
    model: String,
}

impl OpenAiLlm {
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
            model: model.into(),
        }
    }
}

#[async_trait]
impl Llm for OpenAiLlm {
    async fn generate(&self, prompt: &str) -> Result<String> {
        self.generate_with_system("You are a helpful assistant.", prompt)
            .await
    }

    async fn generate_with_system(&self, system: &str, user: &str) -> Result<String> {
        let request = SimpleChatRequest {
            model: &self.model,
            messages: vec![
                SimpleChatMessage {
                    role: "system",
                    content: system,
                },
                SimpleChatMessage {
                    role: "user",
                    content: user,
                },
            ],
        };

        let response: SimpleChatResponse = self
            .client
            .post("https://api.openai.com/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::Llm(format!("OpenAI request failed: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Llm(format!("OpenAI response parse failed: {e}")))?;

        response
            .choices
            .into_iter()
            .next()
            .map(|c| c.message.content)
            .ok_or_else(|| Error::Llm("OpenAI returned no choices".into()))
    }

    async fn chat(&self, messages: &[ChatMessage], tools: &[ToolDef]) -> Result<ChatResponse> {
        let mut api_messages: Vec<Value> = Vec::new();

        for msg in messages {
            match (&msg.role, &msg.content) {
                (ChatRole::System, ChatContent::Text { text }) => {
                    api_messages.push(serde_json::json!({
                        "role": "system",
                        "content": text,
                    }));
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
                    let tc: Vec<Value> = calls
                        .iter()
                        .map(|c| {
                            serde_json::json!({
                                "id": c.id,
                                "type": "function",
                                "function": {
                                    "name": c.name,
                                    "arguments": c.arguments.to_string(),
                                },
                            })
                        })
                        .collect();
                    let mut msg_val = serde_json::json!({
                        "role": "assistant",
                        "tool_calls": tc,
                    });
                    if let Some(t) = text {
                        msg_val["content"] = Value::String(t.clone());
                    }
                    api_messages.push(msg_val);
                }
                (ChatRole::Tool, ChatContent::ToolResult { call_id, result }) => {
                    api_messages.push(serde_json::json!({
                        "role": "tool",
                        "tool_call_id": call_id,
                        "content": result,
                    }));
                }
                _ => {}
            }
        }

        let api_tools: Vec<Value> = tools
            .iter()
            .map(|t| {
                serde_json::json!({
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.parameters,
                    },
                })
            })
            .collect();

        let mut body = serde_json::json!({
            "model": self.model,
            "messages": api_messages,
        });

        if !api_tools.is_empty() {
            body["tools"] = Value::Array(api_tools);
        }

        let response: OpenAiChatResponse = self
            .client
            .post("https://api.openai.com/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::Llm(format!("OpenAI chat request failed: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Llm(format!("OpenAI chat response parse failed: {e}")))?;

        let choice = response
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| Error::Llm("OpenAI returned no choices".into()))?;

        if let Some(tool_calls) = choice.message.tool_calls
            && !tool_calls.is_empty()
        {
            let calls: Vec<ToolCallRequest> = tool_calls
                .into_iter()
                .map(|tc| {
                    let arguments = serde_json::from_str(&tc.function.arguments)
                        .unwrap_or(Value::Object(Default::default()));
                    ToolCallRequest {
                        id: tc.id,
                        name: tc.function.name,
                        arguments,
                    }
                })
                .collect();

            let text = choice.message.content;
            return Ok(ChatResponse::ToolCalls { text, calls });
        }

        Ok(ChatResponse::Message(
            choice.message.content.unwrap_or_default(),
        ))
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}

// -- Simple generate types --

#[derive(Serialize)]
struct SimpleChatRequest<'a> {
    model: &'a str,
    messages: Vec<SimpleChatMessage<'a>>,
}

#[derive(Serialize)]
struct SimpleChatMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Deserialize)]
struct SimpleChatResponse {
    choices: Vec<SimpleChatChoice>,
}

#[derive(Deserialize)]
struct SimpleChatChoice {
    message: SimpleChatResponseMessage,
}

#[derive(Deserialize)]
struct SimpleChatResponseMessage {
    content: String,
}

// -- Chat types --

#[derive(Deserialize)]
struct OpenAiChatResponse {
    choices: Vec<OpenAiChatChoice>,
}

#[derive(Deserialize)]
struct OpenAiChatChoice {
    message: OpenAiChatMessage,
}

#[derive(Deserialize)]
struct OpenAiChatMessage {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<OpenAiToolCall>>,
}

#[derive(Deserialize)]
struct OpenAiToolCall {
    id: String,
    function: OpenAiFunctionCall,
}

#[derive(Deserialize)]
struct OpenAiFunctionCall {
    name: String,
    arguments: String,
}
