//! Ollama LLM provider.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::llm::Llm;
use crate::llm::types::{
    ChatContent, ChatMessage, ChatResponse, ChatRole, ToolCallRequest, ToolDef,
};

/// Ollama LLM provider (offline, local).
pub struct OllamaLlm {
    client: reqwest::Client,
    url: String,
    model: String,
}

impl OllamaLlm {
    pub fn new(url: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.into(),
            model: model.into(),
        }
    }
}

#[async_trait]
impl Llm for OllamaLlm {
    async fn generate(&self, prompt: &str) -> Result<String> {
        let request = OllamaGenerateRequest {
            model: &self.model,
            prompt,
            system: None,
            stream: false,
        };

        let response: OllamaGenerateResponse = self
            .client
            .post(format!("{}/api/generate", self.url))
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::Llm(format!("ollama request failed: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Llm(format!("ollama response parse failed: {e}")))?;

        Ok(response.response)
    }

    async fn generate_with_system(&self, system: &str, user: &str) -> Result<String> {
        let request = OllamaGenerateRequest {
            model: &self.model,
            prompt: user,
            system: Some(system),
            stream: false,
        };

        let response: OllamaGenerateResponse = self
            .client
            .post(format!("{}/api/generate", self.url))
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::Llm(format!("ollama request failed: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Llm(format!("ollama response parse failed: {e}")))?;

        Ok(response.response)
    }

    async fn chat(&self, messages: &[ChatMessage], tools: &[ToolDef]) -> Result<ChatResponse> {
        let mut ollama_messages = Vec::new();

        for msg in messages {
            match (&msg.role, &msg.content) {
                (ChatRole::System, ChatContent::Text { text }) => {
                    ollama_messages.push(OllamaChatMessage {
                        role: "system",
                        content: text.clone(),
                        tool_calls: None,
                    });
                }
                (ChatRole::User, ChatContent::Text { text }) => {
                    ollama_messages.push(OllamaChatMessage {
                        role: "user",
                        content: text.clone(),
                        tool_calls: None,
                    });
                }
                (ChatRole::Assistant, ChatContent::Text { text }) => {
                    ollama_messages.push(OllamaChatMessage {
                        role: "assistant",
                        content: text.clone(),
                        tool_calls: None,
                    });
                }
                (ChatRole::Assistant, ChatContent::ToolCalls { text, calls }) => {
                    let tc: Vec<OllamaToolCall> = calls
                        .iter()
                        .map(|c| OllamaToolCall {
                            function: OllamaFunctionCall {
                                name: c.name.clone(),
                                arguments: c.arguments.clone(),
                            },
                        })
                        .collect();
                    ollama_messages.push(OllamaChatMessage {
                        role: "assistant",
                        content: text.clone().unwrap_or_default(),
                        tool_calls: Some(tc),
                    });
                }
                (ChatRole::Tool, ChatContent::ToolResult { result, .. }) => {
                    ollama_messages.push(OllamaChatMessage {
                        role: "tool",
                        content: result.clone(),
                        tool_calls: None,
                    });
                }
                _ => {}
            }
        }

        let ollama_tools: Vec<OllamaToolDef> = tools
            .iter()
            .map(|t| OllamaToolDef {
                r#type: "function",
                function: OllamaFunction {
                    name: &t.name,
                    description: &t.description,
                    parameters: &t.parameters,
                },
            })
            .collect();

        let request = OllamaChatRequest {
            model: &self.model,
            messages: &ollama_messages,
            tools: if ollama_tools.is_empty() {
                None
            } else {
                Some(&ollama_tools)
            },
            stream: false,
        };

        let response: OllamaChatResponse = self
            .client
            .post(format!("{}/api/chat", self.url))
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::Llm(format!("ollama chat request failed: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Llm(format!("ollama chat response parse failed: {e}")))?;

        let msg = response.message;

        if let Some(tool_calls) = msg.tool_calls
            && !tool_calls.is_empty()
        {
            let calls: Vec<ToolCallRequest> = tool_calls
                .into_iter()
                .enumerate()
                .map(|(i, tc)| ToolCallRequest {
                    id: format!("call_{i}"),
                    name: tc.function.name,
                    arguments: tc.function.arguments,
                })
                .collect();

            let text = if msg.content.is_empty() {
                None
            } else {
                Some(msg.content)
            };

            return Ok(ChatResponse::ToolCalls { text, calls });
        }

        Ok(ChatResponse::Message(msg.content))
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}

// -- Generate API types --

#[derive(Serialize)]
struct OllamaGenerateRequest<'a> {
    model: &'a str,
    prompt: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<&'a str>,
    stream: bool,
}

#[derive(Deserialize)]
struct OllamaGenerateResponse {
    response: String,
}

// -- Chat API types --

#[derive(Serialize)]
struct OllamaChatRequest<'a> {
    model: &'a str,
    messages: &'a [OllamaChatMessage<'a>],
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<&'a [OllamaToolDef<'a>]>,
    stream: bool,
}

#[derive(Serialize)]
struct OllamaChatMessage<'a> {
    role: &'a str,
    content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<OllamaToolCall>>,
}

#[derive(Serialize, Deserialize)]
struct OllamaToolCall {
    function: OllamaFunctionCall,
}

#[derive(Serialize, Deserialize)]
struct OllamaFunctionCall {
    name: String,
    arguments: Value,
}

#[derive(Serialize)]
struct OllamaToolDef<'a> {
    r#type: &'a str,
    function: OllamaFunction<'a>,
}

#[derive(Serialize)]
struct OllamaFunction<'a> {
    name: &'a str,
    description: &'a str,
    parameters: &'a Value,
}

#[derive(Deserialize)]
struct OllamaChatResponse {
    message: OllamaChatResponseMessage,
}

#[derive(Deserialize)]
struct OllamaChatResponseMessage {
    #[serde(default)]
    content: String,
    #[serde(default)]
    tool_calls: Option<Vec<OllamaToolCall>>,
}
