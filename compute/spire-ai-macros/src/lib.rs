//! Derive macros for spire-ai.
//!
//! Provides `#[derive(Doc)]` to automatically implement the `Doc` trait.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

/// Derive the `Doc` trait for a struct.
///
/// # Attributes
///
/// - `#[id]` — Mark the primary key field (required, must be `String`)
/// - `#[embed]` — Include this field in the embedding text
/// - `#[no_embed]` — On the struct: disable embedding entirely
///
/// # Behavior
///
/// - If no `#[embed]` attributes and no `#[no_embed]`: all `String` fields (except `#[id]`) are embedded
/// - If any `#[embed]` is present: only those fields are embedded
/// - `#[no_embed]` on struct: `embed_text()` returns empty string
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Doc, Serialize, Deserialize, Clone)]
/// struct Article {
///     #[id]
///     slug: String,
///     title: String,       // auto-embedded
///     content: String,     // auto-embedded
///     views: i64,          // not embedded (not String)
/// }
/// ```
#[proc_macro_derive(Doc, attributes(id, embed, no_embed))]
pub fn derive_doc(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_doc(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

fn expand_doc(input: DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Check for #[no_embed] on the struct
    let no_embed = input.attrs.iter().any(|a| a.path().is_ident("no_embed"));

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(f) => &f.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &input,
                    "Doc can only be derived for structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &input,
                "Doc can only be derived for structs",
            ));
        }
    };

    // Find #[id] field
    let mut id_field = None;
    let mut explicit_embed_fields = Vec::new();
    let mut has_explicit_embed = false;

    for field in fields.iter() {
        let field_ident = field
            .ident
            .as_ref()
            .ok_or_else(|| syn::Error::new_spanned(field, "expected named field"))?;

        for attr in &field.attrs {
            if attr.path().is_ident("id") {
                if id_field.is_some() {
                    return Err(syn::Error::new_spanned(
                        attr,
                        "only one field can be marked #[id]",
                    ));
                }
                id_field = Some(field_ident.clone());
            }
            if attr.path().is_ident("embed") {
                has_explicit_embed = true;
                explicit_embed_fields.push(field_ident.clone());
            }
        }
    }

    let id_field = id_field.ok_or_else(|| {
        syn::Error::new_spanned(&input, "Doc requires one field marked with #[id]")
    })?;

    // Generate embed_text() body
    let embed_body = if no_embed {
        quote! { String::new() }
    } else if has_explicit_embed {
        // Only embed fields marked with #[embed]
        if explicit_embed_fields.is_empty() {
            quote! { String::new() }
        } else {
            let field_refs: Vec<_> = explicit_embed_fields
                .iter()
                .map(|f| quote! { self.#f.as_str() })
                .collect();
            quote! {
                [#(#field_refs),*].join("\n\n")
            }
        }
    } else {
        // Auto-embed: all String fields except the #[id] field
        let auto_fields: Vec<_> = fields
            .iter()
            .filter_map(|f| {
                let ident = f.ident.as_ref()?;
                if *ident == id_field {
                    return None;
                }
                // Check if type is String
                if is_string_type(&f.ty) {
                    Some(ident.clone())
                } else {
                    None
                }
            })
            .collect();

        if auto_fields.is_empty() {
            quote! { String::new() }
        } else {
            let field_refs: Vec<_> = auto_fields
                .iter()
                .map(|f| quote! { self.#f.as_str() })
                .collect();
            quote! {
                [#(#field_refs),*].join("\n\n")
            }
        }
    };

    let expanded = quote! {
        impl #impl_generics spire_ai::Doc for #name #ty_generics #where_clause {
            fn id(&self) -> &str {
                &self.#id_field
            }

            fn embed_text(&self) -> String {
                #embed_body
            }
        }
    };

    Ok(expanded)
}

/// Check if a type is `String`.
fn is_string_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
    {
        return segment.ident == "String";
    }
    false
}
