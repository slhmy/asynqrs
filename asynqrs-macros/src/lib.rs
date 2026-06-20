//! Proc macros for `asynqrs` typed task ergonomics.
//!
//! This crate is intended to be used through the main `asynqrs` crate's
//! `macros` feature:
//!
//! ```toml
//! asynqrs = { version = "0.2", features = ["macros", "serde"] }
//! ```
//!
//! `#[derive(TaskPayload)]` implements `asynqrs::TypedTaskPayload` for a Rust
//! payload type with an explicit `#[task_type = "..."]` attribute. The generated
//! code targets public `asynqrs` APIs and uses the main crate's serde-gated JSON
//! support path for payload encoding and decoding.

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Expr, Lit, LitStr, Meta, parse_macro_input, parse_quote};

/// Derives `asynqrs::TypedTaskPayload` for a JSON-backed task payload type.
///
/// The derive requires an explicit `#[task_type = "..."]` attribute:
///
/// ```rust,ignore
/// use asynqrs::{TaskPayload, TypedTaskPayload};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Serialize, Deserialize, TaskPayload)]
/// #[task_type = "email:welcome"]
/// struct WelcomeEmail {
///     user_id: u64,
/// }
///
/// let task = WelcomeEmail { user_id: 42 }.into_task()?;
/// assert_eq!(task.task_type(), WelcomeEmail::TASK_TYPE);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// Generated code calls public `asynqrs` APIs only:
/// `TypedTaskPayload`, `TaskPayloadError`, `encode_json_task_payload`, and
/// `decode_json_task_payload`.
///
/// The downstream crate must enable both `asynqrs/macros` and `asynqrs/serde`
/// for this derive to compile. Missing, blank, duplicate, and non-string
/// `task_type` attributes are reported as compile errors.
#[proc_macro_derive(TaskPayload, attributes(task_type))]
pub fn derive_task_payload(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_task_payload(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand_task_payload(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let type_name = input.ident;
    let mut generics = input.generics;
    let task_type = task_type_attribute(&input.attrs)?;
    let task_type_value = task_type.value();

    if task_type_value.trim().is_empty() {
        return Err(syn::Error::new_spanned(
            task_type,
            "`task_type` must contain one or more non-whitespace characters",
        ));
    }

    let payload_type: syn::Type = {
        let (_, ty_generics, _) = generics.split_for_impl();
        parse_quote!(#type_name #ty_generics)
    };
    generics.make_where_clause().predicates.push(parse_quote!(
        #payload_type: ::asynqrs::serde::Serialize + ::asynqrs::serde::de::DeserializeOwned
    ));
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::asynqrs::TypedTaskPayload for #type_name #ty_generics #where_clause {
            const TASK_TYPE: &'static str = #task_type_value;

            fn encode_payload(self) -> ::std::result::Result<::std::vec::Vec<u8>, ::asynqrs::TaskPayloadError> {
                ::asynqrs::encode_json_task_payload(&self)
            }

            fn decode_payload(bytes: &[u8]) -> ::std::result::Result<Self, ::asynqrs::TaskPayloadError> {
                ::asynqrs::decode_json_task_payload(bytes)
            }
        }
    })
}

fn task_type_attribute(attrs: &[syn::Attribute]) -> syn::Result<LitStr> {
    let mut found = None;

    for attr in attrs {
        if attr.path().is_ident("task_type") {
            if found.is_some() {
                return Err(syn::Error::new_spanned(
                    attr,
                    "duplicate `task_type` attribute",
                ));
            }
            found = Some(parse_task_type_literal(attr)?);
        }
    }

    found.ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "missing `#[task_type = \"...\"]` attribute",
        )
    })
}

fn parse_task_type_literal(attr: &syn::Attribute) -> syn::Result<LitStr> {
    match &attr.meta {
        Meta::NameValue(name_value) => match &name_value.value {
            Expr::Lit(expr_lit) => match &expr_lit.lit {
                Lit::Str(literal) => Ok(literal.clone()),
                _ => Err(syn::Error::new_spanned(
                    &name_value.value,
                    "`task_type` value must be a string literal",
                )),
            },
            _ => Err(syn::Error::new_spanned(
                &name_value.value,
                "`task_type` value must be a string literal",
            )),
        },
        Meta::List(_) => attr.parse_args::<LitStr>(),
        Meta::Path(_) => Err(syn::Error::new_spanned(
            attr,
            "`task_type` must be written as `#[task_type = \"...\"]`",
        )),
    }
}
