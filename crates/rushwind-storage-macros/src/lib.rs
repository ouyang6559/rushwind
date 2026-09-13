//! Derive macros for the RushWind storage mapper pair — the compile-time
//! spelling of go-crud's `go-utils/mapper` DTO↔Entity mapping.
//!
//! ```ignore
//! #[derive(ToRecord, FromRecord)]
//! struct User {
//!     id: i64,
//!     name: String,
//!     score: Option<f64>,
//!     active: bool,
//! }
//! ```
//!
//! Field types: `String`, `i64`, `f64`, `bool`, and `Option<_>` of those —
//! the contract's scalar model, nothing more. `None` maps to
//! [`Value::Null`](rushwind_storage::Value::Null) and back; a missing or
//! wrongly-typed field on the read face is
//! [`StorageError::InvalidQuery`](rushwind_storage::StorageError::InvalidQuery).

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::LitStr;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Ident, Type};

/// One supported field shape: which scalar it encodes as, and whether it
/// is optional (`None` ↔ `NULL`).
#[derive(Clone, Copy)]
enum Scalar {
    Text,
    Int,
    Real,
    Bool,
}

fn scalar_of(ty: &Type) -> syn::Result<(Scalar, bool)> {
    let Type::Path(path) = ty else {
        return Err(syn::Error::new_spanned(
            ty,
            "unsupported field type: expected String, i64, f64, bool or Option<_> of those",
        ));
    };
    let segment = path
        .path
        .segments
        .last()
        .expect("a type path has at least one segment");
    if segment.ident == "Option" {
        let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
            return Err(syn::Error::new_spanned(
                ty,
                "Option needs an explicit type argument",
            ));
        };
        let Some(syn::GenericArgument::Type(inner)) = args.args.first() else {
            return Err(syn::Error::new_spanned(
                ty,
                "Option needs an explicit type argument",
            ));
        };
        let (scalar, nested_nullable) = scalar_of(inner)?;
        if nested_nullable {
            return Err(syn::Error::new_spanned(
                ty,
                "Option<Option<_>> is not supported",
            ));
        }
        return Ok((scalar, true));
    }
    let scalar = match segment.ident.to_string().as_str() {
        "String" => Scalar::Text,
        "i64" => Scalar::Int,
        "f64" => Scalar::Real,
        "bool" => Scalar::Bool,
        other => {
            return Err(syn::Error::new_spanned(
                ty,
                format!(
                    "unsupported field type {other}: the derives support String, i64, f64, bool \
                     and Option<_> of those"
                ),
            ))
        }
    };
    Ok((scalar, false))
}

struct Field {
    name: Ident,
    scalar: Scalar,
    nullable: bool,
}

fn fields_of(input: &DeriveInput) -> syn::Result<Vec<Field>> {
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            input,
            "the storage derives apply only to structs",
        ));
    };
    let Fields::Named(named) = &data.fields else {
        return Err(syn::Error::new_spanned(
            input,
            "the storage derives need named fields",
        ));
    };
    named
        .named
        .iter()
        .map(|field| {
            let Some(name) = &field.ident else {
                return Err(syn::Error::new_spanned(field, "expected a named field"));
            };
            let (scalar, nullable) = scalar_of(&field.ty)?;
            Ok(Field {
                name: name.clone(),
                scalar,
                nullable,
            })
        })
        .collect()
}

#[proc_macro_derive(ToRecord)]
pub fn derive_to_record(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let fields = match fields_of(&input) {
        Ok(fields) => fields,
        Err(error) => return error.to_compile_error().into(),
    };
    let writes = fields.iter().map(|field| {
        let name = &field.name;
        let name_literal = name.to_string();
        let expression = write_expression_inner(field);
        quote! { record.insert(#name_literal, #expression); }
    });
    let expanded = quote! {
        impl ::rushwind_storage::ToRecord for #name {
            fn to_record(&self) -> ::rushwind_storage::Record {
                let mut record = ::rushwind_storage::Record::new();
                #(#writes)*
                record
            }
        }
    };
    expanded.into()
}

fn write_expression_inner(field: &Field) -> TokenStream2 {
    let name = &field.name;
    match (field.scalar, field.nullable) {
        (Scalar::Text, false) => quote! { ::rushwind_storage::Value::Text(self.#name.clone()) },
        (Scalar::Int, false) => quote! { ::rushwind_storage::Value::Int(self.#name) },
        (Scalar::Real, false) => quote! { ::rushwind_storage::Value::Real(self.#name) },
        (Scalar::Bool, false) => quote! { ::rushwind_storage::Value::Bool(self.#name) },
        (Scalar::Text, true) => quote! {
            match &self.#name {
                Some(v) => ::rushwind_storage::Value::Text(v.clone()),
                None => ::rushwind_storage::Value::Null,
            }
        },
        (Scalar::Int, true) => quote! {
            match &self.#name {
                Some(v) => ::rushwind_storage::Value::Int(*v),
                None => ::rushwind_storage::Value::Null,
            }
        },
        (Scalar::Real, true) => quote! {
            match &self.#name {
                Some(v) => ::rushwind_storage::Value::Real(*v),
                None => ::rushwind_storage::Value::Null,
            }
        },
        (Scalar::Bool, true) => quote! {
            match &self.#name {
                Some(v) => ::rushwind_storage::Value::Bool(*v),
                None => ::rushwind_storage::Value::Null,
            }
        },
    }
}

#[proc_macro_derive(FromRecord)]
pub fn derive_from_record(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let fields = match fields_of(&input) {
        Ok(fields) => fields,
        Err(error) => return error.to_compile_error().into(),
    };
    let reads = fields.iter().map(|field| {
        let name = &field.name;
        let name_literal = name.to_string();
        let expected = match field.scalar {
            Scalar::Text => "text",
            Scalar::Int => "int",
            Scalar::Real => "real",
            Scalar::Bool => "bool",
        };
        let missing_message = LitStr::new(
            &format!("field {name_literal:?} is missing from the record"),
            name.span(),
        );
        let kind_mismatch_message = LitStr::new(
            &format!("field {name_literal:?} expects {expected}, got {{}}"),
            name.span(),
        );
        let pattern = match (field.scalar, field.nullable) {
            (Scalar::Text, false) => quote! {
                ::rushwind_storage::Value::Text(s) => s.clone()
            },
            (Scalar::Int, false) => quote! {
                ::rushwind_storage::Value::Int(i) => *i
            },
            (Scalar::Real, false) => quote! {
                ::rushwind_storage::Value::Real(r) => *r
            },
            (Scalar::Bool, false) => quote! {
                ::rushwind_storage::Value::Bool(b) => *b
            },
            (Scalar::Text, true) => quote! {
                ::rushwind_storage::Value::Null => None,
                ::rushwind_storage::Value::Text(s) => Some(s.clone())
            },
            (Scalar::Int, true) => quote! {
                ::rushwind_storage::Value::Null => None,
                ::rushwind_storage::Value::Int(i) => Some(*i)
            },
            (Scalar::Real, true) => quote! {
                ::rushwind_storage::Value::Null => None,
                ::rushwind_storage::Value::Real(r) => Some(*r)
            },
            (Scalar::Bool, true) => quote! {
                ::rushwind_storage::Value::Null => None,
                ::rushwind_storage::Value::Bool(b) => Some(*b)
            },
        };
        if field.nullable {
            quote! {
                #name: match record.get(#name_literal) {
                    None => None,
                    Some(value) => match value {
                        #pattern,
                        other => {
                            return Err(::rushwind_storage::StorageError::InvalidQuery(format!(
                                #kind_mismatch_message,
                                other.type_name()
                            )))
                        }
                    },
                }
            }
        } else {
            quote! {
                #name: match record.get(#name_literal) {
                    Some(value) => match value {
                        #pattern,
                        other => {
                            return Err(::rushwind_storage::StorageError::InvalidQuery(format!(
                                #kind_mismatch_message,
                                other.type_name()
                            )))
                        }
                    },
                    None => {
                        return Err(::rushwind_storage::StorageError::InvalidQuery(
                            #missing_message.to_owned(),
                        ))
                    }
                }
            }
        }
    });
    let expanded = quote! {
        impl ::rushwind_storage::FromRecord for #name {
            fn from_record(record: &::rushwind_storage::Record) -> ::std::result::Result<Self, ::rushwind_storage::StorageError> {
                Ok(Self {
                    #(#reads,)*
                })
            }
        }
    };
    expanded.into()
}
