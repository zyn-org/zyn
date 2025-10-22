// SPDX-License-Identifier: AGPL-3.0

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{ToTokens, quote};

use syn::{
  Data, DeriveInput, Fields, Ident, LitStr, Result as SynResult, Token,
  parse::{Parse, ParseStream},
  parse_macro_input,
};

// Structure to hold our parameter attributes
struct ParamAttr {
  name: Option<String>,
  validate: Option<String>,
}

// Parse implementation for attribute arguments
impl Parse for ParamAttr {
  fn parse(input: ParseStream) -> SynResult<Self> {
    let mut name = None;
    let mut validate = None;

    // Parse comma-separated key=value pairs
    while !input.is_empty() {
      let key: Ident = input.parse()?;
      let key_str = key.to_string();

      input.parse::<Token![=]>()?;

      if key_str == "name" {
        let value: LitStr = input.parse()?;
        name = Some(value.value());
      } else if key_str == "validate" {
        let value: LitStr = input.parse()?;

        match value.value().as_str() {
          "non_zero" | "non_empty" => {},
          _ => return Err(syn::Error::new(key.span(), "unknown validation attribute")),
        }
        validate = Some(value.value());
      } else {
        return Err(syn::Error::new(key.span(), "unknown parameter attribute"));
      }

      if !input.is_empty() {
        input.parse::<Token![,]>()?;
      }
    }

    Ok(ParamAttr { name, validate })
  }
}

#[derive(PartialEq)]
enum FieldKind {
  Regular,
  Optional,
  Vec,
}

struct FieldInfo {
  /// The struct field name.
  field_name: syn::Ident,

  /// The name of the parameter to write/read.
  /// If not specified, defaults to the field name.
  param_name: String,

  /// Field inner type.
  typ: syn::Type,

  /// Field kind.
  kind: FieldKind,

  /// Field's attribute parameters.
  attr: Option<ParamAttr>,
}

/// Implementation of the `#[derive(MessageParameters)]` procedural macro
#[proc_macro_derive(ProtocolMessageParameters, attributes(param))]
pub fn derive_message_parameters(input: TokenStream) -> TokenStream {
  // Parse the input tokens into a syntax tree
  let input = parse_macro_input!(input as DeriveInput);

  // Generate the implementation
  generate_message_parameters(&input).unwrap_or_else(|err| {
    let compile_error = err.to_compile_error();
    quote! { #compile_error }.into()
  })
}

fn generate_message_parameters(input: &DeriveInput) -> Result<TokenStream, syn::Error> {
  // Extract the struct fields
  let fields = extract_struct_field_info(input)?;

  // Get the name of the type we're implementing MessageParameters for
  let struct_name = &input.ident;

  // Separate id field from other fields and sort remaining fields alphabetically
  let mut id_field = None;
  let mut other_fields = Vec::new();

  for field in fields.iter() {
    if field.param_name == "id" {
      id_field = Some(field);
    } else {
      other_fields.push(field);
    }
  }

  // Sort other fields alphabetically by parameter name
  other_fields.sort_by(|a, b| a.param_name.cmp(&b.param_name));

  // Combine fields with id first (if present)
  let ordered_fields: Vec<&FieldInfo> =
    if let Some(id) = id_field { std::iter::once(id).chain(other_fields).collect() } else { other_fields };

  // Generate encoding code for each field in canonical order
  let param_encoding = ordered_fields.iter().map(|field| {
    let field_name = &field.field_name;
    let param_name = field.param_name.to_string();

    if field.kind == FieldKind::Optional {
      quote! {
        if let Some(field_inner_ref) = self.#field_name.as_ref() {
          n += parameter_writer.write_param(#param_name.as_bytes(), field_inner_ref)?;
        }
      }
    } else if field.kind == FieldKind::Vec {
      quote! {
          n += parameter_writer.write_param_slice(#param_name.as_bytes(), &self.#field_name)?;
      }
    } else {
      quote! {
          n += parameter_writer.write_param(#param_name.as_bytes(), &self.#field_name)?;
      }
    }
  });

  // Generate decoding code for each field
  let param_decoding = fields.iter().map(|field| {
    let field_name = &field.field_name;
    let param_name = field.param_name.to_string();

    let as_impl = {
      let typ_token = field.typ.to_token_stream();

      let method_name = {
        match typ_token.to_string().as_str() {
          "StringAtom" => "as_atom",
          _ => &format!("as_{}", typ_token),
        }
      };

      let method = Ident::new(&method_name.to_lowercase(), proc_macro2::Span::call_site());
      quote! { #method }
    };

    if field.kind == FieldKind::Optional {
      quote! {
          if p.name == #param_name.as_bytes() {
            self.#field_name = Some(p.#as_impl()?);
            continue;
          }
      }
    } else if field.kind == FieldKind::Vec {
      quote! {
        if p.name == #param_name.as_bytes() {
          self.#field_name.push(p.#as_impl()?);
          continue;
        }
      }
    } else {
      quote! {
          if p.name == #param_name.as_bytes() {
            self.#field_name = p.#as_impl()?;
            continue;
          }
      }
    }
  });

  // Generate validation code for each field
  let param_validations = fields.iter().map(|field| {
    let field_name = &field.field_name;
    let field_name_str = field_name.to_string();
    let validate = field.attr.as_ref().and_then(|attr| attr.validate.as_ref());

    if let Some(validate) = validate {
      match validate.as_str() {
        "non_zero" => {
          quote! {
            if self.#field_name == 0 {
              return Err(anyhow::anyhow!("{} field must be non-zero", #field_name_str));
            }
          }
        },
        "non_empty" => {
          quote! {
            if self.#field_name.is_empty() {
              return Err(anyhow::anyhow!("{} field must be non-empty", #field_name_str));
            }
          }
        },
        _ => unreachable!(),
      }
    } else {
      quote! {}
    }
  });

  // Generate the encode implementation
  let encode_impl = quote! {
    let mut n: usize = 0;

     #(#param_encoding)*

    Ok(n)
  };

  // Generate the decode implementation
  let decode_impl = quote! {
      for param_res in parameter_reader {
        let p = param_res?;

        #(#param_decoding)*
      }
      Ok(())
  };

  // Generate the validation implementation
  let validate_impl = quote! {

      #(#param_validations)*

      Ok(())
  };

  // Generate the final implementation
  let expanded = quote! {
      impl ProtocolMessageParameters for #struct_name {
          fn encode(&self, parameter_writer: &mut ParameterWriter) -> anyhow::Result<usize> {
            #encode_impl
          }

          fn decode(&mut self, parameter_reader: &mut ParameterReader) -> anyhow::Result<()> {
            #decode_impl
          }

          fn validate(&self) -> anyhow::Result<()> {
            use crate::Error;

            #validate_impl
          }
      }
  };

  // Convert back to a token stream
  Ok(TokenStream::from(expanded))
}

fn extract_struct_field_info(input: &DeriveInput) -> Result<Vec<FieldInfo>, syn::Error> {
  match &input.data {
    Data::Struct(data) => match &data.fields {
      Fields::Named(fields_named) => {
        let mut result = Vec::new();
        for field in fields_named.named.iter() {
          result.push(field_to_field_info(field)?);
        }
        Ok(result)
      },
      _ => Err(syn::Error::new(proc_macro2::Span::call_site(), "MessageParameters only works with named fields")),
    },
    _ => Err(syn::Error::new(proc_macro2::Span::call_site(), "MessageParameters only works with structs")),
  }
}

fn field_to_field_info(field: &syn::Field) -> SynResult<FieldInfo> {
  let mut param_attr: Option<ParamAttr> = None;
  for attr in &field.attrs {
    if attr.path().is_ident("param") {
      param_attr = Some(attr.parse_args()?);
    } else {
      return Err(syn::Error::new(proc_macro2::Span::call_site(), "unknown attribute"));
    }
  }

  // Extract the type and determine if it's Option<T>, Vec<T>, or a regular type
  let (field_kind, inner_type) = match &field.ty {
    syn::Type::Path(type_path) => {
      if let Some(segment) = type_path.path.segments.first() {
        if segment.ident == "Option" {
          // Handle Option<T>
          if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
            if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
              (FieldKind::Optional, inner_type.clone())
            } else {
              (FieldKind::Optional, field.ty.clone())
            }
          } else {
            (FieldKind::Optional, field.ty.clone())
          }
        } else if segment.ident == "Vec" {
          // Handle Vec<T>
          if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
            if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
              (FieldKind::Vec, inner_type.clone())
            } else {
              (FieldKind::Vec, field.ty.clone())
            }
          } else {
            (FieldKind::Vec, field.ty.clone())
          }
        } else {
          // Regular type
          (FieldKind::Regular, field.ty.clone())
        }
      } else {
        (FieldKind::Regular, field.ty.clone())
      }
    },
    _ => (FieldKind::Regular, field.ty.clone()),
  };

  let field_ref = field.ident.as_ref().unwrap();

  Ok(FieldInfo {
    field_name: field_ref.clone(),
    param_name: match &param_attr {
      Some(attr) => attr.name.clone().unwrap_or_else(|| field_ref.to_string()),
      None => field_ref.to_string(),
    },
    kind: field_kind,
    attr: param_attr,
    typ: inner_type,
  })
}
